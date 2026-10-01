"""Prefix routing with least-cost ordering, circuit breakers and per-carrier TPS limits."""
import asyncio
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class RouteOption:
    carrier: str
    cost: float


@dataclass(frozen=True)
class Route:
    name: str
    prefix: str
    options: tuple  # RouteOption, cheapest first


def route_prefixes(route: dict) -> list:
    """A route has either `prefix: "44"` or `prefixes: ["9613", "96170"]`."""
    if "prefixes" in route:
        return [str(p) for p in route["prefixes"]]
    return [str(route["prefix"])]


class Router:
    def __init__(self, config: dict):
        entries = []
        for r in config.get("routes", []):
            options = tuple(sorted(
                (RouteOption(o["carrier"], float(o["cost"])) for o in r["carriers"]),
                key=lambda o: o.cost,
            ))
            for prefix in route_prefixes(r):
                entries.append(Route(name=r["name"], prefix=prefix, options=options))
        self.routes = sorted(entries, key=lambda r: len(r.prefix), reverse=True)

    def match(self, msisdn: str):
        """Longest-prefix match, the way telecom routing tables work."""
        digits = msisdn.lstrip("+")
        for route in self.routes:
            if digits.startswith(route.prefix):
                return route
        return None


class CircuitBreaker:
    """Stops sending to a carrier after repeated failures, then probes it again.

    closed -> (N consecutive failures) -> open -> (cool-down) -> half-open
    half-open -> success -> closed, failure -> open
    """

    def __init__(self, failure_threshold=5, reset_after=30.0, clock=time.monotonic):
        self.failure_threshold = failure_threshold
        self.reset_after = reset_after
        self.clock = clock
        self.failures = 0
        self.opened_at = None

    @property
    def state(self) -> str:
        if self.opened_at is None:
            return "closed"
        if self.clock() - self.opened_at >= self.reset_after:
            return "half-open"
        return "open"

    def allow(self) -> bool:
        return self.state != "open"

    def record_success(self):
        self.failures = 0
        self.opened_at = None

    def record_failure(self):
        self.failures += 1
        if self.state == "half-open" or self.failures >= self.failure_threshold:
            self.opened_at = self.clock()


class TokenBucket:
    """Limits submit_sm per second to the throughput agreed with the carrier."""

    def __init__(self, rate: float, burst: float = None, clock=time.monotonic):
        self.rate = rate
        self.capacity = burst or rate
        self.tokens = self.capacity
        self.clock = clock
        self.updated = clock()
        self._lock = asyncio.Lock()

    def _refill(self):
        now = self.clock()
        self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
        self.updated = now

    async def acquire(self):
        async with self._lock:
            while True:
                self._refill()
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                await asyncio.sleep((1 - self.tokens) / self.rate)
