import asyncio

import pytest

from smsgw.config import validate
from smsgw.routing import CircuitBreaker, Router, TokenBucket

CONFIG = {
    "carriers": {"a": {}, "b": {}},
    "routes": [
        {"name": "lb", "prefix": "961", "carriers": [{"carrier": "a", "cost": 0.03},
                                                      {"carrier": "b", "cost": 0.02}]},
        {"name": "lb-mobile", "prefix": "9613", "carriers": [{"carrier": "a", "cost": 0.01}]},
        {"name": "default", "prefix": "", "carriers": [{"carrier": "b", "cost": 0.1}]},
    ],
}


def test_longest_prefix_wins():
    router = Router(CONFIG)
    assert router.match("+96131234567").name == "lb-mobile"
    assert router.match("9611234567").name == "lb"
    assert router.match("447700900123").name == "default"


def test_options_sorted_by_cost():
    route = Router(CONFIG).match("9611234567")
    assert [o.carrier for o in route.options] == ["b", "a"]


def test_no_route_without_default():
    config = {**CONFIG, "routes": CONFIG["routes"][:2]}
    assert Router(config).match("447700900123") is None


def test_validate_rejects_unknown_carrier():
    bad = {"carriers": {"a": {}}, "routes": [{"name": "x", "prefix": "1",
                                              "carriers": [{"carrier": "zzz", "cost": 1}]}]}
    with pytest.raises(ValueError):
        validate(bad)


def test_circuit_breaker_opens_and_half_opens():
    now = [0.0]
    breaker = CircuitBreaker(failure_threshold=3, reset_after=10, clock=lambda: now[0])
    for _ in range(3):
        assert breaker.allow()
        breaker.record_failure()
    assert breaker.state == "open" and not breaker.allow()
    now[0] = 10
    assert breaker.state == "half-open" and breaker.allow()
    breaker.record_failure()  # probe failed: open again
    assert breaker.state == "open"
    now[0] = 20
    breaker.record_success()
    assert breaker.state == "closed"


def test_token_bucket_limits_rate():
    async def run():
        bucket = TokenBucket(rate=50, burst=5)
        loop = asyncio.get_running_loop()
        start = loop.time()
        for _ in range(15):
            await bucket.acquire()
        return loop.time() - start

    elapsed = asyncio.run(run())
    assert 0.15 <= elapsed < 0.6  # 5 immediately, then 10 more at 50/s = 0.2s


def test_route_with_several_prefixes():
    config = {"carriers": {"a": {}}, "routes": [
        {"name": "lb-mobile", "prefixes": ["9613", "96170"],
         "carriers": [{"carrier": "a", "cost": 1}]},
        {"name": "lb", "prefix": "961", "carriers": [{"carrier": "a", "cost": 2}]},
    ]}
    validate(config)
    router = Router(config)
    assert router.match("+96170123456").name == "lb-mobile"
    assert router.match("+9613123456").name == "lb-mobile"
    assert router.match("+9611123456").name == "lb"


def test_shipped_config_is_valid():
    import os

    from smsgw.config import load

    config = load(os.path.join(os.path.dirname(__file__), "..", "..", "config", "routes.yaml"))
    assert Router(config).match("+96170123456").name == "lebanon-mobile"
