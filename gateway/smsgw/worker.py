"""Queue consumer: routes each message, submits it over SMPP with failover, handles DLRs."""
import asyncio
import json
import logging
import os
import signal
import socket
import time

from prometheus_client import start_http_server

from . import config as config_loader
from . import encoding, metrics
from .routing import CircuitBreaker, Router, TokenBucket
from .smpp import codec
from .smpp.client import SmppClient, SmppError
from .store import FINAL_STATES, Store

log = logging.getLogger("worker")


class Carrier:
    def __init__(self, name, cfg, on_deliver):
        self.name = name
        self.client = SmppClient(
            cfg["host"], int(cfg["port"]), cfg["system_id"], cfg["password"],
            on_deliver=lambda pdu: on_deliver(self, pdu),
            enquire_link_interval=float(cfg.get("enquire_link_interval", 30)),
        )
        self.breaker = CircuitBreaker(int(cfg.get("failure_threshold", 5)),
                                      float(cfg.get("reset_after_seconds", 30)))
        self.bucket = TokenBucket(float(cfg.get("tps", 50)))

    @property
    def available(self) -> bool:
        return self.client.connected and self.breaker.allow()

    def state(self) -> dict:
        return {"connected": self.client.connected, "circuit": self.breaker.state}


class Worker:
    def __init__(self, config, store: Store, consumer=None, concurrency=50, max_attempts=4,
                 retry_base_delay=1.0):
        self.store = store
        self.router = Router(config)
        self.consumer = consumer or socket.gethostname()
        self.concurrency = asyncio.Semaphore(concurrency)
        self.max_attempts = max_attempts
        self.retry_base_delay = retry_base_delay
        self.carriers = {name: Carrier(name, cfg, self.on_deliver)
                         for name, cfg in config["carriers"].items()}
        self.running = True
        self._tasks = set()

    # ---------- lifecycle ----------
    async def run(self):
        await self.store.ensure_group()
        background = [asyncio.create_task(self._supervise(c)) for c in self.carriers.values()]
        background.append(asyncio.create_task(self._publish_state()))
        background.append(asyncio.create_task(self._reclaim_loop()))
        try:
            while self.running:
                for entry_id, fields in await self.store.read_batch(self.consumer):
                    await self._spawn(entry_id, fields)
        finally:
            if self._tasks:
                await asyncio.wait(self._tasks, timeout=30)
            for task in background:
                task.cancel()
            for carrier in self.carriers.values():
                await carrier.client.close()

    def stop(self):
        log.info("stopping: finishing in-flight messages")
        self.running = False

    async def _spawn(self, entry_id, fields):
        await self.concurrency.acquire()
        task = asyncio.create_task(self._process_entry(entry_id, fields))
        self._tasks.add(task)
        task.add_done_callback(lambda t: (self._tasks.discard(t), self.concurrency.release()))

    async def _supervise(self, carrier: Carrier):
        """Keep the SMPP session bound, reconnecting with exponential backoff."""
        delay = 1
        while True:
            try:
                await carrier.client.connect()
                delay = 1
                await self._publish_carrier(carrier)
                await carrier.client.closed.wait()
            except (OSError, asyncio.TimeoutError, SmppError) as exc:
                log.warning("cannot bind to %s: %s (retry in %ss)", carrier.name, exc, delay)
            await self._publish_carrier(carrier)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

    async def _publish_carrier(self, carrier: Carrier):
        """Expose carrier health as metrics and in Redis (for GET /api/v1/routes)."""
        state = carrier.state()
        metrics.CARRIER_CONNECTED.labels(carrier.name).set(int(state["connected"]))
        metrics.CARRIER_CIRCUIT_OPEN.labels(carrier.name).set(int(state["circuit"] == "open"))
        await self.store.publish_carrier_state(
            carrier.name, json.dumps({**state, "worker": self.consumer, "at": time.time()}))

    async def _publish_state(self):
        while True:
            for carrier in self.carriers.values():
                await self._publish_carrier(carrier)
            try:
                metrics.QUEUE_BACKLOG.set(await self.store.backlog())
            except Exception:  # stream may not exist yet
                pass
            await asyncio.sleep(5)

    async def _reclaim_loop(self):
        while True:
            await asyncio.sleep(30)
            for entry_id, fields in await self.store.reclaim_stale(self.consumer):
                log.warning("reclaimed stale message %s", fields)
                await self._spawn(entry_id, fields)

    # ---------- sending ----------
    async def _process_entry(self, entry_id, fields):
        message_id = (fields.get(b"id") or fields.get("id"))
        message_id = message_id.decode() if isinstance(message_id, bytes) else message_id
        try:
            await self.send(message_id)
        except Exception:
            log.exception("failed to process %s", message_id)
            return  # not acked: another worker will reclaim it
        await self.store.ack(entry_id)

    async def send(self, message_id):
        msg = await self.store.get(message_id)
        if msg is None or msg["status"] in FINAL_STATES or msg["status"] == "SUBMITTED":
            return  # expired, or already handled before a crash/redelivery
        route = self.router.match(msg["to"])
        if route is None:
            return await self._finish(msg, "FAILED", error="no route")

        data_coding, payload = encoding.encode(msg["text"])
        destination = msg["to"].lstrip("+")
        attempts = int(msg.get("attempts", 0))

        for attempt in range(self.max_attempts):
            for option in route.options:
                carrier = self.carriers[option.carrier]
                if not carrier.available:
                    continue
                attempts += 1
                await carrier.bucket.acquire()
                started = time.perf_counter()
                try:
                    carrier_id = await carrier.client.submit(msg["from"], destination,
                                                             data_coding, payload)
                except (SmppError, ConnectionError, asyncio.TimeoutError) as exc:
                    carrier.breaker.record_failure()
                    result = f"0x{exc.status:02x}" if isinstance(exc, SmppError) else "error"
                    metrics.SUBMITS.labels(carrier.name, route.name, result).inc()
                    metrics.FAILOVERS.labels(carrier.name).inc()
                    log.warning("submit %s via %s failed: %s", message_id, carrier.name, exc)
                    continue
                metrics.SUBMIT_LATENCY.labels(carrier.name).observe(time.perf_counter() - started)
                carrier.breaker.record_success()
                metrics.SUBMITS.labels(carrier.name, route.name, "ok").inc()
                metrics.SEGMENTS.labels(carrier.name, route.name).inc(int(msg["segments"]))
                await self.store.link_carrier_id(message_id, carrier.name, carrier_id)
                await self.store.update(message_id, status="SUBMITTED", carrier=carrier.name,
                                        carrier_message_id=carrier_id, attempts=attempts,
                                        submitted_at=time.time())
                return
            # every carrier for this route failed or is unavailable: back off and retry
            await asyncio.sleep(self.retry_base_delay * 2 ** attempt)
        await self._finish(msg, "FAILED", error="all carriers failed", attempts=attempts)

    async def _finish(self, msg, status, **fields):
        await self.store.update(msg["id"], status=status, done_at=time.time(), **fields)
        metrics.MESSAGES_FINAL.labels(msg.get("route", "none"), status).inc()

    # ---------- delivery receipts ----------
    async def on_deliver(self, carrier: Carrier, pdu):
        if not codec.is_receipt(pdu):
            log.info("ignoring mobile-originated message from %s", pdu.get("source_addr"))
            return
        receipt = codec.parse_receipt(pdu)
        message_id = await self.store.find_by_carrier_id(carrier.name, receipt.get("id"))
        if message_id is None:
            # the receipt can overtake our own bookkeeping by a few milliseconds
            await asyncio.sleep(0.5)
            message_id = await self.store.find_by_carrier_id(carrier.name, receipt.get("id"))
        if message_id is None:
            log.warning("receipt for unknown message %s from %s", receipt.get("id"), carrier.name)
            return
        msg = await self.store.get(message_id)
        stat = receipt.get("stat", "UNKNOWN")
        now = time.time()
        metrics.DLRS.labels(carrier.name, msg["route"], stat).inc()
        if msg.get("submitted_at"):
            metrics.DLR_LATENCY.labels(carrier.name).observe(now - float(msg["submitted_at"]))
        metrics.END_TO_END.labels(msg["route"]).observe(now - float(msg["created_at"]))
        await self._finish(msg, "DELIVERED" if stat == "DELIVRD" else "UNDELIVERED",
                           dlr_stat=stat, dlr_error=receipt.get("err", ""))


async def main_async():
    import redis.asyncio as redis

    config = config_loader.load()
    store = Store(redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0")))
    worker = Worker(config, store, concurrency=int(os.getenv("WORKER_CONCURRENCY", "50")))
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, worker.stop)
    await worker.run()


def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    start_http_server(int(os.getenv("METRICS_PORT", "9100")))
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
