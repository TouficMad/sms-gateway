"""Full flow: API -> Redis stream -> worker -> SMPP -> simulated carriers -> DLR -> status."""
import asyncio
import contextlib

import httpx
import pytest
import redis.asyncio as redis

from smsgw.api import create_app
from smsgw.routing import Router
from smsgw.simulator import Faults, Smsc
from smsgw.store import Store
from smsgw.worker import Worker

ROUTES = [
    {"name": "uk", "prefix": "44", "carriers": [{"carrier": "carrier-a", "cost": 0.028},
                                                 {"carrier": "carrier-b", "cost": 0.030}]},
    {"name": "lebanon", "prefix": "961", "carriers": [{"carrier": "carrier-b", "cost": 0.031},
                                                       {"carrier": "carrier-a", "cost": 0.035}]},
]
FAST = dict(dlr_delay_seconds=0.05, delivery_rate=1.0)


@contextlib.asynccontextmanager
async def gateway(redis_url, faults_a=None, faults_b=None, api_keys=frozenset()):
    conn = redis.from_url(redis_url)
    await conn.flushdb()
    store = Store(conn)
    smscs, servers, carriers = {}, [], {}
    for name, faults in (("carrier-a", faults_a), ("carrier-b", faults_b)):
        smsc = Smsc(name=name, faults=faults or Faults(**FAST))
        server = await asyncio.start_server(smsc.handle, "127.0.0.1", 0)
        smscs[name] = smsc
        servers.append(server)
        carriers[name] = {"host": "127.0.0.1", "port": server.sockets[0].getsockname()[1],
                          "system_id": "gateway", "password": "secret", "tps": 500,
                          "failure_threshold": 3, "reset_after_seconds": 60}
    config = {"carriers": carriers, "routes": ROUTES}
    worker = Worker(config, store, consumer="test", retry_base_delay=0.05, max_attempts=2)
    worker_task = asyncio.create_task(worker.run())
    for _ in range(100):  # wait until both sessions are bound (or refused)
        if all(c.client.connected or smscs[n].faults.down for n, c in worker.carriers.items()):
            break
        await asyncio.sleep(0.02)

    app = create_app(store=store, router=Router(config), api_keys=set(api_keys))
    await store.ensure_group()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw") as client:
        try:
            yield client, smscs, worker
        finally:
            worker.stop()
            await asyncio.wait_for(worker_task, 10)
            for server in servers:
                server.close()
            await conn.aclose()


async def send(client, to, text="Hello", sender="TOUFIC", **headers):
    resp = await client.post("/api/v1/messages", json={"to": to, "from": sender, "text": text},
                             headers=headers)
    assert resp.status_code == 202, resp.text
    return resp.json()


async def wait_final(client, message_id, timeout=10):
    for _ in range(int(timeout / 0.05)):
        msg = (await client.get(f"/api/v1/messages/{message_id}")).json()
        if msg["status"] in ("DELIVERED", "UNDELIVERED", "FAILED"):
            return msg
        await asyncio.sleep(0.05)
    raise AssertionError(f"message stuck in {msg['status']}")


def run(coro):
    return asyncio.run(coro)


def test_messages_are_delivered_via_cheapest_carrier(redis_url):
    async def scenario():
        async with gateway(redis_url) as (client, smscs, _):
            uk = await send(client, "+447700900123")
            lb = await send(client, "+96170123456", text="مرحبا من بيروت")
            assert lb["encoding"] == "UCS2"
            uk, lb = await wait_final(client, uk["id"]), await wait_final(client, lb["id"])
            assert (uk["status"], uk["carrier"]) == ("DELIVERED", "carrier-a")
            assert (lb["status"], lb["carrier"]) == ("DELIVERED", "carrier-b")
            assert lb["dlr_stat"] == "DELIVRD"
    run(scenario())


def test_failover_when_carrier_is_down(redis_url):
    async def scenario():
        async with gateway(redis_url, faults_a=Faults(down=True, **FAST)) as (client, *_):
            msg = await wait_final(client, (await send(client, "+447700900123"))["id"])
            assert (msg["status"], msg["carrier"]) == ("DELIVERED", "carrier-b")
    run(scenario())


def test_submit_errors_fail_over_and_open_circuit(redis_url):
    async def scenario():
        faults_b = Faults(submit_error_rate=1.0, **FAST)
        async with gateway(redis_url, faults_b=faults_b) as (client, _, worker):
            ids = [(await send(client, "+96170123456"))["id"] for _ in range(5)]
            for message_id in ids:
                msg = await wait_final(client, message_id)
                assert (msg["status"], msg["carrier"]) == ("DELIVERED", "carrier-a")
            assert worker.carriers["carrier-b"].breaker.state == "open"
    run(scenario())


def test_undelivered_receipt(redis_url):
    async def scenario():
        faults_a = Faults(dlr_delay_seconds=0.05, delivery_rate=0.0)
        async with gateway(redis_url, faults_a=faults_a) as (client, *_):
            msg = await wait_final(client, (await send(client, "+447700900123"))["id"])
            assert msg["status"] == "UNDELIVERED"
            assert msg["dlr_stat"] in ("UNDELIV", "EXPIRED", "REJECTD")
    run(scenario())


def test_failed_when_every_carrier_is_down(redis_url):
    async def scenario():
        down = Faults(down=True, **FAST)
        async with gateway(redis_url, faults_a=down, faults_b=down) as (client, *_):
            msg = await wait_final(client, (await send(client, "+447700900123"))["id"])
            assert (msg["status"], msg["error"]) == ("FAILED", "all carriers failed")
    run(scenario())


def test_long_message_uses_message_payload(redis_url):
    async def scenario():
        async with gateway(redis_url) as (client, *_):
            accepted = await send(client, "+447700900123", text="A" * 400)
            assert accepted["segments"] == 3
            assert (await wait_final(client, accepted["id"]))["status"] == "DELIVERED"
    run(scenario())


@pytest.mark.parametrize("payload,status", [
    ({"to": "12345", "from": "TOUFIC", "text": "x"}, 422),          # not E.164
    ({"to": "+447700900123", "from": "WAY TOO LONG SENDER", "text": "x"}, 422),
    ({"to": "+447700900123", "from": "TOUFIC", "text": ""}, 422),
    ({"to": "+12025550123", "from": "TOUFIC", "text": "x"}, 422),   # no route to +1
])
def test_validation(redis_url, payload, status):
    async def scenario():
        async with gateway(redis_url) as (client, *_):
            assert (await client.post("/api/v1/messages", json=payload)).status_code == status
    run(scenario())


def test_api_key_required_when_configured(redis_url):
    async def scenario():
        async with gateway(redis_url, api_keys={"k1"}) as (client, *_):
            body = {"to": "+447700900123", "from": "TOUFIC", "text": "x"}
            assert (await client.post("/api/v1/messages", json=body)).status_code == 401
            await send(client, "+447700900123", **{"X-API-Key": "k1"})
    run(scenario())


def test_routes_endpoint_shows_carrier_state(redis_url):
    async def scenario():
        async with gateway(redis_url) as (client, *_):
            await asyncio.sleep(0.2)  # first state publish
            routes = (await client.get("/api/v1/routes")).json()
            uk = next(r for r in routes if r["name"] == "uk")
            assert uk["carriers"][0]["carrier"] == "carrier-a"
            assert uk["carriers"][0]["connected"] is True
    run(scenario())


def test_receipts_queued_while_disconnected_arrive_after_rebind(redis_url):
    async def scenario():
        slow = Faults(dlr_delay_seconds=0.5, delivery_rate=1.0)
        async with gateway(redis_url, faults_a=slow) as (client, smscs, worker):
            accepted = await send(client, "+447700900123")
            for _ in range(100):
                msg = (await client.get(f"/api/v1/messages/{accepted['id']}")).json()
                if msg["status"] == "SUBMITTED":
                    break
                await asyncio.sleep(0.02)
            smscs["carrier-a"].drop_sessions()  # link drops before the receipt is sent
            msg = await wait_final(client, accepted["id"], timeout=15)  # worker re-binds in ~1s
            assert (msg["status"], msg["carrier"]) == ("DELIVERED", "carrier-a")
            assert smscs["carrier-a"].stats["binds"] == 2
    run(scenario())
