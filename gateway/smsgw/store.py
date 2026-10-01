"""Message state in Redis and the outbound queue (a Redis Stream with a consumer group)."""
import time
import uuid

STREAM = "sms:outbound"
GROUP = "workers"
MESSAGE_TTL = 7 * 24 * 3600
CARRIER_STATE = "sms:carriers"

FINAL_STATES = {"DELIVERED", "UNDELIVERED", "FAILED"}


def _text(value):
    return value.decode() if isinstance(value, bytes) else value


def _decode_hash(data: dict) -> dict:
    return {_text(k): _text(v) for k, v in data.items()}


def _msg_key(message_id):
    return f"sms:msg:{message_id}"


def _carrier_ref_key(carrier, carrier_message_id):
    return f"sms:ref:{carrier}:{carrier_message_id}"


class Store:
    def __init__(self, redis):
        self.redis = redis

    async def ensure_group(self):
        try:
            await self.redis.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        except Exception as exc:  # BUSYGROUP: already exists
            if "BUSYGROUP" not in str(exc):
                raise

    async def enqueue(self, to, sender, text, encoding, segments, route) -> dict:
        message_id = uuid.uuid4().hex
        message = {
            "id": message_id,
            "to": to,
            "from": sender,
            "text": text,
            "encoding": encoding,
            "segments": segments,
            "route": route,
            "status": "ACCEPTED",
            "attempts": 0,
            "created_at": time.time(),
        }
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(_msg_key(message_id), mapping={k: str(v) for k, v in message.items()})
            pipe.expire(_msg_key(message_id), MESSAGE_TTL)
            pipe.xadd(STREAM, {"id": message_id}, maxlen=1_000_000, approximate=True)
            await pipe.execute()
        return message

    async def get(self, message_id) -> dict:
        data = await self.redis.hgetall(_msg_key(message_id))
        if not data:
            return None
        return _decode_hash(data)

    async def update(self, message_id, **fields):
        await self.redis.hset(_msg_key(message_id), mapping={k: str(v) for k, v in fields.items()})

    async def link_carrier_id(self, message_id, carrier, carrier_message_id):
        await self.redis.set(_carrier_ref_key(carrier, carrier_message_id), message_id,
                             ex=MESSAGE_TTL)

    async def find_by_carrier_id(self, carrier, carrier_message_id):
        return _text(await self.redis.get(_carrier_ref_key(carrier, carrier_message_id)))

    async def read_batch(self, consumer, count=50, block_ms=1000):
        resp = await self.redis.xreadgroup(GROUP, consumer, {STREAM: ">"}, count=count,
                                           block=block_ms)
        return [(entry_id, fields) for _, entries in resp or [] for entry_id, fields in entries]

    async def reclaim_stale(self, consumer, idle_ms=60_000, count=50):
        """Take over messages a crashed worker read but never acknowledged."""
        _, entries, _ = await self.redis.xautoclaim(STREAM, GROUP, consumer, idle_ms, "0-0",
                                                    count=count)
        return [(entry_id, fields) for entry_id, fields in entries if fields]

    async def ack(self, entry_id):
        await self.redis.xack(STREAM, GROUP, entry_id)

    async def backlog(self) -> int:
        """Messages waiting to be read plus messages read but not yet acknowledged."""
        groups = await self.redis.xinfo_groups(STREAM)
        for group in groups:
            if _text(group["name"]) == GROUP:
                return int(group.get("lag") or 0) + int(group["pending"])
        return 0

    async def publish_carrier_state(self, carrier, state: dict):
        await self.redis.hset(CARRIER_STATE, carrier, str(state))

    async def carrier_states(self) -> dict:
        raw = await self.redis.hgetall(CARRIER_STATE)
        return _decode_hash(raw)
