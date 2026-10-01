"""Simulated carrier SMSC: speaks SMPP 3.4 and sends delivery receipts.

Behaviour is configurable through environment variables and can be changed at
runtime through a small HTTP control API, to demo failures and failover:

    PUT /faults {"down": true}                 # drop sessions and refuse binds
    PUT /faults {"delivery_rate": 0.3}         # most messages UNDELIV
    PUT /faults {"submit_error_rate": 0.5}     # half of submit_sm fail
"""
import asyncio
import logging
import os
import random
import uuid
from collections import deque
from datetime import datetime, timezone

from fastapi import FastAPI
from pydantic import BaseModel

from . import encoding
from .smpp import codec
from .smpp.codec import Cmd, Pdu, Status, Tag

log = logging.getLogger("smsc")

FAILED_STATES = [("UNDELIV", 1), ("EXPIRED", 27), ("REJECTD", 11)]


class Faults(BaseModel):
    down: bool = False
    delivery_rate: float = 0.95
    submit_error_rate: float = 0.0
    dlr_delay_seconds: float = 1.0
    tps: int = 200


class Smsc:
    def __init__(self, name="smsc", system_id="gateway", password="secret", faults=None,
                 rng=None):
        self.name = name
        self.system_id, self.password = system_id, password
        self.faults = faults or Faults()
        self.rng = rng or random.Random()
        self.sessions = set()  # bound sessions
        self.queued_receipts = deque(maxlen=100_000)  # receipts waiting for a bound session
        self.stats = {"binds": 0, "submitted": 0, "rejected": 0, "delivered": 0, "failed": 0}
        self._window_start, self._window_count = 0.0, 0

    # ---- SMPP server ----
    async def handle(self, reader, writer):
        bound = False
        try:
            while True:
                pdu = await codec.read_pdu(reader)
                if pdu.command_id == Cmd.BIND_TRANSCEIVER:
                    status = self._check_bind(pdu)
                    bound = status == Status.OK
                    await self._send(writer, codec.response_for(pdu, status, system_id=self.name))
                    if not bound:
                        break
                    self.stats["binds"] += 1
                    self.sessions.add(writer)
                    await self._flush_receipts()
                elif pdu.command_id == Cmd.ENQUIRE_LINK:
                    await self._send(writer, codec.response_for(pdu))
                elif pdu.command_id == Cmd.UNBIND:
                    await self._send(writer, codec.response_for(pdu))
                    break
                elif pdu.command_id == Cmd.SUBMIT_SM:
                    if not bound:
                        resp = codec.response_for(pdu, Status.INVALID_BIND_STATUS)
                        await self._send(writer, resp)
                        continue
                    await self._submit(writer, pdu)
                elif pdu.is_response:
                    continue  # deliver_sm_resp / enquire_link_resp
                else:
                    await self._send(writer, Pdu(Cmd.GENERIC_NACK, pdu.sequence,
                                                 Status.INVALID_COMMAND))
        except (asyncio.IncompleteReadError, ConnectionError, codec.PduError):
            pass
        finally:
            self.sessions.discard(writer)
            writer.close()

    def _check_bind(self, pdu):
        if self.faults.down:
            return Status.BIND_FAILED
        if pdu.get("system_id") != self.system_id or pdu.get("password") != self.password:
            return Status.INVALID_PASSWORD
        return Status.OK

    def _throttled(self):
        now = asyncio.get_running_loop().time()
        if now - self._window_start >= 1:
            self._window_start, self._window_count = now, 0
        self._window_count += 1
        return self._window_count > self.faults.tps

    async def _submit(self, writer, pdu):
        if self._throttled():
            self.stats["rejected"] += 1
            return await self._send(writer, codec.response_for(pdu, Status.THROTTLED))
        if self.rng.random() < self.faults.submit_error_rate:
            self.stats["rejected"] += 1
            return await self._send(writer, codec.response_for(pdu, Status.SYSTEM_ERROR))

        message_id = uuid.uuid4().hex[:16]
        self.stats["submitted"] += 1
        await self._send(writer, codec.response_for(pdu, message_id=message_id))

        payload = pdu.tlvs.get(Tag.MESSAGE_PAYLOAD) or pdu.get("short_message")
        text = encoding.decode(pdu.get("data_coding"), payload)
        log.info("%s accepted %s -> %s: %r", self.name, message_id, pdu.get("destination_addr"),
                 text[:40])
        if pdu.get("registered_delivery", 0) & 0x01:
            task = asyncio.create_task(self._deliver_receipt(writer, pdu, message_id, text))
            task.add_done_callback(self._log_task_error)

    async def _deliver_receipt(self, writer, submit, message_id, text):
        submitted = datetime.now(timezone.utc)
        delay = self.rng.expovariate(1 / self.faults.dlr_delay_seconds)
        await asyncio.sleep(min(delay, 30))
        if self.rng.random() < self.faults.delivery_rate:
            stat, err = "DELIVRD", 0
            self.stats["delivered"] += 1
        else:
            stat, err = self.rng.choice(FAILED_STATES)
            self.stats["failed"] += 1
        receipt = codec.format_receipt(message_id, stat, err, submitted,
                                       datetime.now(timezone.utc), text)
        dlr = Pdu(Cmd.DELIVER_SM, self.rng.randint(1, 0x7FFFFFFF), params={
            "source_addr_ton": 1, "source_addr_npi": 1,
            "source_addr": submit.get("destination_addr"),
            "dest_addr_ton": submit.get("source_addr_ton"),
            "dest_addr_npi": submit.get("source_addr_npi"),
            "destination_addr": submit.get("source_addr"),
            "esm_class": 0x04,
            # the receipt quotes the start of the message; non-ASCII becomes "?"
            "short_message": receipt.encode("ascii", errors="replace"),
        }, tlvs={Tag.RECEIPTED_MESSAGE_ID: message_id.encode("ascii") + b"\x00"})
        await self._send_receipt(dlr, prefer=writer)

    async def _send_receipt(self, dlr, prefer=None):
        """Like a real SMSC: use the submitting session, any other bound session, or queue it."""
        for writer in ([prefer] if prefer in self.sessions else []) + list(self.sessions):
            if writer.is_closing():
                self.sessions.discard(writer)
                continue
            try:
                return await self._send(writer, dlr)
            except ConnectionError:
                self.sessions.discard(writer)
        self.queued_receipts.append(dlr)

    async def _flush_receipts(self):
        while self.queued_receipts and self.sessions:
            await self._send_receipt(self.queued_receipts.popleft())

    @staticmethod
    def _log_task_error(task):
        if not task.cancelled() and task.exception():
            log.error("delivery receipt failed", exc_info=task.exception())

    @staticmethod
    async def _send(writer, pdu):
        writer.write(codec.encode(pdu))
        await writer.drain()

    def drop_sessions(self):
        for writer in list(self.sessions):
            self.sessions.discard(writer)
            writer.close()

    # ---- control API ----
    def control_app(self) -> FastAPI:
        app = FastAPI(title=f"{self.name} control")

        @app.get("/health")
        def health():
            return {"status": "down" if self.faults.down else "ok"}

        @app.get("/stats")
        def stats():
            return {"name": self.name, "sessions": len(self.sessions),
                    "queued_receipts": len(self.queued_receipts), **self.stats,
                    "faults": self.faults.model_dump()}

        @app.put("/faults")
        def set_faults(update: dict):
            self.faults = Faults(**{**self.faults.model_dump(), **update})
            if self.faults.down:
                self.drop_sessions()
            log.warning("%s faults now %s", self.name, self.faults)
            return self.faults

        @app.delete("/faults")
        def reset_faults():
            self.faults = _faults_from_env()
            return self.faults

        return app


def _faults_from_env() -> Faults:
    return Faults(
        delivery_rate=float(os.getenv("DELIVERY_RATE", "0.95")),
        submit_error_rate=float(os.getenv("SUBMIT_ERROR_RATE", "0")),
        dlr_delay_seconds=float(os.getenv("DLR_DELAY_SECONDS", "1.0")),
        tps=int(os.getenv("TPS", "200")),
    )


async def serve(smsc: Smsc, smpp_port: int, http_port: int):
    import uvicorn

    server = await asyncio.start_server(smsc.handle, "0.0.0.0", smpp_port)
    log.info("%s listening for SMPP on :%d, control API on :%d", smsc.name, smpp_port, http_port)
    http = uvicorn.Server(uvicorn.Config(smsc.control_app(), host="0.0.0.0", port=http_port,
                                         log_level="warning"))
    async with server:
        await asyncio.gather(server.serve_forever(), http.serve())


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    smsc = Smsc(
        name=os.getenv("SMSC_NAME", "smsc"),
        system_id=os.getenv("SMPP_SYSTEM_ID", "gateway"),
        password=os.getenv("SMPP_PASSWORD", "secret"),
        faults=_faults_from_env(),
    )
    smpp_port, http_port = int(os.getenv("SMPP_PORT", "2775")), int(os.getenv("HTTP_PORT", "8000"))
    asyncio.run(serve(smsc, smpp_port, http_port))


if __name__ == "__main__":
    main()
