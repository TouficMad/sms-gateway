"""Asynchronous SMPP transceiver client (ESME side)."""
import asyncio
import itertools
import logging

from . import codec
from .codec import Cmd, Pdu, Status

log = logging.getLogger(__name__)


class SmppError(Exception):
    def __init__(self, status: int, message: str = ""):
        super().__init__(message or f"SMPP error status 0x{status:08x}")
        self.status = status


class SmppClient:
    """One bound transceiver session.

    Requests are matched to responses by sequence number, so many submit_sm can
    be in flight at once (SMPP "windowing"). Incoming deliver_sm PDUs are
    acknowledged and passed to `on_deliver`.
    """

    def __init__(self, host, port, system_id, password, on_deliver=None,
                 enquire_link_interval=30.0, response_timeout=10.0):
        self.host, self.port = host, port
        self.system_id, self.password = system_id, password
        self.on_deliver = on_deliver
        self.enquire_link_interval = enquire_link_interval
        self.response_timeout = response_timeout
        self._seq = itertools.count(1)
        self._pending = {}
        self._reader = self._writer = None
        self._tasks = []
        self.closed = asyncio.Event()
        self.closed.set()

    @property
    def connected(self) -> bool:
        return not self.closed.is_set()

    async def connect(self):
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port), self.response_timeout
        )
        self.closed.clear()
        self._tasks = [asyncio.create_task(self._read_loop())]
        try:
            await self.request(Pdu(Cmd.BIND_TRANSCEIVER, 0, params={
                "system_id": self.system_id,
                "password": self.password,
                "interface_version": 0x34,
            }))
        except Exception:
            await self.close()
            raise
        self._tasks.append(asyncio.create_task(self._keepalive()))
        log.info("bound to %s:%s as %s", self.host, self.port, self.system_id)

    async def request(self, pdu: Pdu) -> Pdu:
        if self.closed.is_set():
            raise ConnectionError("SMPP session is not connected")
        pdu.sequence = next(self._seq)
        future = asyncio.get_running_loop().create_future()
        self._pending[pdu.sequence] = future
        try:
            self._writer.write(codec.encode(pdu))
            await self._writer.drain()
            resp = await asyncio.wait_for(future, self.response_timeout)
        finally:
            self._pending.pop(pdu.sequence, None)
        if resp.status != Status.OK:
            raise SmppError(resp.status)
        return resp

    async def submit(self, source, destination, data_coding, payload: bytes,
                     registered_delivery=1) -> str:
        params = {
            "source_addr_ton": 5 if not source.isdigit() else 1,  # alphanumeric / international
            "source_addr_npi": 0 if not source.isdigit() else 1,
            "source_addr": source,
            "dest_addr_ton": 1,
            "dest_addr_npi": 1,
            "destination_addr": destination,
            "registered_delivery": registered_delivery,
            "data_coding": data_coding,
        }
        tlvs = {}
        if len(payload) > 254:
            tlvs[codec.Tag.MESSAGE_PAYLOAD] = payload  # long message in a single PDU
        else:
            params["short_message"] = payload
        resp = await self.request(Pdu(Cmd.SUBMIT_SM, 0, params=params, tlvs=tlvs))
        return resp.get("message_id")

    async def close(self):
        if self._writer and not self.closed.is_set():
            try:
                self._writer.write(codec.encode(Pdu(Cmd.UNBIND, next(self._seq))))
                await asyncio.wait_for(self._writer.drain(), 1)
            except (ConnectionError, asyncio.TimeoutError):
                pass
        self._shutdown()
        current = asyncio.current_task()
        for task in self._tasks:
            if task is not current:
                task.cancel()

    def _shutdown(self):
        self.closed.set()
        if self._writer:
            self._writer.close()
        for future in self._pending.values():
            if not future.done():
                future.set_exception(ConnectionError("SMPP session closed"))
        self._pending.clear()

    async def _send(self, pdu: Pdu):
        self._writer.write(codec.encode(pdu))
        await self._writer.drain()

    async def _read_loop(self):
        try:
            while True:
                pdu = await codec.read_pdu(self._reader)
                if pdu.is_response:
                    future = self._pending.get(pdu.sequence)
                    if future and not future.done():
                        future.set_result(pdu)
                elif pdu.command_id == Cmd.DELIVER_SM:
                    await self._send(codec.response_for(pdu))
                    if self.on_deliver:
                        try:
                            await self.on_deliver(pdu)
                        except Exception:
                            log.exception("deliver_sm handler failed")
                elif pdu.command_id == Cmd.ENQUIRE_LINK:
                    await self._send(codec.response_for(pdu))
                elif pdu.command_id == Cmd.UNBIND:
                    await self._send(codec.response_for(pdu))
                    break
                else:
                    await self._send(Pdu(Cmd.GENERIC_NACK, pdu.sequence, Status.INVALID_COMMAND))
        except (asyncio.IncompleteReadError, ConnectionError, codec.PduError) as exc:
            log.warning("SMPP connection to %s:%s lost: %s", self.host, self.port, exc)
        finally:
            self._shutdown()
            for task in self._tasks:
                if task is not asyncio.current_task():
                    task.cancel()

    async def _keepalive(self):
        while not self.closed.is_set():
            await asyncio.sleep(self.enquire_link_interval)
            try:
                await self.request(Pdu(Cmd.ENQUIRE_LINK, 0))
            except (ConnectionError, asyncio.TimeoutError, SmppError):
                log.warning("enquire_link to %s:%s failed", self.host, self.port)
                self._shutdown()
