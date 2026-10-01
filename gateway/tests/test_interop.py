"""Checks the simulator against smpplib, an independent SMPP implementation."""
import asyncio
import threading

import smpplib.client
import smpplib.consts

from smsgw.simulator import Faults, Smsc


def test_smpplib_client_gets_submit_resp_and_receipt():
    result = {}

    def client(port, done):
        c = smpplib.client.Client("127.0.0.1", port, allow_unknown_opt_params=True)
        try:
            c.connect()
            c.bind_transceiver(system_id="gateway", password="secret")
            c.send_message(source_addr_ton=smpplib.consts.SMPP_TON_INTL, source_addr="96170000000",
                           dest_addr_ton=smpplib.consts.SMPP_TON_INTL,
                           destination_addr="447700900123", short_message=b"Hello",
                           registered_delivery=True)
            result["resp"] = c.read_pdu()
            result["dlr"] = c.read_pdu()
            c.unbind()
            c.disconnect()
        finally:
            done.set()

    async def run():
        smsc = Smsc(faults=Faults(delivery_rate=1.0, dlr_delay_seconds=0.05))
        server = await asyncio.start_server(smsc.handle, "127.0.0.1", 0)
        done = threading.Event()
        thread = threading.Thread(target=client, args=(server.sockets[0].getsockname()[1], done))
        thread.start()
        while not done.is_set():
            await asyncio.sleep(0.02)
        server.close()

    asyncio.run(run())
    message_id = result["resp"].message_id
    assert result["resp"].status == 0 and message_id
    assert result["dlr"].command == "deliver_sm"
    assert result["dlr"].receipted_message_id == message_id
    assert b"stat:DELIVRD" in result["dlr"].short_message


def test_bad_password_is_rejected():
    async def run():
        from smsgw.smpp.client import SmppClient, SmppError

        smsc = Smsc()
        server = await asyncio.start_server(smsc.handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        client = SmppClient("127.0.0.1", port, "gateway", "wrong")
        try:
            await client.connect()
        except SmppError as exc:
            return exc.status
        finally:
            server.close()

    assert asyncio.run(run()) == 0x0E  # ESME_RINVPASWD
