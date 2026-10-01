from datetime import datetime

import pytest

from smsgw.smpp import codec
from smsgw.smpp.codec import Cmd, Pdu, Status, Tag


def roundtrip(pdu):
    return codec.decode(codec.encode(pdu))


def test_enquire_link_bytes_match_spec():
    assert codec.encode(Pdu(Cmd.ENQUIRE_LINK, 7)) == bytes.fromhex(
        "00000010" "00000015" "00000000" "00000007"
    )


def test_bind_transceiver_roundtrip():
    pdu = Pdu(Cmd.BIND_TRANSCEIVER, 1, params={
        "system_id": "gateway", "password": "secret", "interface_version": 0x34})
    decoded = roundtrip(pdu)
    assert decoded.get("system_id") == "gateway"
    assert decoded.get("password") == "secret"
    assert decoded.get("interface_version") == 0x34


def test_submit_sm_roundtrip_with_tlv():
    pdu = Pdu(Cmd.SUBMIT_SM, 42, params={
        "source_addr": "TOUFIC", "destination_addr": "447700900123",
        "registered_delivery": 1, "data_coding": 8, "short_message": "مرحبا".encode("utf-16-be"),
    }, tlvs={Tag.MESSAGE_PAYLOAD: b"x" * 300})
    decoded = roundtrip(pdu)
    assert decoded.sequence == 42
    assert decoded.get("destination_addr") == "447700900123"
    assert decoded.get("short_message").decode("utf-16-be") == "مرحبا"
    assert decoded.tlvs[Tag.MESSAGE_PAYLOAD] == b"x" * 300


def test_error_response_has_no_body():
    resp = codec.response_for(Pdu(Cmd.SUBMIT_SM, 3), Status.THROTTLED)
    data = codec.encode(resp)
    assert len(data) == 16
    assert codec.decode(data).status == Status.THROTTLED


def test_long_short_message_rejected():
    with pytest.raises(codec.PduError):
        codec.encode(Pdu(Cmd.SUBMIT_SM, 1, params={"short_message": b"x" * 255}))


def test_decode_rejects_bad_length():
    data = codec.encode(Pdu(Cmd.ENQUIRE_LINK, 1))
    with pytest.raises(codec.PduError):
        codec.decode(data + b"\x00")


def test_receipt_format_and_parse():
    text = codec.format_receipt("abc123", "UNDELIV", 1, datetime(2026, 1, 2, 3, 4),
                                datetime(2026, 1, 2, 3, 5), "hello")
    assert text.startswith("id:abc123 sub:001 dlvrd:000 submit date:2601020304")
    pdu = Pdu(Cmd.DELIVER_SM, 1, params={"esm_class": 0x04, "short_message": text.encode()})
    assert codec.is_receipt(pdu)
    fields = codec.parse_receipt(pdu)
    assert (fields["id"], fields["stat"], fields["err"]) == ("abc123", "UNDELIV", "001")


def test_receipt_tlv_wins_over_text():
    pdu = Pdu(Cmd.DELIVER_SM, 1,
              params={"esm_class": 0x04, "short_message": b"id:999 stat:DELIVRD"},
              tlvs={Tag.RECEIPTED_MESSAGE_ID: b"3e7\x00"})
    assert codec.parse_receipt(pdu)["id"] == "3e7"
