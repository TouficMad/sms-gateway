"""SMPP v3.4 PDU encoding and decoding.

Every PDU is a 16-byte header (length, command_id, status, sequence) followed by
a body of mandatory parameters and optional TLVs. Bodies are described as field
lists so encoding and decoding share one definition.
"""
import re
import struct
from dataclasses import dataclass, field

HEADER = struct.Struct(">IIII")
MAX_PDU_LENGTH = 64 * 1024


class Cmd:
    GENERIC_NACK = 0x80000000
    BIND_TRANSCEIVER = 0x00000009
    BIND_TRANSCEIVER_RESP = 0x80000009
    SUBMIT_SM = 0x00000004
    SUBMIT_SM_RESP = 0x80000004
    DELIVER_SM = 0x00000005
    DELIVER_SM_RESP = 0x80000005
    UNBIND = 0x00000006
    UNBIND_RESP = 0x80000006
    ENQUIRE_LINK = 0x00000015
    ENQUIRE_LINK_RESP = 0x80000015


class Status:
    OK = 0x00000000
    INVALID_LENGTH = 0x00000001  # ESME_RINVMSGLEN
    INVALID_COMMAND = 0x00000003  # ESME_RINVCMDID
    INVALID_BIND_STATUS = 0x00000004  # ESME_RINVBNDSTS
    SYSTEM_ERROR = 0x00000008  # ESME_RSYSERR
    BIND_FAILED = 0x0000000D  # ESME_RBINDFAIL
    INVALID_PASSWORD = 0x0000000E  # ESME_RINVPASWD
    THROTTLED = 0x00000058  # ESME_RTHROTTLED


class Tag:
    RECEIPTED_MESSAGE_ID = 0x001E
    MESSAGE_STATE = 0x0427
    MESSAGE_PAYLOAD = 0x0424


# Field types: "c" = C-octet string (NUL terminated), "b" = 1 byte integer,
# "sm" = sm_length byte followed by that many bytes of short_message.
_SM_FIELDS = [
    ("service_type", "c"),
    ("source_addr_ton", "b"),
    ("source_addr_npi", "b"),
    ("source_addr", "c"),
    ("dest_addr_ton", "b"),
    ("dest_addr_npi", "b"),
    ("destination_addr", "c"),
    ("esm_class", "b"),
    ("protocol_id", "b"),
    ("priority_flag", "b"),
    ("schedule_delivery_time", "c"),
    ("validity_period", "c"),
    ("registered_delivery", "b"),
    ("replace_if_present_flag", "b"),
    ("data_coding", "b"),
    ("sm_default_msg_id", "b"),
    ("short_message", "sm"),
]

BODIES = {
    Cmd.BIND_TRANSCEIVER: [
        ("system_id", "c"),
        ("password", "c"),
        ("system_type", "c"),
        ("interface_version", "b"),
        ("addr_ton", "b"),
        ("addr_npi", "b"),
        ("address_range", "c"),
    ],
    Cmd.BIND_TRANSCEIVER_RESP: [("system_id", "c")],
    Cmd.SUBMIT_SM: _SM_FIELDS,
    Cmd.SUBMIT_SM_RESP: [("message_id", "c")],
    Cmd.DELIVER_SM: _SM_FIELDS,
    Cmd.DELIVER_SM_RESP: [("message_id", "c")],
}

_DEFAULTS = {"c": "", "b": 0, "sm": b""}


class PduError(Exception):
    """Raised when bytes on the wire are not a valid PDU."""


@dataclass
class Pdu:
    command_id: int
    sequence: int
    status: int = Status.OK
    params: dict = field(default_factory=dict)
    tlvs: dict = field(default_factory=dict)  # tag -> bytes

    @property
    def is_response(self) -> bool:
        return bool(self.command_id & 0x80000000)

    def get(self, name, default=None):
        return self.params.get(name, default)


def encode(pdu: Pdu) -> bytes:
    body = bytearray()
    # Error responses carry no body (SMPP 3.4 section 3.3).
    fields = BODIES.get(pdu.command_id, []) if not (pdu.is_response and pdu.status) else []
    for name, kind in fields:
        value = pdu.params.get(name, _DEFAULTS[kind])
        if kind == "c":
            body += value.encode("ascii") + b"\x00"
        elif kind == "b":
            body.append(value & 0xFF)
        else:
            if len(value) > 254:
                raise PduError("short_message longer than 254 octets, use message_payload TLV")
            body.append(len(value))
            body += value
    for tag, value in pdu.tlvs.items():
        body += struct.pack(">HH", tag, len(value)) + value
    return HEADER.pack(HEADER.size + len(body), pdu.command_id, pdu.status, pdu.sequence) + body


def decode(data: bytes) -> Pdu:
    if len(data) < HEADER.size:
        raise PduError("PDU shorter than header")
    length, command_id, status, sequence = HEADER.unpack_from(data)
    if length != len(data):
        raise PduError(f"command_length {length} does not match {len(data)} bytes")
    pdu = Pdu(command_id, sequence, status)
    pos = HEADER.size
    fields = BODIES.get(command_id, []) if not (pdu.is_response and status) else []
    try:
        for name, kind in fields:
            if pos >= length and pdu.is_response:
                break  # some SMSCs omit the body of a response
            if kind == "c":
                end = data.index(b"\x00", pos)
                pdu.params[name] = data[pos:end].decode("ascii", errors="replace")
                pos = end + 1
            elif kind == "b":
                pdu.params[name] = data[pos]
                pos += 1
            else:
                sm_length = data[pos]
                pdu.params[name] = bytes(data[pos + 1:pos + 1 + sm_length])
                pos += 1 + sm_length
        while pos + 4 <= length:
            tag, tlv_length = struct.unpack_from(">HH", data, pos)
            pdu.tlvs[tag] = bytes(data[pos + 4:pos + 4 + tlv_length])
            pos += 4 + tlv_length
    except (ValueError, IndexError) as exc:
        raise PduError(f"malformed body for command 0x{command_id:08x}") from exc
    return pdu


async def read_pdu(reader) -> Pdu:
    """Read exactly one PDU from an asyncio StreamReader."""
    header = await reader.readexactly(4)
    (length,) = struct.unpack(">I", header)
    if not HEADER.size <= length <= MAX_PDU_LENGTH:
        raise PduError(f"invalid command_length {length}")
    return decode(header + await reader.readexactly(length - 4))


def response_for(pdu: Pdu, status: int = Status.OK, **params) -> Pdu:
    return Pdu(pdu.command_id | 0x80000000, pdu.sequence, status, params)


# ---- delivery receipts (SMPP 3.4 Appendix B) ----

_RECEIPT = re.compile(r"(\w+(?: date)?):(\S*)")


def format_receipt(message_id, stat, err, submit_date, done_date, text="") -> str:
    delivered = "001" if stat == "DELIVRD" else "000"
    return (
        f"id:{message_id} sub:001 dlvrd:{delivered} "
        f"submit date:{submit_date:%y%m%d%H%M} done date:{done_date:%y%m%d%H%M} "
        f"stat:{stat} err:{err:03d} text:{text[:20]}"
    )


def parse_receipt(pdu: Pdu) -> dict:
    """Extract id/stat/err from a deliver_sm delivery receipt.

    The receipted_message_id TLV wins over the text, because some SMSCs format
    the id differently in the text (e.g. decimal vs hex).
    """
    text = pdu.params.get("short_message", b"").decode("latin-1")
    fields = {k.replace(" ", "_"): v for k, v in _RECEIPT.findall(text)}
    if Tag.RECEIPTED_MESSAGE_ID in pdu.tlvs:
        fields["id"] = pdu.tlvs[Tag.RECEIPTED_MESSAGE_ID].rstrip(b"\x00").decode("ascii")
    return fields


def is_receipt(pdu: Pdu) -> bool:
    # esm_class bits 2-5 = 0001 means "SMSC delivery receipt"
    return pdu.command_id == Cmd.DELIVER_SM and (pdu.params.get("esm_class", 0) & 0x3C) == 0x04
