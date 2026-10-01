"""SMS character encoding: GSM 03.38 7-bit default alphabet vs UCS-2, and segment counting."""
import math

# GSM 03.38 basic character set, index = septet value
GSM_BASIC = (
    "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞ\x1bÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?"
    "¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà"
)
# Extension table, reached with the 0x1B escape: each costs two septets
GSM_EXTENSION = {
    "\f": 0x0A, "^": 0x14, "{": 0x28, "}": 0x29, "\\": 0x2F,
    "[": 0x3C, "~": 0x3D, "]": 0x3E, "|": 0x40, "€": 0x65,
}
_BASIC_INDEX = {ch: i for i, ch in enumerate(GSM_BASIC) if ch != "\x1b"}
_EXT_REVERSE = {v: k for k, v in GSM_EXTENSION.items()}

DATA_CODING_GSM7 = 0x00
DATA_CODING_UCS2 = 0x08


def is_gsm7(text: str) -> bool:
    return all(ch in _BASIC_INDEX or ch in GSM_EXTENSION for ch in text)


def analyse(text: str) -> dict:
    """Return encoding, length in characters/units and number of SMS segments."""
    if is_gsm7(text):
        units = sum(2 if ch in GSM_EXTENSION else 1 for ch in text)
        single, multi, encoding = 160, 153, "GSM7"
    else:
        units = len(text.encode("utf-16-be")) // 2
        single, multi, encoding = 70, 67, "UCS2"
    segments = 1 if units <= single else math.ceil(units / multi)
    return {"encoding": encoding, "units": units, "segments": segments}


def encode(text: str) -> tuple:
    """Encode text for SMPP. Returns (data_coding, bytes).

    GSM7 is sent unpacked (one septet per octet), which is what SMPP expects
    for data_coding 0x00.
    """
    if is_gsm7(text):
        out = bytearray()
        for ch in text:
            if ch in _BASIC_INDEX:
                out.append(_BASIC_INDEX[ch])
            else:
                out += bytes((0x1B, GSM_EXTENSION[ch]))
        return DATA_CODING_GSM7, bytes(out)
    return DATA_CODING_UCS2, text.encode("utf-16-be")


def decode(data_coding: int, payload: bytes) -> str:
    if data_coding == DATA_CODING_UCS2:
        return payload.decode("utf-16-be", errors="replace")
    out, escape = [], False
    for b in payload:
        if escape:
            out.append(_EXT_REVERSE.get(b, "?"))
            escape = False
        elif b == 0x1B:
            escape = True
        elif b < len(GSM_BASIC):
            out.append(GSM_BASIC[b])
        else:
            out.append("?")
    return "".join(out)
