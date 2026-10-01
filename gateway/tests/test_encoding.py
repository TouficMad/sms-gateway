import pytest

from smsgw import encoding


@pytest.mark.parametrize("text,expected", [
    ("Hello", {"encoding": "GSM7", "units": 5, "segments": 1}),
    ("x" * 160, {"encoding": "GSM7", "units": 160, "segments": 1}),
    ("x" * 161, {"encoding": "GSM7", "units": 161, "segments": 2}),
    ("€" * 80, {"encoding": "GSM7", "units": 160, "segments": 1}),  # extension = 2 septets
    ("مرحبا", {"encoding": "UCS2", "units": 5, "segments": 1}),
    ("ب" * 71, {"encoding": "UCS2", "units": 71, "segments": 2}),
    ("Hi 😀", {"encoding": "UCS2", "units": 5, "segments": 1}),  # emoji = surrogate pair
])
def test_analyse(text, expected):
    assert encoding.analyse(text) == expected


@pytest.mark.parametrize("text", ["Hello @ £5 {ok} €", "Ελλάδα? ΔΦΓ", "مرحبا بك", "Ünïcødé ✓"])
def test_encode_decode_roundtrip(text):
    assert encoding.decode(*encoding.encode(text)) == text


def test_gsm7_is_unpacked_septets():
    assert encoding.encode("@A€") == (0x00, bytes([0x00, 0x41, 0x1B, 0x65]))
