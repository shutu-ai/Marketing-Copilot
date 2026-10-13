import pytest
import smpplib.consts
import smpplib.gsm

from services import smpp_gateway
from services.smpp_gateway import (
    _encode_utf16be, _make_parts, _make_task_parts, _make_ucs2_parts,
)


def test_ascii_uses_smpplib_make_parts():
    assert _make_parts("Hello SMPP") == smpplib.gsm.make_parts("Hello SMPP")


def test_flash_ascii_uses_gsm_class_zero_data_coding():
    parts, coding, esm_class = _make_task_parts({"content": "Hello SMPP", "flash_sms": True})

    assert parts == [b"Hello SMPP"]
    assert coding == 0xF0
    assert esm_class == smpplib.consts.SMPP_MSGTYPE_DEFAULT


def test_flash_ucs2_uses_unicode_class_zero_data_coding():
    parts, coding, esm_class = _make_task_parts({"content": "你好", "flash_sms": True})

    assert parts == [b"O`Y}"]
    assert coding == 0x18
    assert esm_class == smpplib.consts.SMPP_MSGTYPE_DEFAULT


def test_normal_task_keeps_standard_data_coding():
    _parts, coding, _esm_class = _make_task_parts({"content": "你好"})

    assert coding == smpplib.consts.SMPP_ENCODING_ISO10646


@pytest.mark.parametrize("text", [
    "Hello SMPP",
    "a" * 161,
    "እንደምን አለህ",
    "你" * 71 + "😀",
])
def test_flash_changes_only_data_coding_for_parts(text, monkeypatch):
    monkeypatch.setattr(smpp_gateway.random, "randint", lambda _start, _end: 7)
    normal_parts, normal_coding, normal_esm_class = _make_task_parts({"content": text})
    flash_parts, flash_coding, flash_esm_class = _make_task_parts(
        {"content": text, "flash_sms": True}
    )

    assert flash_parts == normal_parts
    assert flash_esm_class == normal_esm_class
    assert flash_coding != normal_coding
    assert flash_coding == (0x18 if normal_coding == smpplib.consts.SMPP_ENCODING_ISO10646 else 0xF0)


def test_manual_utf16be_encodes_bmp_and_surrogate_pair():
    assert _encode_utf16be("中😀") == b"\x4e\x2d\xd8\x3d\xde\x00"
    assert _encode_utf16be("中\ud83d\ude00") == b"\x4e\x2d\xd8\x3d\xde\x00"


def test_manual_utf16be_rejects_unpaired_surrogate():
    with pytest.raises(UnicodeEncodeError):
        _encode_utf16be("bad\ud800")


def test_codec_missing_uses_ucs2_fallback(monkeypatch, caplog):
    def missing_codec(_text):
        raise LookupError("unknown encoding: utf-16-be")

    monkeypatch.setattr(smpplib.gsm, "make_parts", missing_codec)
    with caplog.at_level("WARNING"):
        parts, coding, esm_class = _make_parts("中文😀")

    assert parts == [_encode_utf16be("中文😀")]
    assert coding == smpplib.consts.SMPP_ENCODING_ISO10646
    assert esm_class == smpplib.consts.SMPP_MSGTYPE_DEFAULT
    assert "SMPP UCS-2 fallback" in caplog.text


def test_long_ucs2_fallback_uses_udh_and_keeps_surrogate_pair(monkeypatch):
    def missing_codec(_text):
        raise LookupError("unknown encoding: utf-16-be")

    monkeypatch.setattr(smpplib.gsm, "make_parts", missing_codec)
    text = "中" * 66 + "😀" + "文" * 3
    parts, coding, esm_class = _make_parts(text)

    assert len(parts) == 2
    assert coding == smpplib.consts.SMPP_ENCODING_ISO10646
    assert esm_class == smpplib.consts.SMPP_GSMFEAT_UDHI
    reference = parts[0][3]
    assert all(part[:3] == b"\x05\x00\x03" for part in parts)
    assert all(part[3] == reference for part in parts)
    assert [part[4] for part in parts] == [2, 2]
    assert [part[5] for part in parts] == [1, 2]
    assert all(len(part[6:]) <= smpplib.consts.UCS2_PART_SIZE for part in parts)
    payload = b"".join(part[6:] for part in parts)
    assert payload == _encode_utf16be(text)
    assert parts[0][-2:] != b"\xd8\x3d"


def test_non_utf16be_lookup_error_is_reraised(monkeypatch):
    error = LookupError("unknown encoding: custom-codec")

    def other_missing_codec(_text):
        raise error

    monkeypatch.setattr(smpplib.gsm, "make_parts", other_missing_codec)
    with pytest.raises(LookupError) as raised:
        _make_parts("中文")

    assert raised.value is error


def test_ucs2_fallback_rejects_more_than_255_parts(monkeypatch):
    def missing_codec(_text):
        raise LookupError("unknown encoding: utf-16-be")

    monkeypatch.setattr(smpplib.gsm, "make_parts", missing_codec)
    with pytest.raises(smpplib.gsm.exceptions.MessageTooLong):
        _make_parts("中" * (smpplib.consts.UCS2_PART_SIZE // 2 * 256))
