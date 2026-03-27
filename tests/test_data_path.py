import cbrrr
import pytest
from leb128 import u as leb

from goeoview.carfile import CARFileError, parse as parse_car
from goeoview.helpers import parse_cid
from goeoview.relay import (
    CommitValidationError,
    FirehoseProtocolError,
    parse_firehose_message_type,
)


def test_parse_firehose_message_type_rejects_unknown_prefix():
    with pytest.raises(FirehoseProtocolError):
        parse_firehose_message_type(b"not-a-firehose-packet")


def test_parse_firehose_message_type_recognizes_sync():
    header = cbrrr.encode_dag_cbor({"t": "#sync", "op": 1})
    body = cbrrr.encode_dag_cbor({"did": "did:plc:test", "rev": "abc"})
    msg_type, remainder = parse_firehose_message_type(header + body)
    assert msg_type == "sync"
    assert remainder == body


def test_decode_sync_header_extracts_fields():
    from goeoview.relay import _decode_sync_header

    message = cbrrr.encode_dag_cbor(
        {
            "did": "did:plc:test",
            "rev": "3mhpo2yjlqz2z",
            "seq": 99,
            "time": "2026-03-23T08:41:57.187Z",
            "blocks": b"\x00",
        }
    )

    header = _decode_sync_header(message)
    assert header["seq"] == 99
    assert header["did"] == "did:plc:test"
    assert header["rev"] == "3mhpo2yjlqz2z"
    assert header["time"] == "2026-03-23T08:41:57.187Z"


def test_parse_cid_rejects_invalid_multibase():
    with pytest.raises(ValueError):
        parse_cid("not-a-cid")


def test_carfile_rejects_short_block():
    header = cbrrr.encode_dag_cbor({"version": 1, "roots": []})
    car = leb.encode(len(header)) + header + leb.encode(2) + b"\x01\x02"
    with pytest.raises(CARFileError):
        parse_car(car)


def test_decode_commit_header_returns_none_for_toobig():
    from goeoview.relay import _decode_commit_header

    message = cbrrr.encode_dag_cbor(
        {
            "tooBig": True,
            "time": "2024-01-01T00:00:00+00:00",
            "seq": 10,
            "repo": "did:plc:test",
        }
    )

    assert _decode_commit_header(message) is None


def test_decode_commit_header_extracts_fields():
    from goeoview.relay import _decode_commit_header

    message = cbrrr.encode_dag_cbor(
        {
            "tooBig": False,
            "time": "2024-01-01T00:00:00+00:00",
            "seq": 42,
            "repo": "did:plc:test",
            "blocks": b"",
        }
    )

    header = _decode_commit_header(message)
    assert header["seq"] == 42
    assert header["repo"] == "did:plc:test"
    assert header["time"] == "2024-01-01T00:00:00+00:00"
