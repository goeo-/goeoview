from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import orjson
import pytest

from goeoview.config import Config


@pytest.mark.asyncio
async def test_backfill_export_updates_cursor(tmp_path):
    from goeoview.plc import backfill_export
    from goeoview.plc_store import PLCStateStore

    mock_db = MagicMock()
    mock_db.db.insert = AsyncMock()

    record = {
        "did": "did:plc:test123",
        "cid": "bafyreidummydummydummydummydummydummydummydummydummy",
        "createdAt": "2024-01-01T00:00:00.000Z",
        "seq": 7,
        "operation": {
            "prev": None,
            "rotationKeys": ["did:key:zDummy"],
            "verificationMethods": {"atproto": "did:key:zSigning"},
            "alsoKnownAs": ["at://alice.test"],
            "services": {
                "atproto_pds": {
                    "type": "AtprotoPersonalDataServer",
                    "endpoint": "https://pds.example.com",
                }
            },
            "sig": "AAAA",
        },
    }

    response_body_data = orjson.dumps(record) + b"\n"
    response_body_empty = b""

    @asynccontextmanager
    async def _resp(status, body):
        resp = MagicMock(status=status)
        resp.read = AsyncMock(return_value=body)
        yield resp

    call_count = 0

    @asynccontextmanager
    async def _fake_get(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            async with _resp(200, response_body_data) as r:
                yield r
        else:
            async with _resp(200, response_body_empty) as r:
                yield r

    store = PLCStateStore(str(tmp_path / "plc_state.db"))
    config = Config()

    mock_http = MagicMock()
    mock_session = MagicMock()
    mock_session.get = _fake_get
    mock_http.trusted.return_value = mock_session

    with patch("goeoview.plc.fetch_prev_records", new=AsyncMock(return_value={})):
        with patch("goeoview.plc.validate_records", return_value=[]):
            cursor = await backfill_export(mock_db, MagicMock(), 0, config, mock_http, store)

    assert cursor == 7
    assert await store.get_cursor() == 7


@pytest.mark.asyncio
async def test_backfill_export_accepts_timestamp_cursor(tmp_path):
    from goeoview.plc import backfill_export
    from goeoview.plc_store import PLCStateStore

    mock_db = MagicMock()
    mock_db.db.insert = AsyncMock()

    get_calls = []

    @asynccontextmanager
    async def _fake_get(*args, **kwargs):
        get_calls.append((args, kwargs))
        resp = MagicMock(status=200)
        resp.read = AsyncMock(return_value=b"")
        yield resp

    store = PLCStateStore(str(tmp_path / "plc_state.db"))
    config = Config()

    mock_http = MagicMock()
    mock_session = MagicMock()
    mock_session.get = _fake_get
    mock_http.trusted.return_value = mock_session

    cursor = await backfill_export(
        mock_db,
        MagicMock(),
        "2025-04-03T17:34:59.442Z",
        config,
        mock_http,
        store,
    )

    assert cursor == "2025-04-03T17:34:59.442Z"
    assert len(get_calls) == 1
    assert get_calls[0] == (
        ("https://plc.directory/export",),
        {"params": {"count": config.plc_poller_count, "after": "2025-04-03T17:34:59.442Z"}},
    )


def test_should_fallback_to_export_for_known_stream_reasons():
    from goeoview.plc import should_fallback_to_export

    assert should_fallback_to_export(Exception("OutdatedCursor"))
    assert should_fallback_to_export(Exception("FutureCursor"))
    assert should_fallback_to_export(Exception("ConsumerTooSlow"))
    assert not should_fallback_to_export(Exception("some other failure"))


def test_stream_retry_interval_is_positive():
    from goeoview.plc import STREAM_RETRY_INTERVAL

    assert STREAM_RETRY_INTERVAL > 0


def test_export_target_interval_is_positive():
    config = Config()
    assert config.plc_export_target_interval > 0


class _WSContextManager:
    """Async context manager wrapper that yields the given ws mock."""

    def __init__(self, ws):
        self._ws = ws

    async def __aenter__(self):
        return self._ws

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _make_ws_session(fake_ws):
    """Create a mock aiohttp.ClientSession whose ws_connect returns fake_ws."""
    mock_session = MagicMock()
    mock_session.ws_connect = MagicMock(return_value=_WSContextManager(fake_ws))
    mock_session.close = AsyncMock()
    return mock_session


@pytest.mark.asyncio
async def test_stream_export_logs_connect_and_batched_ingest(tmp_path, caplog):
    """Batched logging: flush on disconnect with records < 1000 and elapsed < 5s."""
    import logging

    from goeoview.plc import stream_export
    from goeoview.plc_store import PLCStateStore

    mock_db = MagicMock()
    mock_db.db.insert = AsyncMock()

    records = [
        {"did": "did:plc:aaa", "seq": 10, "operation": {}},
        {"did": "did:plc:bbb", "seq": 11, "operation": {}},
    ]

    messages = [orjson.dumps(r).decode() for r in records]
    receive_results = [
        aiohttp.WSMessage(aiohttp.WSMsgType.TEXT, m, None) for m in messages
    ] + [aiohttp.WSMessage(aiohttp.WSMsgType.CLOSE, None, None)]

    mock_ws = AsyncMock()
    mock_ws.receive = AsyncMock(side_effect=receive_results)

    store = PLCStateStore(str(tmp_path / "plc_state.db"))
    config = Config()

    with (
        patch("goeoview.plc.aiohttp.ClientSession", return_value=_make_ws_session(mock_ws)),
        patch("goeoview.plc.fetch_prev_records", new=AsyncMock(return_value={})),
        patch("goeoview.plc.validate_records", return_value=[]),
        caplog.at_level(logging.INFO, logger="goeoview.plc"),
    ):
        with pytest.raises(aiohttp.ClientError):
            await stream_export(mock_db, MagicMock(), 5, config, store)

    log_messages = [r.message for r in caplog.records if r.name == "goeoview.plc"]

    # Should log connect
    connect_msgs = [m for m in log_messages if "stream connected" in m]
    assert len(connect_msgs) == 1
    assert "cursor=5" in connect_msgs[0]

    # Should flush one batched summary on disconnect
    ingest_msgs = [m for m in log_messages if "stream ingest" in m]
    assert len(ingest_msgs) == 1
    msg = ingest_msgs[0]
    assert "records=2" in msg
    assert "cursor=11" in msg
    assert "rows=" in msg
    assert "prev=" in msg
    assert "validate=" in msg
    assert "persist=" in msg
    assert "total=" in msg
    assert "rate=" in msg


@pytest.mark.asyncio
async def test_stream_export_logs_batch_at_count_threshold(tmp_path, caplog):
    """Logs a batch summary every 1000 records."""
    import logging

    from goeoview.plc import STREAM_LOG_BATCH_SIZE, stream_export
    from goeoview.plc_store import PLCStateStore

    mock_db = MagicMock()
    mock_db.db.insert = AsyncMock()

    num_records = STREAM_LOG_BATCH_SIZE + 5
    records = [
        {"did": f"did:plc:{i:04d}", "seq": 100 + i, "operation": {}}
        for i in range(num_records)
    ]

    messages = [orjson.dumps(r).decode() for r in records]
    receive_results = [
        aiohttp.WSMessage(aiohttp.WSMsgType.TEXT, m, None) for m in messages
    ] + [aiohttp.WSMessage(aiohttp.WSMsgType.CLOSE, None, None)]

    mock_ws = AsyncMock()
    mock_ws.receive = AsyncMock(side_effect=receive_results)

    store = PLCStateStore(str(tmp_path / "plc_state.db"))
    config = Config()

    with (
        patch("goeoview.plc.aiohttp.ClientSession", return_value=_make_ws_session(mock_ws)),
        patch("goeoview.plc.fetch_prev_records", new=AsyncMock(return_value={})),
        patch("goeoview.plc.validate_records", return_value=[]),
        caplog.at_level(logging.INFO, logger="goeoview.plc"),
    ):
        with pytest.raises(aiohttp.ClientError):
            await stream_export(mock_db, MagicMock(), 50, config, store)

    log_messages = [r.message for r in caplog.records if r.name == "goeoview.plc"]
    ingest_msgs = [m for m in log_messages if "stream ingest" in m]

    # One batch at 1000, one flush of remaining 5
    assert len(ingest_msgs) == 2
    assert f"records={STREAM_LOG_BATCH_SIZE}" in ingest_msgs[0]
    assert "records=5" in ingest_msgs[1]


@pytest.mark.asyncio
async def test_stream_export_logs_batch_at_time_threshold(tmp_path, caplog):
    """Logs a batch summary every 5 seconds."""
    import logging

    from goeoview.plc import stream_export
    from goeoview.plc_store import PLCStateStore

    mock_db = MagicMock()
    mock_db.db.insert = AsyncMock()

    records = [
        {"did": "did:plc:aaa", "seq": 10, "operation": {}},
        {"did": "did:plc:bbb", "seq": 11, "operation": {}},
        {"did": "did:plc:ccc", "seq": 12, "operation": {}},
    ]

    messages = [orjson.dumps(r).decode() for r in records]
    receive_results = [
        aiohttp.WSMessage(aiohttp.WSMsgType.TEXT, m, None) for m in messages
    ] + [aiohttp.WSMessage(aiohttp.WSMsgType.CLOSE, None, None)]

    mock_ws = AsyncMock()
    mock_ws.receive = AsyncMock(side_effect=receive_results)

    store = PLCStateStore(str(tmp_path / "plc_state.db"))
    config = Config()

    # perf_counter() call sites:
    #   1. batch_started = perf_counter()  [initial]
    #   Per record (7 calls):
    #     prev_started, batch_prev end, validate_started, validate end,
    #     persist_started, persist end, batch time check
    #   flush_batch (2 calls): batch_elapsed, batch_started reset
    #   final flush (1 call): batch_elapsed (returns early if empty)
    time_values = iter([
        0.0,   # batch_started initial
        # record 1
        0.0,   # prev_started
        0.0,   # batch_prev end
        0.0,   # validate_started
        0.0,   # validate end
        0.0,   # persist_started
        0.0,   # persist end
        1.0,   # batch time check (1.0 - 0.0 = 1s < 5s, no flush)
        # record 2
        2.0,   # prev_started
        2.0,   # batch_prev end
        2.0,   # validate_started
        2.0,   # validate end
        2.0,   # persist_started
        2.0,   # persist end
        6.0,   # batch time check (6.0 - 0.0 = 6s >= 5s, flush!)
        # flush_batch
        6.0,   # batch_elapsed (6.0 - 0.0 = 6s)
        6.0,   # batch_started reset
        # record 3
        6.0,   # prev_started
        6.0,   # batch_prev end
        6.0,   # validate_started
        6.0,   # validate end
        6.0,   # persist_started
        6.0,   # persist end
        7.0,   # batch time check (7.0 - 6.0 = 1s < 5s, no flush)
        # final flush (from inner finally)
        7.0,   # batch_elapsed (7.0 - 6.0 = 1s)
        # final flush resets but doesn't matter
        7.0,   # batch_started reset
        # outer finally flush (batch_records == 0, returns early)
        # no perf_counter call needed
    ])

    with (
        patch("goeoview.plc.aiohttp.ClientSession", return_value=_make_ws_session(mock_ws)),
        patch("goeoview.plc.fetch_prev_records", new=AsyncMock(return_value={})),
        patch("goeoview.plc.validate_records", return_value=[]),
        patch("goeoview.plc.perf_counter", side_effect=lambda: next(time_values)),
        caplog.at_level(logging.INFO, logger="goeoview.plc"),
    ):
        with pytest.raises(aiohttp.ClientError):
            await stream_export(mock_db, MagicMock(), 5, config, store)

    log_messages = [r.message for r in caplog.records if r.name == "goeoview.plc"]
    ingest_msgs = [m for m in log_messages if "stream ingest" in m]

    # One batch after 5s (2 records), one flush of remaining 1
    assert len(ingest_msgs) == 2
    assert "records=2" in ingest_msgs[0]
    assert "records=1" in ingest_msgs[1]
