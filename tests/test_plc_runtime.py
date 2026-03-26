from unittest.mock import AsyncMock, MagicMock, patch

import orjson
import pytest
from httpx_ws._exceptions import WebSocketNetworkError

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

    response_with_data = MagicMock(status_code=200, content=orjson.dumps(record) + b"\n")
    empty_response = MagicMock(status_code=200, content=b"")

    store = PLCStateStore(str(tmp_path / "plc_state.db"))
    config = Config()

    mock_http = MagicMock()
    mock_client = MagicMock()
    mock_client.get = AsyncMock(side_effect=[response_with_data, empty_response])
    mock_http.plc.return_value = mock_client

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

    empty_response = MagicMock(status_code=200, content=b"")

    store = PLCStateStore(str(tmp_path / "plc_state.db"))
    config = Config()

    mock_http = MagicMock()
    mock_client = MagicMock()
    mock_client.get = AsyncMock(return_value=empty_response)
    mock_http.plc.return_value = mock_client

    cursor = await backfill_export(
        mock_db,
        MagicMock(),
        "2025-04-03T17:34:59.442Z",
        config,
        mock_http,
        store,
    )

    assert cursor == "2025-04-03T17:34:59.442Z"
    mock_client.get.assert_awaited_once_with(
        "https://plc.directory/export",
        params={"count": config.plc_poller_count, "after": "2025-04-03T17:34:59.442Z"},
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


@pytest.mark.asyncio
async def test_stream_export_logs_connect_and_batched_ingest(tmp_path, caplog):
    """Batched logging: flush on disconnect with records < 1000 and elapsed < 5s."""
    import logging
    from contextlib import asynccontextmanager

    from goeoview.plc import stream_export
    from goeoview.plc_store import PLCStateStore

    mock_db = MagicMock()
    mock_db.db.insert = AsyncMock()

    records = [
        {"did": "did:plc:aaa", "seq": 10, "operation": {}},
        {"did": "did:plc:bbb", "seq": 11, "operation": {}},
    ]

    mock_ws = AsyncMock()
    messages = [orjson.dumps(r).decode() for r in records]
    mock_ws.receive_text = AsyncMock(
        side_effect=messages + [WebSocketNetworkError()]
    )

    @asynccontextmanager
    async def fake_aconnect_ws(url, client=None):
        yield mock_ws

    store = PLCStateStore(str(tmp_path / "plc_state.db"))
    config = Config()

    with (
        patch("goeoview.plc.aconnect_ws", fake_aconnect_ws),
        patch("goeoview.plc.fetch_prev_records", new=AsyncMock(return_value={})),
        patch("goeoview.plc.validate_records", return_value=[]),
        caplog.at_level(logging.INFO, logger="goeoview.plc"),
    ):
        with pytest.raises(WebSocketNetworkError):
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
    from contextlib import asynccontextmanager

    from goeoview.plc import STREAM_LOG_BATCH_SIZE, stream_export
    from goeoview.plc_store import PLCStateStore

    mock_db = MagicMock()
    mock_db.db.insert = AsyncMock()

    num_records = STREAM_LOG_BATCH_SIZE + 5
    records = [
        {"did": f"did:plc:{i:04d}", "seq": 100 + i, "operation": {}}
        for i in range(num_records)
    ]

    mock_ws = AsyncMock()
    messages = [orjson.dumps(r).decode() for r in records]
    mock_ws.receive_text = AsyncMock(
        side_effect=messages + [WebSocketNetworkError()]
    )

    @asynccontextmanager
    async def fake_aconnect_ws(url, client=None):
        yield mock_ws

    store = PLCStateStore(str(tmp_path / "plc_state.db"))
    config = Config()

    with (
        patch("goeoview.plc.aconnect_ws", fake_aconnect_ws),
        patch("goeoview.plc.fetch_prev_records", new=AsyncMock(return_value={})),
        patch("goeoview.plc.validate_records", return_value=[]),
        caplog.at_level(logging.INFO, logger="goeoview.plc"),
    ):
        with pytest.raises(WebSocketNetworkError):
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
    from contextlib import asynccontextmanager

    from goeoview.plc import stream_export
    from goeoview.plc_store import PLCStateStore

    mock_db = MagicMock()
    mock_db.db.insert = AsyncMock()

    records = [
        {"did": "did:plc:aaa", "seq": 10, "operation": {}},
        {"did": "did:plc:bbb", "seq": 11, "operation": {}},
        {"did": "did:plc:ccc", "seq": 12, "operation": {}},
    ]

    mock_ws = AsyncMock()
    messages = [orjson.dumps(r).decode() for r in records]
    mock_ws.receive_text = AsyncMock(
        side_effect=messages + [WebSocketNetworkError()]
    )

    @asynccontextmanager
    async def fake_aconnect_ws(url, client=None):
        yield mock_ws

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
        patch("goeoview.plc.aconnect_ws", fake_aconnect_ws),
        patch("goeoview.plc.fetch_prev_records", new=AsyncMock(return_value={})),
        patch("goeoview.plc.validate_records", return_value=[]),
        patch("goeoview.plc.perf_counter", side_effect=lambda: next(time_values)),
        caplog.at_level(logging.INFO, logger="goeoview.plc"),
    ):
        with pytest.raises(WebSocketNetworkError):
            await stream_export(mock_db, MagicMock(), 5, config, store)

    log_messages = [r.message for r in caplog.records if r.name == "goeoview.plc"]
    ingest_msgs = [m for m in log_messages if "stream ingest" in m]

    # One batch after 5s (2 records), one flush of remaining 1
    assert len(ingest_msgs) == 2
    assert "records=2" in ingest_msgs[0]
    assert "records=1" in ingest_msgs[1]
