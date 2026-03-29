from unittest.mock import AsyncMock, MagicMock, patch

import cbrrr
import pytest
from httpx_ws._exceptions import WebSocketNetworkError

from goeoview.helpers import sb32d


class _FakeWS:
    def __init__(self) -> None:
        self.calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def receive_bytes(self):
        self.calls += 1
        if self.calls == 1:
            return b"payload"
        raise WebSocketNetworkError("disconnect")


@pytest.mark.asyncio
@patch("goeoview.relay.firehose")
async def test_start_firehose_retries_websocket_errors(mock_firehose):
    from goeoview.relay import start_firehose

    call_count = 0

    async def side_effect(url, did_resolver, config, quarantine, pool, registry):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise WebSocketNetworkError("lost connection")
        raise KeyboardInterrupt("stop test")

    mock_firehose.side_effect = side_effect

    mock_config = MagicMock()
    mock_config.firehose_workers = 1
    mock_registry = MagicMock()
    mock_registry.watch = AsyncMock()
    with patch("goeoview.quarantine.QuarantineStore"):
        with patch("goeoview.hooks.HookRegistry", return_value=mock_registry):
            with pytest.raises(KeyboardInterrupt, match="stop test"):
                await start_firehose("bsky.network", MagicMock(), mock_config)

    assert mock_firehose.call_count == 3


@pytest.mark.asyncio
async def test_firehose_flushes_memory_before_reconnect(tmp_path):
    from goeoview.relay import firehose

    mock_db = MagicMock()
    mock_db.db.query = AsyncMock(return_value=MagicMock(row_count=0))

    mock_config = MagicMock()
    mock_config.firehose_workers = 1
    mock_config.firehose_batch_size = 1
    mock_config.firehose_insert_every = 9999
    mock_config.firehose_print_clock_skew_every = 100
    mock_config.quarantine_db_path = str(tmp_path / "quarantine.db")

    fake_header = {"seq": 5, "repo": "did:plc:test", "time": "2025-01-01T00:00:00+00:00"}
    fake_rows = [("did:plc:test", "app.bsky.feed.post", "abc", 1, b"{}", None, "create")]

    async def fake_process_batch(batch, memory, did_resolver, config, loop, pool, _validate_commit, quarantine=None, **kwargs):
        memory.extend(fake_rows)

    insert_mock = AsyncMock()

    with patch("goeoview.relay.DB", return_value=mock_db):
        with patch("goeoview.relay.aconnect_ws", return_value=_FakeWS()):
            with patch("goeoview.relay.parse_firehose_message_type", return_value=("commit", b"body")):
                with patch("goeoview.relay._decode_commit_header", return_value=fake_header):
                    with patch("goeoview.relay._process_batch", side_effect=fake_process_batch):
                        with patch("goeoview.relay.insert_commits", insert_mock):
                            from goeoview.quarantine import QuarantineStore
                            quarantine = QuarantineStore(str(tmp_path / "quarantine.db"))
                            mock_registry = MagicMock()
                            with pytest.raises(WebSocketNetworkError):
                                await firehose("bsky.network", MagicMock(), mock_config, quarantine, None, mock_registry)

    insert_mock.assert_awaited_once()
    assert insert_mock.await_args.args[3] == 5


@pytest.mark.asyncio
async def test_process_batch_skips_blocked_pds(tmp_path):
    """Commits from PDS hosts matching blocked_pds patterns are silently skipped."""
    from collections import deque
    from goeoview.relay import _process_batch

    batch = [
        (b"bridgy-bytes", 10, "did:plc:bridgy", None),
        (b"bridgy-slash-bytes", 11, "did:plc:bridgy2", None),
        (b"stream-bytes", 12, "did:plc:stream", None),
        (b"normal-bytes", 13, "did:plc:normal", None),
    ]

    mock_resolver = MagicMock()

    def make_user(pds):
        user = MagicMock()
        user.pds = pds
        user.atproto_key = "zFakeKey"
        return user

    users = {
        "did:plc:bridgy": make_user("https://atproto.brid.gy"),
        "did:plc:bridgy2": make_user("https://atproto.brid.gy/"),
        "did:plc:stream": make_user("https://prod-ams0.stream.place"),
        "did:plc:normal": make_user("https://pds.example.com"),
    }
    mock_resolver.resolve = AsyncMock(side_effect=lambda did: users[did])

    mock_config = MagicMock()
    mock_config.blocked_pds = ["atproto.brid.gy", ".stream.place"]

    def fake_validate(msg, key):
        return {"ok": True, "rows": [("did", "col", "rk", 1, b"{}", None, "create")]}

    from goeoview.quarantine import QuarantineStore
    quarantine = QuarantineStore(str(tmp_path / "quarantine.db"))

    memory = deque()
    await _process_batch(batch, memory, mock_resolver, mock_config, None, None, fake_validate, quarantine)

    # Only the normal commit should produce rows
    assert len(memory) == 1


@pytest.mark.asyncio
async def test_failed_commit_quarantined(tmp_path):
    """Non-fatal worker errors must quarantine the raw message, not silently drop it."""
    from collections import deque
    from goeoview.relay import _process_batch
    from goeoview.quarantine import QuarantineStore

    quarantine = QuarantineStore(str(tmp_path / "quarantine.db"))

    bad_msg = b"raw-commit-bytes"
    batch = [
        (b"good-bytes", 10, "did:plc:good", None),
        (bad_msg, 11, "did:plc:bad", None),
        (b"also-good", 12, "did:plc:good2", None),
    ]

    mock_resolver = MagicMock()
    mock_user = MagicMock()
    mock_user.pds = "https://pds.example.com"
    mock_user.atproto_key = "zFakeKey"
    mock_resolver.resolve = AsyncMock(return_value=mock_user)

    good_result = {"ok": True, "rows": [("did", "col", "rk", 1, b"{}", None, "create")]}
    bad_result = {"ok": False, "error": "mst proof failed for app.bsky.feed.post/abc"}

    def fake_validate(msg, key):
        if msg == bad_msg:
            return bad_result
        return good_result

    memory = deque()
    await _process_batch(batch, memory, mock_resolver, MagicMock(), None, None, fake_validate, quarantine)

    # Good commits produced rows
    assert len(memory) == 2

    # Bad commit quarantined with metadata
    assert quarantine.count() == 1
    recent = quarantine.recent()
    assert recent[0]["repo"] == "did:plc:bad"
    assert recent[0]["seq"] == 11
    assert recent[0]["stage"] == "worker"
    assert "mst proof failed" in recent[0]["reason"]


@pytest.mark.asyncio
async def test_cursor_does_not_advance_past_failed_batch(tmp_path):
    """If _process_batch raises, last_safe_seq must not advance past the pre-batch value."""
    from goeoview.relay import firehose

    mock_db = MagicMock()
    mock_db.db.query = AsyncMock(return_value=MagicMock(row_count=1, first_row=[100]))
    mock_db.db.insert = AsyncMock()

    mock_config = MagicMock()
    mock_config.firehose_workers = 1
    mock_config.firehose_batch_size = 1
    mock_config.firehose_insert_every = 9999
    mock_config.firehose_print_clock_skew_every = 100
    mock_config.firehose_batch_timeout = None
    mock_config.quarantine_db_path = str(tmp_path / "quarantine.db")

    fake_header = {"seq": 200, "repo": "did:plc:test", "time": "2025-01-01T00:00:00+00:00"}

    async def exploding_process_batch(batch, memory, *args):
        from goeoview.relay import CommitValidationError
        raise CommitValidationError("fatal error in batch")

    insert_mock = AsyncMock()

    with patch("goeoview.relay.DB", return_value=mock_db):
        with patch("goeoview.relay.aconnect_ws", return_value=_FakeWS()):
            with patch("goeoview.relay.parse_firehose_message_type", return_value=("commit", b"body")):
                with patch("goeoview.relay._decode_commit_header", return_value=fake_header):
                    with patch("goeoview.relay._process_batch", side_effect=exploding_process_batch):
                        with patch("goeoview.relay.insert_commits", insert_mock):
                            from goeoview.quarantine import QuarantineStore
                            quarantine = QuarantineStore(str(tmp_path / "quarantine.db"))
                            mock_registry = MagicMock()
                            with pytest.raises(Exception):
                                await firehose("bsky.network", MagicMock(), mock_config, quarantine, None, mock_registry)

    # insert_commits should NOT have been called with seq=200
    # It may be called in the finally block with the pre-batch cursor (100)
    for call in insert_mock.await_args_list:
        persisted_seq = call.args[3]
        assert persisted_seq == 100, f"cursor advanced to {persisted_seq}, should stay at 100"


@pytest.mark.asyncio
async def test_handle_commit_serial_uses_historical_key_when_plc_ahead(tmp_path):
    """When current key fails and PLC has a newer op than the commit time,
    try the historical key that was active at commit time."""
    from goeoview.relay import _handle_commit_serial
    from goeoview.quarantine import QuarantineStore

    commit_time = "2026-03-20T12:00:00Z"
    repo = "did:plc:testhistorical"
    old_key = "zOldKeyMultibase"
    new_key = "zNewKeyMultibase"
    expected_rows = [("did:plc:testhistorical", "app.bsky.feed.post", "abc", 1, b"{}", None, "create")]

    message = cbrrr.encode_dag_cbor({
        "repo": repo,
        "rev": "aaaaaaa",
        "since": None,
        "time": commit_time,
        "seq": 42,
        "tooBig": False,
        "blocks": b"",
        "commit": None,
        "ops": [],
    })

    # Current key (new_key) fails, old_key succeeds
    def fake_validate(msg, key):
        if key == old_key:
            return {"ok": True, "rows": expected_rows}
        return {"ok": False, "retry": True, "repo": repo, "seq": 42}

    # DID resolver returns user with new key
    mock_user = MagicMock()
    mock_user.did = repo
    mock_user.atproto_key = new_key
    mock_resolver = MagicMock()
    mock_resolver.resolve = AsyncMock(return_value=mock_user)

    # DB returns historical key from plc_op — PLC has an op AFTER commit time
    mock_query_result = MagicMock()
    mock_query_result.row_count = 1
    mock_query_result.first_row = [f"did:key:{old_key}"]
    mock_db = MagicMock()
    mock_db.db.query = AsyncMock(return_value=mock_query_result)

    quarantine = QuarantineStore(str(tmp_path / "quarantine.db"))

    with patch("goeoview.relay.validate_commit", side_effect=fake_validate):
        with patch("goeoview.relay.DB", return_value=mock_db):
            rows = await _handle_commit_serial(message, mock_resolver, quarantine)

    assert rows == expected_rows


@pytest.mark.asyncio
async def test_handle_commit_serial_falls_through_to_retries_when_plc_not_ahead(tmp_path):
    """When PLC has no op newer than the commit, fall through to existing retry loop."""
    from goeoview.relay import _handle_commit_serial
    from goeoview.quarantine import QuarantineStore

    commit_time = "2026-03-20T12:00:00Z"
    repo = "did:plc:testnohistory"
    current_key = "zCurrentKey"

    message = cbrrr.encode_dag_cbor({
        "repo": repo,
        "rev": "aaaaaaa",
        "since": None,
        "time": commit_time,
        "seq": 42,
        "tooBig": False,
        "blocks": b"",
        "commit": None,
        "ops": [],
    })

    call_count = 0
    def fake_validate(msg, key):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return {"ok": False, "retry": True, "repo": repo, "seq": 42}
        return {"ok": True, "rows": [("row",)]}

    mock_user = MagicMock()
    mock_user.did = repo
    mock_user.atproto_key = current_key
    mock_resolver = MagicMock()
    mock_resolver.resolve = AsyncMock(return_value=mock_user)
    mock_resolver.resolve.cache_invalidate = MagicMock()

    # DB returns no historical key (PLC not ahead)
    mock_query_result = MagicMock()
    mock_query_result.row_count = 0
    mock_db = MagicMock()
    mock_db.db.query = AsyncMock(return_value=mock_query_result)

    quarantine = QuarantineStore(str(tmp_path / "quarantine.db"))

    with patch("goeoview.relay.validate_commit", side_effect=fake_validate):
        with patch("goeoview.relay.DB", return_value=mock_db):
            rows = await _handle_commit_serial(message, mock_resolver, quarantine)

    # Should have gone through retry loop (3 validate_commit calls total:
    # 1 initial + 1 historical check skipped + 2 retries where 2nd succeeds)
    assert rows == [("row",)]
    assert call_count == 3


class _TwoMessageWS:
    """Sends two commit messages then disconnects."""

    def __init__(self, headers):
        self._headers = list(headers)
        self._index = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def receive_bytes(self):
        if self._index < len(self._headers):
            self._index += 1
            return b"payload"
        raise WebSocketNetworkError("disconnect")


@pytest.mark.asyncio
async def test_naive_timestamp_quarantined(tmp_path):
    """A commit whose time field lacks timezone info must be quarantined, not crash."""
    from goeoview.relay import firehose

    mock_db = MagicMock()
    mock_db.db.query = AsyncMock(return_value=MagicMock(row_count=0))

    mock_config = MagicMock()
    mock_config.firehose_workers = 1
    mock_config.firehose_batch_size = 100
    mock_config.firehose_insert_every = 9999
    mock_config.firehose_print_clock_skew_every = 100
    mock_config.firehose_batch_timeout = None

    # This is the actual offending timestamp from atom.new.incorso.xyz
    naive_header = {"seq": 5, "repo": "did:plc:47awjkxlto45stih3zqt2jt2", "time": "2026-03-25T18:09:25.6053381"}
    good_header = {"seq": 6, "repo": "did:plc:good", "time": "2026-03-25T18:09:26.000Z"}

    headers = [naive_header, good_header]
    header_index = [0]

    def fake_decode_header(msg):
        i = header_index[0]
        header_index[0] += 1
        return headers[i]

    async def fake_process_batch(batch, memory, *args, **kwargs):
        pass

    insert_mock = AsyncMock()

    with patch("goeoview.relay.DB", return_value=mock_db):
        with patch("goeoview.relay.aconnect_ws", return_value=_TwoMessageWS(headers)):
            with patch("goeoview.relay.parse_firehose_message_type", return_value=("commit", b"body")):
                with patch("goeoview.relay._decode_commit_header", side_effect=fake_decode_header):
                    with patch("goeoview.relay._process_batch", side_effect=fake_process_batch):
                        with patch("goeoview.relay.insert_commits", insert_mock):
                            from goeoview.quarantine import QuarantineStore
                            quarantine = QuarantineStore(str(tmp_path / "quarantine.db"))
                            mock_registry = MagicMock()
                            with pytest.raises(WebSocketNetworkError):
                                await firehose("bsky.network", MagicMock(), mock_config, quarantine, None, mock_registry)

    # The naive-timestamp commit should be quarantined
    assert quarantine.count() == 1
    recent = quarantine.recent()
    assert recent[0]["repo"] == "did:plc:47awjkxlto45stih3zqt2jt2"
    assert recent[0]["seq"] == 5
    assert recent[0]["stage"] == "firehose"
    assert "datetime" in recent[0]["reason"].lower() or "timezone" in recent[0]["reason"].lower()


class _SyncThenDisconnectWS:
    """Sends a #sync message followed by a disconnect."""

    def __init__(self, sync_did, sync_rev, sync_seq):
        self._msg = (
            cbrrr.encode_dag_cbor({"t": "#sync", "op": 1})
            + cbrrr.encode_dag_cbor({
                "did": sync_did,
                "rev": sync_rev,
                "seq": sync_seq,
                "time": "2026-03-23T08:00:00.000Z",
                "blocks": b"\x00",
            })
        )
        self._sent = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def receive_bytes(self):
        if not self._sent:
            self._sent = True
            return self._msg
        raise WebSocketNetworkError("disconnect")


@pytest.mark.asyncio
async def test_sync_message_inserts_gap(tmp_path):
    """A #sync message should insert a gap row for the DID."""
    from goeoview.relay import firehose

    mock_db = MagicMock()
    mock_db.db.query = AsyncMock(return_value=MagicMock(row_count=0))
    mock_db.db.insert = AsyncMock()

    mock_config = MagicMock()
    mock_config.firehose_workers = 1
    mock_config.firehose_batch_size = 100
    mock_config.firehose_insert_every = 9999
    mock_config.firehose_print_clock_skew_every = 100
    mock_config.firehose_batch_timeout = None
    mock_config.ws_connect_timeout = 10

    sync_did = "did:plc:testdid123"
    sync_rev = "3mhpo2yjlqz2z"
    sync_seq = 500
    expected_rev_int = int.from_bytes(sb32d(sync_rev))

    ws = _SyncThenDisconnectWS(sync_did, sync_rev, sync_seq)

    with patch("goeoview.relay.DB", return_value=mock_db):
        with patch("goeoview.relay.aconnect_ws", return_value=ws):
            from goeoview.quarantine import QuarantineStore
            quarantine = QuarantineStore(str(tmp_path / "quarantine.db"))
            mock_registry = MagicMock()
            with pytest.raises(WebSocketNetworkError):
                await firehose("bsky.network", MagicMock(), mock_config, quarantine, None, mock_registry)

    # Check that a gap was inserted for this DID
    gap_calls = [
        call for call in mock_db.db.insert.await_args_list
        if call.args[0] == "gap"
    ]
    assert len(gap_calls) == 1
    gap_rows = gap_calls[0].args[1]
    assert len(gap_rows) == 1
    assert gap_rows[0] == (sync_did, 0, expected_rev_int)


class _MultiMessageWS:
    """Sends N messages then disconnects."""

    def __init__(self, count):
        self._count = count
        self._index = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def receive_bytes(self):
        if self._index < self._count:
            self._index += 1
            return b"payload"
        raise WebSocketNetworkError("disconnect")


@pytest.mark.asyncio
async def test_duplicate_commit_seq_warns_and_skips(tmp_path):
    """A commit with the same seq as the previous one should warn and skip, not raise."""
    from goeoview.relay import firehose

    mock_db = MagicMock()
    mock_db.db.query = AsyncMock(return_value=MagicMock(row_count=0))

    mock_config = MagicMock()
    mock_config.firehose_workers = 1
    mock_config.firehose_batch_size = 100
    mock_config.firehose_insert_every = 9999
    mock_config.firehose_print_clock_skew_every = 100
    mock_config.firehose_batch_timeout = None

    # Two commits with the same seq
    dupe_header = {"seq": 100, "repo": "did:plc:dupe", "time": "2025-01-01T00:00:00+00:00"}

    async def fake_process_batch(batch, memory, *args, **kwargs):
        memory.extend([("did:plc:dupe", "app.bsky.feed.post", "rk", 1, b"{}", None, "create")] * len(batch))

    insert_mock = AsyncMock()

    with patch("goeoview.relay.DB", return_value=mock_db):
        with patch("goeoview.relay.aconnect_ws", return_value=_MultiMessageWS(2)):
            with patch("goeoview.relay.parse_firehose_message_type", return_value=("commit", b"body")):
                with patch("goeoview.relay._decode_commit_header", return_value=dupe_header):
                    with patch("goeoview.relay._process_batch", side_effect=fake_process_batch):
                        with patch("goeoview.relay.insert_commits", insert_mock):
                            from goeoview.quarantine import QuarantineStore
                            quarantine = QuarantineStore(str(tmp_path / "quarantine.db"))
                            mock_registry = MagicMock()
                            # Should NOT raise — duplicate seq should be skipped
                            with pytest.raises(WebSocketNetworkError):
                                await firehose("bsky.network", MagicMock(), mock_config, quarantine, None, mock_registry)

    # Only 1 commit should have been processed (the duplicate skipped)
    if insert_mock.await_args_list:
        total_rows = sum(len(c.args[1]) for c in insert_mock.await_args_list)
        assert total_rows == 1


@pytest.mark.asyncio
async def test_duplicate_sync_seq_warns_and_skips(tmp_path):
    """A #sync with the same seq as the previous one should warn and skip, not raise."""
    from goeoview.relay import firehose

    sync_did = "did:plc:dupesync"
    sync_rev = "3mhpo2yjlqz2z"
    sync_seq = 500

    # Build the raw sync message
    sync_msg = (
        cbrrr.encode_dag_cbor({"t": "#sync", "op": 1})
        + cbrrr.encode_dag_cbor({
            "did": sync_did,
            "rev": sync_rev,
            "seq": sync_seq,
            "time": "2026-03-23T08:00:00.000Z",
            "blocks": b"\x00",
        })
    )

    class _DupeSyncWS:
        """Sends the same sync message twice then disconnects."""
        def __init__(self):
            self._calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def receive_bytes(self):
            self._calls += 1
            if self._calls <= 2:
                return sync_msg
            raise WebSocketNetworkError("disconnect")

    mock_db = MagicMock()
    mock_db.db.query = AsyncMock(return_value=MagicMock(row_count=0))
    mock_db.db.insert = AsyncMock()

    mock_config = MagicMock()
    mock_config.firehose_workers = 1
    mock_config.firehose_batch_size = 100
    mock_config.firehose_insert_every = 9999
    mock_config.firehose_print_clock_skew_every = 100
    mock_config.firehose_batch_timeout = None

    with patch("goeoview.relay.DB", return_value=mock_db):
        with patch("goeoview.relay.aconnect_ws", return_value=_DupeSyncWS()):
            from goeoview.quarantine import QuarantineStore
            quarantine = QuarantineStore(str(tmp_path / "quarantine.db"))
            mock_registry = MagicMock()
            # Should NOT raise — duplicate sync seq should be skipped
            with pytest.raises(WebSocketNetworkError):
                await firehose("bsky.network", MagicMock(), mock_config, quarantine, None, mock_registry)

    # Only 1 gap should have been inserted (the duplicate skipped)
    gap_calls = [
        call for call in mock_db.db.insert.await_args_list
        if call.args[0] == "gap"
    ]
    assert len(gap_calls) == 1


@pytest.mark.asyncio
async def test_truly_non_monotonic_seq_still_raises(tmp_path):
    """A commit with a seq LOWER than the previous one should still raise."""
    from goeoview.relay import firehose, CommitValidationError

    mock_db = MagicMock()
    mock_db.db.query = AsyncMock(return_value=MagicMock(row_count=0))

    mock_config = MagicMock()
    mock_config.firehose_workers = 1
    mock_config.firehose_batch_size = 100
    mock_config.firehose_insert_every = 9999
    mock_config.firehose_print_clock_skew_every = 100
    mock_config.firehose_batch_timeout = None

    headers = [
        {"seq": 200, "repo": "did:plc:a", "time": "2025-01-01T00:00:00+00:00"},
        {"seq": 100, "repo": "did:plc:b", "time": "2025-01-01T00:00:00+00:00"},
    ]
    header_index = [0]

    def fake_decode(msg):
        i = header_index[0]
        header_index[0] += 1
        return headers[i]

    async def fake_process_batch(batch, memory, *args, **kwargs):
        pass

    with patch("goeoview.relay.DB", return_value=mock_db):
        with patch("goeoview.relay.aconnect_ws", return_value=_MultiMessageWS(2)):
            with patch("goeoview.relay.parse_firehose_message_type", return_value=("commit", b"body")):
                with patch("goeoview.relay._decode_commit_header", side_effect=fake_decode):
                    with patch("goeoview.relay._process_batch", side_effect=fake_process_batch):
                        with patch("goeoview.relay.insert_commits", AsyncMock()):
                            from goeoview.quarantine import QuarantineStore
                            quarantine = QuarantineStore(str(tmp_path / "quarantine.db"))
                            mock_registry = MagicMock()
                            with pytest.raises(CommitValidationError, match="non-monotonic"):
                                await firehose("bsky.network", MagicMock(), mock_config, quarantine, None, mock_registry)


@pytest.mark.asyncio
async def test_insert_commits_null_since_new_did():
    """A commit with since=None for an unknown DID should insert a (did, 0, 0) gap."""
    from collections import deque
    from goeoview.relay import insert_commits

    mock_db = MagicMock()
    # No existing commits for this DID
    mock_db.db.query = AsyncMock(return_value=MagicMock(result_rows=[]))
    mock_db.db.insert = AsyncMock()

    did = "did:plc:newdid"
    memory = deque([(did, "app.bsky.feed.post", "rkey1", 100, b"val", None, "create")])

    await insert_commits(mock_db, memory, "wss://example.com", 1)

    gap_calls = [c for c in mock_db.db.insert.await_args_list if c.args[0] == "gap"]
    assert len(gap_calls) == 1
    gap_rows = gap_calls[0].args[1]
    assert gap_rows == [(did, 0, 0)]


@pytest.mark.asyncio
async def test_insert_commits_null_since_known_did():
    """A commit with since=None for a known DID should log a warning but not insert a gap."""
    from collections import deque
    from goeoview.relay import insert_commits

    mock_db = MagicMock()
    did = "did:plc:knowndid"
    mock_db.db.query = AsyncMock(return_value=MagicMock(result_rows=[(did, 50)]))
    mock_db.db.insert = AsyncMock()

    memory = deque([(did, "app.bsky.feed.post", "rkey1", 100, b"val", None, "create")])

    await insert_commits(mock_db, memory, "wss://example.com", 1)

    gap_calls = [c for c in mock_db.db.insert.await_args_list if c.args[0] == "gap"]
    assert len(gap_calls) == 1
    gap_rows = gap_calls[0].args[1]
    assert gap_rows == []


@pytest.mark.asyncio
async def test_insert_commits_includes_user_in_hook_rows():
    """When a user_map is provided, hook_rows should include the user object."""
    from collections import deque
    from goeoview.relay import insert_commits

    mock_db = MagicMock()
    mock_db.db.query = AsyncMock(return_value=MagicMock(result_rows=[]))
    mock_db.db.insert = AsyncMock()

    did = "did:plc:abc"
    fake_user = MagicMock()
    fake_user.did = did
    fake_user.handle = "alice.test"
    user_map = {did: fake_user}

    memory = deque([(did, "app.bsky.feed.post", "rkey1", 100, b'{"text":"hi"}', None, "create")])

    hook_rows = await insert_commits(mock_db, memory, "wss://example.com", 1, user_map=user_map)

    assert len(hook_rows) == 1
    assert hook_rows[0]["user"] is fake_user


@pytest.mark.asyncio
async def test_insert_commits_user_none_when_not_in_map():
    """When user_map is provided but DID is missing, user should be None."""
    from collections import deque
    from goeoview.relay import insert_commits

    mock_db = MagicMock()
    mock_db.db.query = AsyncMock(return_value=MagicMock(result_rows=[]))
    mock_db.db.insert = AsyncMock()

    did = "did:plc:unknown"
    user_map = {}

    memory = deque([(did, "app.bsky.feed.post", "rkey1", 100, b'{"text":"hi"}', None, "create")])

    hook_rows = await insert_commits(mock_db, memory, "wss://example.com", 1, user_map=user_map)

    assert len(hook_rows) == 1
    assert hook_rows[0]["user"] is None
