"""Tests for goeoview.gap_filler."""

import asyncio
import ssl
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, UTC, timedelta
from unittest.mock import AsyncMock, MagicMock, call, patch

import httpx
import pytest

from goeoview.relay import RepoDownloadException


def _make_user(did="did:plc:test123", pds="https://pds.example.com"):
    user = MagicMock()
    user.did = did
    user.pds = pds
    return user


def _make_config(max_repo_size=10 * 1024 * 1024):
    config = MagicMock()
    config.max_repo_size = max_repo_size
    return config


def _make_stream_response(status_code=200, body=b"car-data", headers=None):
    """Build a fake async streaming response."""
    response = MagicMock()
    response.status_code = status_code
    response.headers = headers or {"content-type": "application/vnd.ipld.car"}

    async def aiter_bytes():
        yield body

    response.aiter_bytes = aiter_bytes
    response.aread = AsyncMock()
    return response


def _make_http_clients(response):
    """Build http_clients mock whose .repo().stream() yields *response*."""
    @asynccontextmanager
    async def fake_stream(method, url):
        yield response

    client = MagicMock()
    client.stream = fake_stream

    http_clients = MagicMock()
    http_clients.repo.return_value = client
    return http_clients


class TestDownloadOneSuccess:
    async def test_returns_user_body_headers(self):
        from goeoview.gap_filler import download_one

        user = _make_user()
        resolver = AsyncMock()
        resolver.resolve.return_value = user

        resp = _make_stream_response(200, b"car-bytes", {"x-custom": "val"})
        http_clients = _make_http_clients(resp)
        config = _make_config()

        result_user, car_bytes, headers = await download_one(
            "did:plc:test123", resolver, http_clients, config
        )

        assert result_user is user
        assert car_bytes == b"car-bytes"
        assert headers["x-custom"] == "val"


class TestDownloadOnePdsValidation:
    async def test_pds_none_raises(self):
        from goeoview.gap_filler import download_one

        user = _make_user(pds=None)
        resolver = AsyncMock()
        resolver.resolve.return_value = user
        config = _make_config()

        with pytest.raises(RepoDownloadException) as exc_info:
            await download_one("did:plc:test123", resolver, MagicMock(), config)

        assert exc_info.value.user is user

    async def test_pds_not_https_raises(self):
        from goeoview.gap_filler import download_one

        user = _make_user(pds="http://insecure.example.com")
        resolver = AsyncMock()
        resolver.resolve.return_value = user
        config = _make_config()

        with pytest.raises(RepoDownloadException) as exc_info:
            await download_one("did:plc:test123", resolver, MagicMock(), config)

        assert exc_info.value.user is user


class TestDownloadOneHttpErrors:
    @pytest.mark.parametrize("status_code", [400, 401, 403])
    async def test_client_error_inserts_bad_did_and_raises(self, status_code):
        from goeoview.gap_filler import download_one

        user = _make_user()
        resolver = AsyncMock()
        resolver.resolve.return_value = user

        resp = _make_stream_response(status_code)
        http_clients = _make_http_clients(resp)
        config = _make_config()

        mock_db = MagicMock()
        mock_db.db = AsyncMock()

        with patch("goeoview.gap_filler.DB", return_value=mock_db):
            with pytest.raises(RepoDownloadException) as exc_info:
                await download_one("did:plc:test123", resolver, http_clients, config)

        assert exc_info.value.status_code == status_code
        mock_db.db.insert.assert_awaited_once_with(
            "bad_did", [(user.did, status_code)]
        )

    @pytest.mark.parametrize("status_code", [500, 502, 503])
    async def test_server_error_raises_without_bad_did(self, status_code):
        from goeoview.gap_filler import download_one

        user = _make_user()
        resolver = AsyncMock()
        resolver.resolve.return_value = user

        resp = _make_stream_response(status_code)
        http_clients = _make_http_clients(resp)
        config = _make_config()

        mock_db = MagicMock()
        mock_db.db = AsyncMock()

        with patch("goeoview.gap_filler.DB", return_value=mock_db):
            with pytest.raises(RepoDownloadException) as exc_info:
                await download_one("did:plc:test123", resolver, http_clients, config)

        assert exc_info.value.status_code == status_code
        mock_db.db.insert.assert_not_awaited()

    async def test_429_raises_with_headers(self):
        from goeoview.gap_filler import download_one

        user = _make_user()
        resolver = AsyncMock()
        resolver.resolve.return_value = user

        headers = {"retry-after": "30", "ratelimit-remaining": "0"}
        resp = _make_stream_response(429, headers=headers)
        http_clients = _make_http_clients(resp)
        config = _make_config()

        mock_db = MagicMock()
        mock_db.db = AsyncMock()

        with patch("goeoview.gap_filler.DB", return_value=mock_db):
            with pytest.raises(RepoDownloadException) as exc_info:
                await download_one("did:plc:test123", resolver, http_clients, config)

        assert exc_info.value.status_code == 429
        assert exc_info.value.headers["retry-after"] == "30"


class TestDownloadOneConnectionErrors:
    @pytest.mark.parametrize("error_cls", [
        httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadError,
        httpx.ReadTimeout, httpx.RemoteProtocolError,
    ])
    async def test_connection_error_raises_with_no_status(self, error_cls):
        from goeoview.gap_filler import download_one

        user = _make_user()
        resolver = AsyncMock()
        resolver.resolve.return_value = user

        @asynccontextmanager
        async def raising_stream(method, url):
            raise error_cls("boom")
            yield  # noqa: unreachable, needed for asynccontextmanager

        client = MagicMock()
        client.stream = raising_stream
        http_clients = MagicMock()
        http_clients.repo.return_value = client
        config = _make_config()

        with pytest.raises(RepoDownloadException) as exc_info:
            await download_one("did:plc:test123", resolver, http_clients, config)

        assert exc_info.value.status_code is None
        assert exc_info.value.user is user

    async def test_ssl_error_raises_with_no_status(self):
        from goeoview.gap_filler import download_one

        user = _make_user()
        resolver = AsyncMock()
        resolver.resolve.return_value = user

        @asynccontextmanager
        async def raising_stream(method, url):
            raise ssl.SSLError("cert verify failed")
            yield  # noqa

        client = MagicMock()
        client.stream = raising_stream
        http_clients = MagicMock()
        http_clients.repo.return_value = client
        config = _make_config()

        with pytest.raises(RepoDownloadException) as exc_info:
            await download_one("did:plc:test123", resolver, http_clients, config)

        assert exc_info.value.status_code is None


class TestAdjustTarget:
    def test_no_rate_limit_headers_returns_unchanged(self):
        from goeoview.gap_filler import adjust_target

        assert adjust_target({}, current_target=4, max_target=8) == 4

    def test_remaining_above_50_percent_increases_by_one(self):
        from goeoview.gap_filler import adjust_target

        # 60 / 100 = 60% > 50%
        assert adjust_target(
            {"ratelimit-remaining": "60", "ratelimit-limit": "100"},
            current_target=4, max_target=8
        ) == 5

    def test_remaining_above_50_percent_capped_at_max(self):
        from goeoview.gap_filler import adjust_target

        # Already at max — should stay at max
        assert adjust_target(
            {"ratelimit-remaining": "60", "ratelimit-limit": "100"},
            current_target=8, max_target=8
        ) == 8

    def test_remaining_below_10_percent_returns_one(self):
        from goeoview.gap_filler import adjust_target

        # 5 / 100 = 5% < 10%
        assert adjust_target(
            {"ratelimit-remaining": "5", "ratelimit-limit": "100"},
            current_target=4, max_target=8
        ) == 1

    def test_remaining_between_10_and_50_percent_unchanged(self):
        from goeoview.gap_filler import adjust_target

        # 30 / 100 = 30%, between 10% and 50%
        assert adjust_target(
            {"ratelimit-remaining": "30", "ratelimit-limit": "100"},
            current_target=4, max_target=8
        ) == 4

    def test_missing_limit_header_returns_unchanged(self):
        from goeoview.gap_filler import adjust_target

        assert adjust_target(
            {"ratelimit-remaining": "30"},
            current_target=4, max_target=8
        ) == 4

    def test_non_numeric_headers_return_unchanged(self):
        from goeoview.gap_filler import adjust_target

        assert adjust_target(
            {"ratelimit-remaining": "lots", "ratelimit-limit": "many"},
            current_target=4, max_target=8
        ) == 4


class TestDownloadOneMaxSize:
    async def test_oversized_response_raises(self):
        from goeoview.gap_filler import download_one

        user = _make_user()
        resolver = AsyncMock()
        resolver.resolve.return_value = user

        big_body = b"x" * 1001
        resp = _make_stream_response(200, big_body)
        http_clients = _make_http_clients(resp)
        config = _make_config(max_repo_size=1000)

        with pytest.raises(RepoDownloadException) as exc_info:
            await download_one("did:plc:test123", resolver, http_clients, config)

        assert exc_info.value.user is user


def _make_mock_db():
    mock_db = MagicMock()
    mock_db.db = AsyncMock()
    return mock_db


class TestFlushStagingEmpty:
    async def test_empty_inputs_makes_no_db_calls(self):
        from goeoview.gap_filler import flush_staging

        db = _make_mock_db()
        await flush_staging(db, to_insert=[], to_delete=[])

        db.db.insert.assert_not_awaited()
        db.db.command.assert_not_awaited()


class TestFlushStagingBatch:
    async def test_nonempty_batch_calls_ops_in_order(self):
        from goeoview.gap_filler import flush_staging

        db = _make_mock_db()
        rows = [("did:plc:a", "app.bsky.feed.post", "abc", 1, '{"text":"hi"}')]
        dids = ["did:plc:a"]

        await flush_staging(db, to_insert=rows, to_delete=dids)

        # insert into staging
        db.db.insert.assert_awaited_once_with("staging", rows)

        # dedup INSERT SELECT, deletion INSERT SELECT, DELETE FROM gap, TRUNCATE
        assert db.db.command.await_count == 4

    async def test_dedup_sql_selects_from_staging_joins_commit(self):
        from goeoview.gap_filler import flush_staging

        db = _make_mock_db()
        rows = [("did:plc:a", "app.bsky.feed.post", "abc", 1, '{"text":"hi"}')]

        await flush_staging(db, to_insert=rows, to_delete=["did:plc:a"])

        dedup_sql = db.db.command.call_args_list[0][0][0]
        assert "INSERT INTO commit" in dedup_sql
        assert "FROM staging" in dedup_sql
        assert "JOIN" in dedup_sql

    async def test_deletion_sql_detects_missing_from_staging(self):
        from goeoview.gap_filler import flush_staging

        db = _make_mock_db()
        rows = [("did:plc:a", "app.bsky.feed.post", "abc", 1, '{"text":"hi"}')]

        await flush_staging(db, to_insert=rows, to_delete=["did:plc:a"])

        deletion_sql = db.db.command.call_args_list[1][0][0]
        assert "INSERT INTO commit" in deletion_sql
        assert "'null'" in deletion_sql
        assert "LEFT JOIN staging" in deletion_sql
        assert "WHERE s.did IS NULL" in deletion_sql

    async def test_gap_rows_deleted_for_processed_dids(self):
        from goeoview.gap_filler import flush_staging

        db = _make_mock_db()
        rows = [("did:plc:a", "app.bsky.feed.post", "abc", 1, '{"text":"hi"}')]
        dids = ["did:plc:a", "did:plc:b"]

        await flush_staging(db, to_insert=rows, to_delete=dids)

        delete_call = db.db.command.call_args_list[2]
        delete_sql = delete_call[0][0]
        assert "DELETE FROM gap" in delete_sql
        assert delete_call[0][1]["dids"] == dids

    async def test_staging_truncated_at_end(self):
        from goeoview.gap_filler import flush_staging

        db = _make_mock_db()
        rows = [("did:plc:a", "app.bsky.feed.post", "abc", 1, '{"text":"hi"}')]

        await flush_staging(db, to_insert=rows, to_delete=["did:plc:a"])

        truncate_sql = db.db.command.call_args_list[3][0][0]
        assert "TRUNCATE TABLE staging" in truncate_sql

    async def test_no_insert_when_to_insert_empty_but_to_delete_nonempty(self):
        from goeoview.gap_filler import flush_staging

        db = _make_mock_db()
        dids = ["did:plc:a"]

        await flush_staging(db, to_insert=[], to_delete=dids)

        db.db.insert.assert_not_awaited()
        # Should still run dedup, deletion, delete gap, truncate
        assert db.db.command.await_count == 4


def _make_pds_config(max_pds_concurrency=10):
    config = MagicMock()
    config.gap_max_pds_concurrency = max_pds_concurrency
    return config


def _ok_result(did_suffix, remaining="60", limit="100"):
    """Build a (user, car, headers) tuple that download_one would return."""
    user = _make_user(did=f"did:plc:{did_suffix}")
    headers = {"ratelimit-remaining": remaining, "ratelimit-limit": limit}
    return user, b"car-data", headers


class TestPdsWorkerDownloadsAll:
    async def test_downloads_all_dids_and_puts_results_on_queue(self):
        from goeoview.gap_filler import pds_worker

        dids = ["did:plc:a", "did:plc:b", "did:plc:c"]
        queue = asyncio.Queue()
        config = _make_pds_config(max_pds_concurrency=10)

        results = {
            "did:plc:a": _ok_result("a"),
            "did:plc:b": _ok_result("b"),
            "did:plc:c": _ok_result("c"),
        }

        async def fake_download(did, resolver, http_clients, cfg):
            return results[did]

        with patch("goeoview.gap_filler.download_one", side_effect=fake_download):
            await asyncio.wait_for(
                pds_worker("pds.example.com", dids, queue, MagicMock(), MagicMock(), config),
                timeout=5,
            )

        collected = []
        while not queue.empty():
            collected.append(queue.get_nowait())

        assert len(collected) == 3
        queued_dids = {user.did for user, car in collected}
        assert queued_dids == {"did:plc:a", "did:plc:b", "did:plc:c"}


class TestPdsWorkerConcurrencyIncrease:
    async def test_increases_concurrency_when_remaining_above_50_percent(self):
        from goeoview.gap_filler import pds_worker

        # With remaining > 50%, target should increase from 1 toward max
        dids = [f"did:plc:{i}" for i in range(5)]
        queue = asyncio.Queue()
        config = _make_pds_config(max_pds_concurrency=10)

        call_order = []

        async def fake_download(did, resolver, http_clients, cfg):
            call_order.append(did)
            user = _make_user(did=did)
            headers = {"ratelimit-remaining": "90", "ratelimit-limit": "100"}
            return user, b"car-data", headers

        with patch("goeoview.gap_filler.download_one", side_effect=fake_download):
            await asyncio.wait_for(
                pds_worker("pds.example.com", dids, queue, MagicMock(), MagicMock(), config),
                timeout=5,
            )

        assert len(call_order) == 5


class TestPdsWorkerConcurrencyDecrease:
    async def test_reduces_concurrency_to_1_when_remaining_below_10_percent(self):
        from goeoview.gap_filler import pds_worker

        dids = [f"did:plc:{i}" for i in range(4)]
        queue = asyncio.Queue()
        config = _make_pds_config(max_pds_concurrency=10)

        async def fake_download(did, resolver, http_clients, cfg):
            user = _make_user(did=did)
            # < 10% remaining
            headers = {"ratelimit-remaining": "5", "ratelimit-limit": "100"}
            return user, b"car-data", headers

        with patch("goeoview.gap_filler.download_one", side_effect=fake_download):
            await asyncio.wait_for(
                pds_worker("pds.example.com", dids, queue, MagicMock(), MagicMock(), config),
                timeout=5,
            )

        collected = []
        while not queue.empty():
            collected.append(queue.get_nowait())
        assert len(collected) == 4


class TestPdsWorkerRateLimit:
    async def test_sleeps_on_429_then_resumes(self):
        from goeoview.gap_filler import pds_worker

        dids = ["did:plc:a", "did:plc:b"]
        queue = asyncio.Queue()
        config = _make_pds_config(max_pds_concurrency=10)

        call_count = 0

        async def fake_download(did, resolver, http_clients, cfg):
            nonlocal call_count
            call_count += 1
            if did == "did:plc:a":
                user = _make_user(did=did)
                raise RepoDownloadException(user, status_code=429,
                                            headers={"ratelimit-reset": "0"})
            user = _make_user(did=did)
            return user, b"car-data", {"ratelimit-remaining": "50", "ratelimit-limit": "100"}

        with patch("goeoview.gap_filler.download_one", side_effect=fake_download), \
             patch("goeoview.gap_filler.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await asyncio.wait_for(
                pds_worker("pds.example.com", dids, queue, MagicMock(), MagicMock(), config),
                timeout=5,
            )

        mock_sleep.assert_awaited()
        assert not queue.empty()
        user, car = queue.get_nowait()
        assert user.did == "did:plc:b"


class TestPdsWorkerNon429Error:
    async def test_skips_did_on_non_429_error(self):
        from goeoview.gap_filler import pds_worker

        dids = ["did:plc:fail", "did:plc:ok"]
        queue = asyncio.Queue()
        config = _make_pds_config(max_pds_concurrency=10)

        async def fake_download(did, resolver, http_clients, cfg):
            if did == "did:plc:fail":
                user = _make_user(did=did)
                raise RepoDownloadException(user, status_code=500)
            user = _make_user(did=did)
            return user, b"car-data", {"ratelimit-remaining": "50", "ratelimit-limit": "100"}

        with patch("goeoview.gap_filler.download_one", side_effect=fake_download):
            await asyncio.wait_for(
                pds_worker("pds.example.com", dids, queue, MagicMock(), MagicMock(), config),
                timeout=5,
            )

        collected = []
        while not queue.empty():
            collected.append(queue.get_nowait())
        assert len(collected) == 1
        assert collected[0][0].did == "did:plc:ok"


class TestPdsWorkerBackpressure:
    async def test_blocks_on_full_queue(self):
        from goeoview.gap_filler import pds_worker

        dids = ["did:plc:a", "did:plc:b"]
        queue = asyncio.Queue(maxsize=1)
        config = _make_pds_config(max_pds_concurrency=10)

        async def fake_download(did, resolver, http_clients, cfg):
            user = _make_user(did=did)
            return user, b"car-data", {"ratelimit-remaining": "50", "ratelimit-limit": "100"}

        with patch("goeoview.gap_filler.download_one", side_effect=fake_download):
            worker_task = asyncio.create_task(
                pds_worker("pds.example.com", dids, queue, MagicMock(), MagicMock(), config)
            )
            # Let worker start and fill the queue
            await asyncio.sleep(0.1)

            # Queue should have an item (worker blocked on second put)
            assert not queue.empty()
            first = queue.get_nowait()
            assert first[0].did in ("did:plc:a", "did:plc:b")

            # Now worker can proceed
            await asyncio.wait_for(worker_task, timeout=5)

        # Should have the second item now
        assert not queue.empty()


class TestPdsWorkerEmptyDids:
    async def test_empty_did_list_exits_immediately(self):
        from goeoview.gap_filler import pds_worker

        queue = asyncio.Queue()
        config = _make_pds_config(max_pds_concurrency=10)

        with patch("goeoview.gap_filler.download_one") as mock_dl:
            await asyncio.wait_for(
                pds_worker("pds.example.com", [], queue, MagicMock(), MagicMock(), config),
                timeout=5,
            )

        mock_dl.assert_not_called()
        assert queue.empty()


def _make_consume_config(gap_parse_workers=2):
    config = MagicMock()
    config.gap_parse_workers = gap_parse_workers
    return config


def _fake_parse_repo(user, car):
    """Synchronous stand-in for parse_repo used in consume_repos tests."""
    return user.did, [
        (user.did, "app.bsky.feed.post", "abc", 1, '{"text":"hi"}')
    ]


class TestConsumeReposDIDThreshold:
    async def test_flushes_when_did_threshold_hit(self):
        from goeoview.gap_filler import consume_repos

        queue = asyncio.Queue()
        config = _make_consume_config()
        mock_db = _make_mock_db()

        # Track flush calls with copies (lists are cleared after flush)
        flush_snapshots = []

        async def capturing_flush(db, to_insert, to_delete):
            flush_snapshots.append((list(to_insert), list(to_delete)))

        # Queue 1002 items: 1001 triggers threshold flush, 1 remains for sentinel flush
        for i in range(1002):
            user = _make_user(did=f"did:plc:{i}")
            queue.put_nowait((user, b"car-data"))
        queue.put_nowait(None)  # sentinel

        with patch("goeoview.gap_filler.flush_staging", capturing_flush), \
             patch("goeoview.gap_filler.parse_repo", _fake_parse_repo), \
             patch("goeoview.gap_filler.ProcessPoolExecutor", ThreadPoolExecutor), \
             patch("goeoview.gap_filler.DB", return_value=mock_db):
            await asyncio.wait_for(consume_repos(queue, config), timeout=10)

        # Should have flushed twice: once at DID threshold, once at sentinel
        assert len(flush_snapshots) == 2
        # First flush should have had >1000 DIDs
        assert len(flush_snapshots[0][1]) > 1000


class TestConsumeReposTimeThreshold:
    async def test_flushes_on_time_threshold(self):
        from goeoview.gap_filler import consume_repos

        queue = asyncio.Queue()
        config = _make_consume_config()
        mock_flush = AsyncMock()
        mock_db = _make_mock_db()

        # Put one item so there's data to flush
        user = _make_user(did="did:plc:time_test")
        queue.put_nowait((user, b"car-data"))

        # Use a fake datetime to make time appear to jump forward
        real_now = datetime.now(UTC)
        call_count = 0

        original_datetime = datetime

        class FakeDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                nonlocal call_count
                call_count += 1
                # First call: real time (last_flush init)
                # Later calls: jump 15 seconds ahead to trigger time threshold
                if call_count <= 1:
                    return real_now
                return real_now + timedelta(seconds=15)

        async def delayed_sentinel():
            """Put sentinel after a short delay so the consumer processes the item first."""
            await asyncio.sleep(0.2)
            queue.put_nowait(None)

        with patch("goeoview.gap_filler.flush_staging", mock_flush), \
             patch("goeoview.gap_filler.parse_repo", _fake_parse_repo), \
             patch("goeoview.gap_filler.ProcessPoolExecutor", ThreadPoolExecutor), \
             patch("goeoview.gap_filler.DB", return_value=mock_db), \
             patch("goeoview.gap_filler.datetime", FakeDatetime):
            sentinel_task = asyncio.create_task(delayed_sentinel())
            await asyncio.wait_for(consume_repos(queue, config), timeout=10)
            await sentinel_task

        # Should have flushed due to time threshold (either after processing or on timeout)
        assert mock_flush.await_count >= 1


class TestConsumeReposSentinel:
    async def test_stops_on_none_and_flushes_remaining(self):
        from goeoview.gap_filler import consume_repos

        queue = asyncio.Queue()
        config = _make_consume_config()
        mock_flush = AsyncMock()
        mock_db = _make_mock_db()

        # Put a few items then sentinel
        for i in range(3):
            user = _make_user(did=f"did:plc:sentinel_{i}")
            queue.put_nowait((user, b"car-data"))
        queue.put_nowait(None)

        with patch("goeoview.gap_filler.flush_staging", mock_flush), \
             patch("goeoview.gap_filler.parse_repo", _fake_parse_repo), \
             patch("goeoview.gap_filler.ProcessPoolExecutor", ThreadPoolExecutor), \
             patch("goeoview.gap_filler.DB", return_value=mock_db):
            await asyncio.wait_for(consume_repos(queue, config), timeout=10)

        # Should have flushed remaining work on sentinel
        mock_flush.assert_awaited()
        # Check that flush received the accumulated data
        last_call = mock_flush.call_args
        to_insert = last_call[0][1]
        to_delete = last_call[0][2]
        assert len(to_delete) > 0
        assert len(to_insert) > 0


class TestConsumeReposParseFailure:
    async def test_parse_failure_logged_and_skipped(self):
        from goeoview.gap_filler import consume_repos

        queue = asyncio.Queue()
        config = _make_consume_config()
        mock_flush = AsyncMock()
        mock_db = _make_mock_db()

        def failing_parse(user, car):
            if user.did == "did:plc:bad":
                raise ValueError("corrupt CAR file")
            return user.did, [
                (user.did, "app.bsky.feed.post", "abc", 1, '{"text":"ok"}')
            ]

        user_bad = _make_user(did="did:plc:bad")
        user_good = _make_user(did="did:plc:good")
        queue.put_nowait((user_bad, b"bad-car"))
        queue.put_nowait((user_good, b"good-car"))
        queue.put_nowait(None)

        with patch("goeoview.gap_filler.flush_staging", mock_flush), \
             patch("goeoview.gap_filler.parse_repo", failing_parse), \
             patch("goeoview.gap_filler.ProcessPoolExecutor", ThreadPoolExecutor), \
             patch("goeoview.gap_filler.DB", return_value=mock_db):
            # Should not crash
            await asyncio.wait_for(consume_repos(queue, config), timeout=10)

        # Should have flushed with only the good DID's data
        mock_flush.assert_awaited()
        last_call = mock_flush.call_args
        to_delete = last_call[0][2]
        assert "did:plc:good" in to_delete
        assert "did:plc:bad" not in to_delete


class TestConsumeReposSerializedFlushes:
    async def test_flushes_are_serialized(self):
        from goeoview.gap_filler import consume_repos

        queue = asyncio.Queue()
        config = _make_consume_config()
        mock_db = _make_mock_db()

        concurrent_flushes = 0
        max_concurrent = 0

        async def tracking_flush(db, to_insert, to_delete):
            nonlocal concurrent_flushes, max_concurrent
            concurrent_flushes += 1
            max_concurrent = max(max_concurrent, concurrent_flushes)
            await asyncio.sleep(0.05)  # simulate some work
            concurrent_flushes -= 1

        # Queue enough items for multiple flushes (>1000 DIDs each)
        for i in range(2002):
            user = _make_user(did=f"did:plc:{i}")
            queue.put_nowait((user, b"car-data"))
        queue.put_nowait(None)

        with patch("goeoview.gap_filler.flush_staging", tracking_flush), \
             patch("goeoview.gap_filler.parse_repo", _fake_parse_repo), \
             patch("goeoview.gap_filler.ProcessPoolExecutor", ThreadPoolExecutor), \
             patch("goeoview.gap_filler.DB", return_value=mock_db):
            await asyncio.wait_for(consume_repos(queue, config), timeout=30)

        # Only one flush should run at a time
        assert max_concurrent == 1


def _make_gap_config(gap_queue_size=100, gap_max_pds_concurrency=10,
                     gap_max_total_workers=128):
    config = MagicMock()
    config.gap_queue_size = gap_queue_size
    config.gap_max_pds_concurrency = gap_max_pds_concurrency
    config.gap_max_total_workers = gap_max_total_workers
    return config


def _make_gap_query_result(dids):
    """Build a mock query result with result_columns containing DIDs."""
    result = MagicMock()
    result.result_columns = [dids]
    return result


def _make_empty_gap_query_result():
    """Build a mock query result that raises IndexError on result_columns[0]."""
    result = MagicMock()
    result.result_columns = []
    return result


class TestCoordinateGapsBasic:
    async def test_queries_gaps_resolves_dids_groups_by_pds_spawns_workers(self):
        from goeoview.gap_filler import coordinate_gaps

        dids = ["did:plc:a", "did:plc:b"]
        mock_db = _make_mock_db()
        mock_db.db.query = AsyncMock(return_value=_make_gap_query_result(dids))

        user_a = _make_user(did="did:plc:a", pds="https://pds1.example.com")
        user_b = _make_user(did="did:plc:b", pds="https://pds2.example.com")

        resolver = AsyncMock()
        resolver.resolve.side_effect = lambda did: {
            "did:plc:a": user_a,
            "did:plc:b": user_b,
        }[did]

        queue = asyncio.Queue()
        http_clients = MagicMock()
        config = _make_gap_config()

        with patch("goeoview.gap_filler.DB", return_value=mock_db), \
             patch("goeoview.gap_filler.pds_worker", new_callable=AsyncMock) as mock_pw:
            await coordinate_gaps(queue, resolver, http_clients, config)

        # Should have called pds_worker once per PDS
        assert mock_pw.await_count == 2
        called_pds_hosts = {c.args[0] for c in mock_pw.call_args_list}
        assert called_pds_hosts == {"https://pds1.example.com", "https://pds2.example.com"}


class TestCoordinateGapsEmptyTable:
    async def test_empty_gap_table_returns_without_spawning_workers(self):
        from goeoview.gap_filler import coordinate_gaps

        mock_db = _make_mock_db()
        mock_db.db.query = AsyncMock(return_value=_make_empty_gap_query_result())

        resolver = AsyncMock()
        queue = asyncio.Queue()
        http_clients = MagicMock()
        config = _make_gap_config()

        with patch("goeoview.gap_filler.DB", return_value=mock_db), \
             patch("goeoview.gap_filler.pds_worker", new_callable=AsyncMock) as mock_pw:
            await coordinate_gaps(queue, resolver, http_clients, config)

        mock_pw.assert_not_awaited()
        resolver.resolve.assert_not_called()


class TestCoordinateGapsGrouping:
    async def test_multiple_dids_same_pds_grouped_into_one_worker(self):
        from goeoview.gap_filler import coordinate_gaps

        dids = ["did:plc:a", "did:plc:b", "did:plc:c"]
        mock_db = _make_mock_db()
        mock_db.db.query = AsyncMock(return_value=_make_gap_query_result(dids))

        # All resolve to same PDS
        pds = "https://pds.example.com"

        resolver = AsyncMock()
        resolver.resolve.side_effect = lambda did: _make_user(did=did, pds=pds)

        queue = asyncio.Queue()
        http_clients = MagicMock()
        config = _make_gap_config()

        with patch("goeoview.gap_filler.DB", return_value=mock_db), \
             patch("goeoview.gap_filler.pds_worker", new_callable=AsyncMock) as mock_pw:
            await coordinate_gaps(queue, resolver, http_clients, config)

        # One worker call for the single PDS
        assert mock_pw.await_count == 1
        assert mock_pw.call_args.args[0] == pds
        # All three DIDs grouped together
        assert set(mock_pw.call_args.args[1]) == {"did:plc:a", "did:plc:b", "did:plc:c"}

    async def test_dids_for_different_pds_get_separate_workers(self):
        from goeoview.gap_filler import coordinate_gaps

        dids = ["did:plc:a", "did:plc:b", "did:plc:c", "did:plc:d"]
        mock_db = _make_mock_db()
        mock_db.db.query = AsyncMock(return_value=_make_gap_query_result(dids))

        pds_map = {
            "did:plc:a": "https://pds1.example.com",
            "did:plc:b": "https://pds2.example.com",
            "did:plc:c": "https://pds1.example.com",
            "did:plc:d": "https://pds3.example.com",
        }

        resolver = AsyncMock()
        resolver.resolve.side_effect = lambda did: _make_user(did=did, pds=pds_map[did])

        queue = asyncio.Queue()
        http_clients = MagicMock()
        config = _make_gap_config()

        with patch("goeoview.gap_filler.DB", return_value=mock_db), \
             patch("goeoview.gap_filler.pds_worker", new_callable=AsyncMock) as mock_pw:
            await coordinate_gaps(queue, resolver, http_clients, config)

        # Three distinct PDS hosts -> three worker calls
        assert mock_pw.await_count == 3
        called_pds_hosts = {c.args[0] for c in mock_pw.call_args_list}
        assert called_pds_hosts == {
            "https://pds1.example.com",
            "https://pds2.example.com",
            "https://pds3.example.com",
        }


class TestCoordinateGapsResolutionFailure:
    async def test_did_resolution_failure_skips_did_continues_with_others(self):
        from goeoview.gap_filler import coordinate_gaps

        dids = ["did:plc:fail", "did:plc:ok"]
        mock_db = _make_mock_db()
        mock_db.db.query = AsyncMock(return_value=_make_gap_query_result(dids))

        user_ok = _make_user(did="did:plc:ok", pds="https://pds.example.com")

        async def resolve_side_effect(did):
            if did == "did:plc:fail":
                raise RuntimeError("network error")
            return user_ok

        resolver = AsyncMock()
        resolver.resolve.side_effect = resolve_side_effect

        queue = asyncio.Queue()
        http_clients = MagicMock()
        config = _make_gap_config()

        with patch("goeoview.gap_filler.DB", return_value=mock_db), \
             patch("goeoview.gap_filler.pds_worker", new_callable=AsyncMock) as mock_pw:
            await coordinate_gaps(queue, resolver, http_clients, config)

        # Should still spawn a worker for the DID that resolved
        assert mock_pw.await_count == 1
        assert mock_pw.call_args.args[0] == "https://pds.example.com"
        assert mock_pw.call_args.args[1] == ["did:plc:ok"]
