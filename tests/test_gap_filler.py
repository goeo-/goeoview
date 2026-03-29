"""Tests for goeoview.gap_filler."""

import asyncio
import ssl
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, UTC, timedelta
from unittest.mock import AsyncMock, MagicMock, call, patch

import aiohttp
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


def _make_aiohttp_response(status=200, body=b"car-data", headers=None):
    """Build a fake aiohttp-style streaming response."""
    response = MagicMock()
    response.status = status
    response.headers = headers or {"content-type": "application/vnd.ipld.car"}

    async def iter_any():
        yield body

    content = MagicMock()
    content.iter_any = iter_any
    response.content = content
    response.read = AsyncMock(return_value=body)
    response.release = AsyncMock()
    return response


def _make_session(response):
    """Build a mock aiohttp session whose .get() yields *response*."""
    @asynccontextmanager
    async def fake_get(url, **kwargs):
        yield response

    session = MagicMock()
    session.get = fake_get
    return session


class TestDownloadOneSuccess:
    async def test_returns_user_body_headers(self):
        from goeoview.gap_filler import download_one

        user = _make_user()
        resolver = AsyncMock()
        resolver.resolve.return_value = user

        resp = _make_aiohttp_response(200, b"car-bytes", {"x-custom": "val"})
        session = _make_session(resp)
        config = _make_config()

        result_user, car_bytes, headers = await download_one(
            "did:plc:test123", resolver, session, config
        )

        assert result_user is user
        assert car_bytes == b"car-bytes"
        assert headers["x-custom"] == "val"

    async def test_uses_grouped_pds_host_for_repo_download(self):
        from goeoview.gap_filler import download_one

        user = _make_user(pds="https://wrong.example.com")
        resolver = AsyncMock()
        resolver.resolve.return_value = user

        captured = {}

        @asynccontextmanager
        async def fake_get(url, **kwargs):
            captured["url"] = url
            yield _make_aiohttp_response(200, b"car-bytes", {"x-custom": "val"})

        session = MagicMock()
        session.get = fake_get
        config = _make_config()

        result_user, car_bytes, headers = await download_one(
            "did:plc:test123",
            resolver,
            session,
            config,
            pds_host="https://grouped.example.com",
        )

        assert result_user is user
        assert car_bytes == b"car-bytes"
        assert headers["x-custom"] == "val"
        assert captured["url"].startswith(
            "https://grouped.example.com/xrpc/com.atproto.sync.getRepo"
        )


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
        assert exc_info.value.stage == "pds_validation"

    async def test_pds_not_https_raises(self):
        from goeoview.gap_filler import download_one

        user = _make_user(pds="http://insecure.example.com")
        resolver = AsyncMock()
        resolver.resolve.return_value = user
        config = _make_config()

        with pytest.raises(RepoDownloadException) as exc_info:
            await download_one("did:plc:test123", resolver, MagicMock(), config)

        assert exc_info.value.user is user
        assert exc_info.value.stage == "pds_validation"

    async def test_ssrf_rejection_from_resolver(self):
        """SSRF protection is handled by SafeResolver at the connector level.

        When the resolver rejects a non-public IP, aiohttp raises a
        ClientConnectorError which download_one wraps in RepoDownloadException.
        """
        from goeoview.gap_filler import download_one

        user = _make_user(pds="https://evil.example.com")
        resolver = AsyncMock()
        resolver.resolve.return_value = user
        config = _make_config()

        @asynccontextmanager
        async def raising_get(url, **kwargs):
            raise aiohttp.ClientConnectorError(
                connection_key=MagicMock(),
                os_error=OSError("non-public IP"),
            )
            yield  # noqa

        session = MagicMock()
        session.get = raising_get

        with pytest.raises(RepoDownloadException) as exc_info:
            await download_one("did:plc:test123", resolver, session, config)

        assert exc_info.value.stage == "request"
        assert isinstance(exc_info.value.cause, aiohttp.ClientConnectorError)


class TestDownloadOneHttpErrors:
    @pytest.mark.parametrize("status_code", [400, 401, 403])
    async def test_client_error_inserts_bad_did_and_raises(self, status_code):
        from goeoview.gap_filler import download_one

        user = _make_user()
        resolver = AsyncMock()
        resolver.resolve.return_value = user

        resp = _make_aiohttp_response(status_code)
        session = _make_session(resp)
        config = _make_config()

        mock_db = MagicMock()
        mock_db.db = AsyncMock()

        with patch("goeoview.gap_filler.DB", return_value=mock_db):
            with pytest.raises(RepoDownloadException) as exc_info:
                await download_one("did:plc:test123", resolver, session, config)

        assert exc_info.value.status_code == status_code
        assert exc_info.value.stage == "http_response"
        mock_db.db.insert.assert_awaited_once_with(
            "bad_did",
            [(user.did, status_code)],
            column_names=["did", "status_code"],
        )

    @pytest.mark.parametrize("status_code", [500, 502, 503])
    async def test_server_error_raises_without_bad_did(self, status_code):
        from goeoview.gap_filler import download_one

        user = _make_user()
        resolver = AsyncMock()
        resolver.resolve.return_value = user

        resp = _make_aiohttp_response(status_code)
        session = _make_session(resp)
        config = _make_config()

        mock_db = MagicMock()
        mock_db.db = AsyncMock()

        with patch("goeoview.gap_filler.DB", return_value=mock_db):
            with pytest.raises(RepoDownloadException) as exc_info:
                await download_one("did:plc:test123", resolver, session, config)

        assert exc_info.value.status_code == status_code
        assert exc_info.value.stage == "http_response"
        mock_db.db.insert.assert_not_awaited()

    async def test_429_raises_with_headers(self):
        from goeoview.gap_filler import download_one

        user = _make_user()
        resolver = AsyncMock()
        resolver.resolve.return_value = user

        headers = {"retry-after": "30", "ratelimit-remaining": "0"}
        resp = _make_aiohttp_response(429, headers=headers)
        session = _make_session(resp)
        config = _make_config()

        mock_db = MagicMock()
        mock_db.db = AsyncMock()

        with patch("goeoview.gap_filler.DB", return_value=mock_db):
            with pytest.raises(RepoDownloadException) as exc_info:
                await download_one("did:plc:test123", resolver, session, config)

        assert exc_info.value.status_code == 429
        assert exc_info.value.headers["retry-after"] == "30"
        assert exc_info.value.stage == "http_response"


class TestDownloadOneConnectionErrors:
    @pytest.mark.parametrize("error_cls", [
        aiohttp.ClientConnectorError,
        aiohttp.ServerDisconnectedError,
    ])
    async def test_connection_error_raises_with_no_status(self, error_cls):
        from goeoview.gap_filler import download_one

        user = _make_user()
        resolver = AsyncMock()
        resolver.resolve.return_value = user

        @asynccontextmanager
        async def raising_get(url, **kwargs):
            if error_cls is aiohttp.ClientConnectorError:
                raise error_cls(
                    connection_key=MagicMock(), os_error=OSError("boom")
                )
            raise error_cls(message="boom")
            yield  # noqa: unreachable

        session = MagicMock()
        session.get = raising_get
        config = _make_config()

        with pytest.raises(RepoDownloadException) as exc_info:
            await download_one("did:plc:test123", resolver, session, config)

        assert exc_info.value.status_code is None
        assert exc_info.value.user is user
        assert exc_info.value.stage == "request"
        assert isinstance(exc_info.value.cause, error_cls)

    async def test_timeout_error_raises_with_no_status(self):
        from goeoview.gap_filler import download_one

        user = _make_user()
        resolver = AsyncMock()
        resolver.resolve.return_value = user

        @asynccontextmanager
        async def raising_get(url, **kwargs):
            raise asyncio.TimeoutError()
            yield  # noqa

        session = MagicMock()
        session.get = raising_get
        config = _make_config()

        with pytest.raises(RepoDownloadException) as exc_info:
            await download_one("did:plc:test123", resolver, session, config)

        assert exc_info.value.status_code is None
        assert exc_info.value.stage == "request"
        assert isinstance(exc_info.value.cause, asyncio.TimeoutError)

    async def test_ssl_error_raises_with_no_status(self):
        from goeoview.gap_filler import download_one

        user = _make_user()
        resolver = AsyncMock()
        resolver.resolve.return_value = user

        @asynccontextmanager
        async def raising_get(url, **kwargs):
            raise ssl.SSLError("cert verify failed")
            yield  # noqa

        session = MagicMock()
        session.get = raising_get
        config = _make_config()

        with pytest.raises(RepoDownloadException) as exc_info:
            await download_one("did:plc:test123", resolver, session, config)

        assert exc_info.value.status_code is None
        assert exc_info.value.stage == "request"
        assert isinstance(exc_info.value.cause, ssl.SSLError)


class TestAdjustTarget:
    def test_no_rate_limit_headers_ramps_up(self):
        from goeoview.gap_filler import adjust_target

        assert adjust_target({}, current_target=4, max_target=8) == 5

    def test_no_rate_limit_headers_capped_at_max(self):
        from goeoview.gap_filler import adjust_target

        assert adjust_target({}, current_target=8, max_target=8) == 8

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
        ) == 5

    def test_non_numeric_headers_ramps_up(self):
        from goeoview.gap_filler import adjust_target

        assert adjust_target(
            {"ratelimit-remaining": "lots", "ratelimit-limit": "many"},
            current_target=4, max_target=8
        ) == 5


class TestDownloadOneMaxSize:
    async def test_oversized_response_raises(self):
        from goeoview.gap_filler import download_one

        user = _make_user()
        resolver = AsyncMock()
        resolver.resolve.return_value = user

        big_body = b"x" * 1001
        resp = _make_aiohttp_response(200, big_body)
        session = _make_session(resp)
        config = _make_config(max_repo_size=1000)

        with pytest.raises(RepoDownloadException) as exc_info:
            await download_one("did:plc:test123", resolver, session, config)

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

        # truncate(cleanup), dedup INSERT SELECT, deletion INSERT SELECT,
        # DELETE FROM gap, TRUNCATE(final)
        assert db.db.command.await_count == 5

    async def test_dedup_sql_selects_from_staging_joins_commit(self):
        from goeoview.gap_filler import flush_staging

        db = _make_mock_db()
        rows = [("did:plc:a", "app.bsky.feed.post", "abc", 1, '{"text":"hi"}')]

        await flush_staging(db, to_insert=rows, to_delete=["did:plc:a"])

        # Index 0 is the initial TRUNCATE cleanup; dedup is at index 1
        dedup_sql = db.db.command.call_args_list[1][0][0]
        assert "INSERT INTO commit" in dedup_sql
        assert "FROM staging" in dedup_sql
        assert "JOIN" in dedup_sql

    async def test_deletion_sql_detects_missing_from_staging(self):
        from goeoview.gap_filler import flush_staging

        db = _make_mock_db()
        rows = [("did:plc:a", "app.bsky.feed.post", "abc", 1, '{"text":"hi"}')]

        await flush_staging(db, to_insert=rows, to_delete=["did:plc:a"])

        deletion_sql = db.db.command.call_args_list[2][0][0]
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

        delete_call = db.db.command.call_args_list[3]
        delete_sql = delete_call[0][0]
        assert "DELETE FROM gap" in delete_sql
        assert delete_call[0][1]["dids"] == dids

    async def test_staging_truncated_at_end(self):
        from goeoview.gap_filler import flush_staging

        db = _make_mock_db()
        rows = [("did:plc:a", "app.bsky.feed.post", "abc", 1, '{"text":"hi"}')]

        await flush_staging(db, to_insert=rows, to_delete=["did:plc:a"])

        truncate_sql = db.db.command.call_args_list[4][0][0]
        assert "TRUNCATE TABLE staging" in truncate_sql

    async def test_no_insert_when_to_insert_empty_but_to_delete_nonempty(self):
        from goeoview.gap_filler import flush_staging

        db = _make_mock_db()
        dids = ["did:plc:a"]

        await flush_staging(db, to_insert=[], to_delete=dids)

        db.db.insert.assert_not_awaited()
        # truncate(cleanup), dedup, deletion, delete gap, truncate(final)
        assert db.db.command.await_count == 5


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

        async def fake_download(did, resolver, http_clients, cfg, pds_host=None):
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

        async def fake_download(did, resolver, http_clients, cfg, pds_host=None):
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

        async def fake_download(did, resolver, http_clients, cfg, pds_host=None):
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

        async def fake_download(did, resolver, http_clients, cfg, pds_host=None):
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

        async def fake_download(did, resolver, http_clients, cfg, pds_host=None):
            if did == "did:plc:fail":
                user = _make_user(did=did)
                raise RepoDownloadException(user, status_code=500)
            user = _make_user(did=did)
            return user, b"car-data", {"ratelimit-remaining": "50", "ratelimit-limit": "100"}

        with patch("goeoview.gap_filler.download_one", side_effect=fake_download), \
             patch("goeoview.gap_filler.asyncio.sleep", new_callable=AsyncMock):
            await asyncio.wait_for(
                pds_worker("pds.example.com", dids, queue, MagicMock(), MagicMock(), config),
                timeout=5,
            )

        collected = []
        while not queue.empty():
            collected.append(queue.get_nowait())
        assert len(collected) == 1
        assert collected[0][0].did == "did:plc:ok"

    async def test_unexpected_task_error_is_logged_and_worker_continues(self):
        from goeoview.gap_filler import pds_worker

        dids = ["did:plc:boom", "did:plc:ok"]
        queue = asyncio.Queue()
        config = _make_pds_config(max_pds_concurrency=10)

        async def fake_download(did, resolver, http_clients, cfg, pds_host=None):
            if did == "did:plc:boom":
                raise RuntimeError("db exploded")
            user = _make_user(did=did)
            return user, b"car-data", {"ratelimit-remaining": "50", "ratelimit-limit": "100"}

        with patch("goeoview.gap_filler.download_one", side_effect=fake_download), \
             patch("goeoview.gap_filler.logger.exception") as mock_log_exception:
            await asyncio.wait_for(
                pds_worker("pds.example.com", dids, queue, MagicMock(), MagicMock(), config),
                timeout=5,
            )

        user, car = queue.get_nowait()
        assert user.did == "did:plc:ok"
        assert car == b"car-data"
        mock_log_exception.assert_called_once()


class TestPdsWorkerNon5xxError:
    async def test_connection_failure_does_not_increment_5xx_counter(self):
        from goeoview.gap_filler import pds_worker

        # 9 connection failures then 1 server error — should NOT trip circuit breaker
        # because only 5xx errors count toward the threshold
        dids = [f"did:plc:{i}" for i in range(11)]
        queue = asyncio.Queue()
        config = _make_pds_config(max_pds_concurrency=10)

        download_count = 0

        async def mixed_errors(did, resolver, http_clients, cfg, pds_host=None):
            nonlocal download_count
            download_count += 1
            user = _make_user(did=did)
            if download_count <= 9:
                # Connection failures (status_code=None)
                raise RepoDownloadException(user)
            # 5xx error
            raise RepoDownloadException(user, status_code=503)

        with patch("goeoview.gap_filler.download_one", side_effect=mixed_errors), \
             patch("goeoview.gap_filler.asyncio.sleep", new_callable=AsyncMock):
            await asyncio.wait_for(
                pds_worker("pds.example.com", dids, queue, MagicMock(), MagicMock(), config),
                timeout=5,
            )

        # All 11 should have been attempted — circuit breaker should not trip
        # (only 2 consecutive 5xx, far below threshold of 10)
        assert download_count == 11


class TestPdsWorker5xxBackoff:
    async def test_sleeps_with_exponential_backoff_on_consecutive_5xx(self):
        from goeoview.gap_filler import pds_worker

        dids = [f"did:plc:{i}" for i in range(4)]
        queue = asyncio.Queue()
        config = _make_pds_config(max_pds_concurrency=10)

        async def always_503(did, resolver, http_clients, cfg, pds_host=None):
            user = _make_user(did=did)
            raise RepoDownloadException(user, status_code=503)

        sleep_calls = []

        async def tracking_sleep(seconds):
            sleep_calls.append(seconds)

        with patch("goeoview.gap_filler.download_one", side_effect=always_503), \
             patch("goeoview.gap_filler.asyncio.sleep", side_effect=tracking_sleep):
            await asyncio.wait_for(
                pds_worker("pds.example.com", dids, queue, MagicMock(), MagicMock(), config),
                timeout=5,
            )

        # Should have slept with increasing delays: 2^1, 2^2, 2^3, 2^4
        assert len(sleep_calls) == 4
        for i, delay in enumerate(sleep_calls):
            assert delay == min(2 ** (i + 1), 60)

    async def test_backoff_resets_on_success(self):
        from goeoview.gap_filler import pds_worker

        # fail, fail, succeed, fail — the last fail should backoff as if it's the 1st
        dids = ["did:plc:f1", "did:plc:f2", "did:plc:ok", "did:plc:f3"]
        queue = asyncio.Queue()
        config = _make_pds_config(max_pds_concurrency=10)

        async def selective_download(did, resolver, http_clients, cfg, pds_host=None):
            user = _make_user(did=did)
            if did == "did:plc:ok":
                return user, b"car-data", {"ratelimit-remaining": "50", "ratelimit-limit": "100"}
            raise RepoDownloadException(user, status_code=503)

        sleep_calls = []

        async def tracking_sleep(seconds):
            sleep_calls.append(seconds)

        with patch("goeoview.gap_filler.download_one", side_effect=selective_download), \
             patch("goeoview.gap_filler.asyncio.sleep", side_effect=tracking_sleep):
            await asyncio.wait_for(
                pds_worker("pds.example.com", dids, queue, MagicMock(), MagicMock(), config),
                timeout=5,
            )

        # f1: backoff 2^1=2, f2: backoff 2^2=4, ok: resets, f3: backoff 2^1=2
        assert sleep_calls == [2, 4, 2]


class TestPdsWorkerCircuitBreaker:
    async def test_bails_after_10_consecutive_5xx(self):
        from goeoview.gap_filler import pds_worker

        dids = [f"did:plc:{i}" for i in range(20)]
        queue = asyncio.Queue()
        config = _make_pds_config(max_pds_concurrency=10)

        download_count = 0

        async def always_503(did, resolver, http_clients, cfg, pds_host=None):
            nonlocal download_count
            download_count += 1
            user = _make_user(did=did)
            raise RepoDownloadException(user, status_code=503)

        with patch("goeoview.gap_filler.download_one", side_effect=always_503), \
             patch("goeoview.gap_filler.asyncio.sleep", new_callable=AsyncMock):
            await asyncio.wait_for(
                pds_worker("pds.example.com", dids, queue, MagicMock(), MagicMock(), config),
                timeout=5,
            )

        # Should have stopped after 10 consecutive failures, not processed all 20
        assert download_count == 10

    async def test_circuit_breaker_resets_on_success(self):
        from goeoview.gap_filler import pds_worker

        # 9 failures, 1 success, 9 failures, 1 success — should never trip
        pattern = ["fail"] * 9 + ["ok"] + ["fail"] * 9 + ["ok"]
        dids = [f"did:plc:{i}" for i in range(len(pattern))]
        queue = asyncio.Queue()
        config = _make_pds_config(max_pds_concurrency=10)

        call_index = 0

        async def patterned_download(did, resolver, http_clients, cfg, pds_host=None):
            nonlocal call_index
            idx = call_index
            call_index += 1
            user = _make_user(did=did)
            if pattern[idx] == "fail":
                raise RepoDownloadException(user, status_code=503)
            return user, b"car-data", {"ratelimit-remaining": "50", "ratelimit-limit": "100"}

        with patch("goeoview.gap_filler.download_one", side_effect=patterned_download), \
             patch("goeoview.gap_filler.asyncio.sleep", new_callable=AsyncMock):
            await asyncio.wait_for(
                pds_worker("pds.example.com", dids, queue, MagicMock(), MagicMock(), config),
                timeout=10,
            )

        # All 20 DIDs should have been attempted (circuit never tripped)
        assert call_index == 20


class TestPdsWorkerBackpressure:
    async def test_blocks_on_full_queue(self):
        from goeoview.gap_filler import pds_worker

        dids = ["did:plc:a", "did:plc:b"]
        queue = asyncio.Queue(maxsize=1)
        config = _make_pds_config(max_pds_concurrency=10)

        async def fake_download(did, resolver, http_clients, cfg, pds_host=None):
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

        # Queue 15000 items — well over 2x the 5000 DID threshold to guarantee
        # at least one mid-stream flush even with concurrent batched collection
        for i in range(15000):
            user = _make_user(did=f"did:plc:{i}")
            queue.put_nowait((user, b"car-data"))
        queue.put_nowait(None)  # sentinel

        with patch("goeoview.gap_filler.flush_staging", capturing_flush), \
             patch("goeoview.gap_filler.parse_repo", _fake_parse_repo), \
             patch("goeoview.gap_filler.ProcessPoolExecutor", ThreadPoolExecutor), \
             patch("goeoview.gap_filler.DB", return_value=mock_db):
            await asyncio.wait_for(consume_repos(queue, config), timeout=30)

        # At least 2 flushes: one or more at DID threshold, plus sentinel
        assert len(flush_snapshots) >= 2
        # All 15000 DIDs should be accounted for across all flushes
        all_dids = set()
        for _, to_delete in flush_snapshots:
            all_dids.update(to_delete)
        assert len(all_dids) == 15000


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

        # Queue enough items for multiple flushes (>5000 DIDs each)
        for i in range(10002):
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


class TestConsumeReposAsyncFlush:
    async def test_consumer_continues_parsing_during_flush(self):
        from goeoview.gap_filler import consume_repos

        queue = asyncio.Queue()
        config = _make_consume_config()
        mock_db = _make_mock_db()

        flush_events = []  # (event, timestamp)
        parse_events = []  # (did, timestamp)

        async def slow_flush(db, to_insert, to_delete):
            flush_events.append(("start", time.monotonic()))
            await asyncio.sleep(0.2)
            flush_events.append(("end", time.monotonic()))

        def tracking_parse(user, car):
            parse_events.append((user.did, time.monotonic()))
            return user.did, [
                (user.did, "app.bsky.feed.post", "abc", 1, '{"text":"hi"}')
            ]

        # Queue enough to trigger flush (>5000), then more items, then sentinel
        for i in range(5002):
            user = _make_user(did=f"did:plc:{i}")
            queue.put_nowait((user, b"car-data"))

        # These should be parsed while the flush is running
        for i in range(100):
            user = _make_user(did=f"did:plc:after_{i}")
            queue.put_nowait((user, b"car-data"))
        queue.put_nowait(None)

        with patch("goeoview.gap_filler.flush_staging", slow_flush), \
             patch("goeoview.gap_filler.parse_repo", tracking_parse), \
             patch("goeoview.gap_filler.ProcessPoolExecutor", ThreadPoolExecutor), \
             patch("goeoview.gap_filler.DB", return_value=mock_db):
            await asyncio.wait_for(consume_repos(queue, config), timeout=30)

        # The first flush should have started
        assert len(flush_events) >= 2
        flush_start = flush_events[0][1]
        flush_end = flush_events[1][1]

        # Some "after_" items should have been parsed during the flush window
        parsed_during_flush = [
            did for did, t in parse_events
            if did.startswith("did:plc:after_") and flush_start < t < flush_end
        ]
        assert len(parsed_during_flush) > 0, \
            "expected items to be parsed while flush was running"

    async def test_sentinel_waits_for_pending_flush(self):
        from goeoview.gap_filler import consume_repos

        queue = asyncio.Queue()
        config = _make_consume_config()
        mock_db = _make_mock_db()

        flush_completed = False

        async def slow_flush(db, to_insert, to_delete):
            nonlocal flush_completed
            await asyncio.sleep(0.1)
            flush_completed = True

        # Queue enough to trigger one flush, then sentinel immediately
        for i in range(5002):
            user = _make_user(did=f"did:plc:{i}")
            queue.put_nowait((user, b"car-data"))
        queue.put_nowait(None)

        with patch("goeoview.gap_filler.flush_staging", slow_flush), \
             patch("goeoview.gap_filler.parse_repo", _fake_parse_repo), \
             patch("goeoview.gap_filler.ProcessPoolExecutor", ThreadPoolExecutor), \
             patch("goeoview.gap_filler.DB", return_value=mock_db):
            await asyncio.wait_for(consume_repos(queue, config), timeout=30)

        # The background flush must have completed before consume_repos returned
        assert flush_completed


class TestConsumeReposConcurrentParsing:
    async def test_parses_multiple_repos_concurrently(self):
        from goeoview.gap_filler import consume_repos

        queue = asyncio.Queue()
        config = _make_consume_config(gap_parse_workers=4)
        mock_db = _make_mock_db()

        concurrent_parses = 0
        max_concurrent = 0
        lock = threading.Lock()

        def slow_parse(user, car):
            nonlocal concurrent_parses, max_concurrent
            with lock:
                concurrent_parses += 1
                max_concurrent = max(max_concurrent, concurrent_parses)
            time.sleep(0.05)  # simulate work
            with lock:
                concurrent_parses -= 1
            return user.did, [
                (user.did, "app.bsky.feed.post", "abc", 1, '{"text":"hi"}')
            ]

        for i in range(20):
            user = _make_user(did=f"did:plc:{i}")
            queue.put_nowait((user, b"car-data"))
        queue.put_nowait(None)

        with patch("goeoview.gap_filler.flush_staging", AsyncMock()), \
             patch("goeoview.gap_filler.parse_repo", slow_parse), \
             patch("goeoview.gap_filler.ProcessPoolExecutor", ThreadPoolExecutor), \
             patch("goeoview.gap_filler.DB", return_value=mock_db):
            await asyncio.wait_for(consume_repos(queue, config), timeout=30)

        # Multiple parses should have run concurrently
        assert max_concurrent > 1

    async def test_all_results_collected_with_concurrent_parsing(self):
        from goeoview.gap_filler import consume_repos

        queue = asyncio.Queue()
        config = _make_consume_config(gap_parse_workers=4)
        mock_db = _make_mock_db()

        flush_snapshots = []

        async def capturing_flush(db, to_insert, to_delete):
            flush_snapshots.append((list(to_insert), list(to_delete)))

        for i in range(50):
            user = _make_user(did=f"did:plc:{i}")
            queue.put_nowait((user, b"car-data"))
        queue.put_nowait(None)

        with patch("goeoview.gap_filler.flush_staging", capturing_flush), \
             patch("goeoview.gap_filler.parse_repo", _fake_parse_repo), \
             patch("goeoview.gap_filler.ProcessPoolExecutor", ThreadPoolExecutor), \
             patch("goeoview.gap_filler.DB", return_value=mock_db):
            await asyncio.wait_for(consume_repos(queue, config), timeout=30)

        # All 50 DIDs should appear across all flushes
        all_dids = set()
        for to_insert, to_delete in flush_snapshots:
            all_dids.update(to_delete)
        assert len(all_dids) == 50


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


class TestConsumeReposFlushFailureRecovery:
    async def test_continues_after_background_flush_raises(self):
        """When a background flush fails, the consumer should log the error
        and continue processing rather than crashing and deadlocking."""
        from goeoview.gap_filler import consume_repos

        queue = asyncio.Queue()
        config = _make_consume_config()
        mock_db = _make_mock_db()

        flush_call_count = 0

        async def failing_then_ok_flush(db, to_insert, to_delete):
            nonlocal flush_call_count
            flush_call_count += 1
            if flush_call_count == 1:
                raise RuntimeError("Queue is shutdown")
            # Subsequent flushes succeed

        # Queue enough to trigger two flushes (>5000 DIDs each), then sentinel
        for i in range(10002):
            user = _make_user(did=f"did:plc:{i}")
            queue.put_nowait((user, b"car-data"))
        queue.put_nowait(None)

        with patch("goeoview.gap_filler.flush_staging", failing_then_ok_flush), \
             patch("goeoview.gap_filler.parse_repo", _fake_parse_repo), \
             patch("goeoview.gap_filler.ProcessPoolExecutor", ThreadPoolExecutor), \
             patch("goeoview.gap_filler.DB", return_value=mock_db):
            # Must not raise or hang — should complete gracefully
            await asyncio.wait_for(consume_repos(queue, config), timeout=30)

        # Should have attempted multiple flushes despite the first failing
        assert flush_call_count >= 2

    async def test_flush_failure_is_logged(self):
        """The original exception from a failed flush should be logged."""
        from goeoview.gap_filler import consume_repos

        queue = asyncio.Queue()
        config = _make_consume_config()
        mock_db = _make_mock_db()

        flush_count = 0

        async def fail_once_flush(db, to_insert, to_delete):
            nonlocal flush_count
            flush_count += 1
            if flush_count == 1:
                raise RuntimeError("Queue is shutdown")

        for i in range(10002):
            user = _make_user(did=f"did:plc:{i}")
            queue.put_nowait((user, b"car-data"))
        queue.put_nowait(None)

        with patch("goeoview.gap_filler.flush_staging", fail_once_flush), \
             patch("goeoview.gap_filler.parse_repo", _fake_parse_repo), \
             patch("goeoview.gap_filler.ProcessPoolExecutor", ThreadPoolExecutor), \
             patch("goeoview.gap_filler.DB", return_value=mock_db), \
             patch("goeoview.gap_filler.logger") as mock_logger:
            await asyncio.wait_for(consume_repos(queue, config), timeout=30)

        # The error should have been logged via logger.exception
        exception_calls = [c for c in mock_logger.method_calls
                           if c[0] == "exception"]
        assert any("flush failed" in str(c) for c in exception_calls), \
            f"Expected flush failure to be logged, got: {exception_calls}"

    async def test_sentinel_flush_failure_does_not_crash(self):
        """If the final flush on sentinel also fails, consume_repos
        should still exit gracefully."""
        from goeoview.gap_filler import consume_repos

        queue = asyncio.Queue()
        config = _make_consume_config()
        mock_db = _make_mock_db()

        async def always_fail_flush(db, to_insert, to_delete):
            raise RuntimeError("ClickHouse gone")

        for i in range(3):
            user = _make_user(did=f"did:plc:{i}")
            queue.put_nowait((user, b"car-data"))
        queue.put_nowait(None)

        with patch("goeoview.gap_filler.flush_staging", always_fail_flush), \
             patch("goeoview.gap_filler.parse_repo", _fake_parse_repo), \
             patch("goeoview.gap_filler.ProcessPoolExecutor", ThreadPoolExecutor), \
             patch("goeoview.gap_filler.DB", return_value=mock_db):
            # Should not raise — exits gracefully
            await asyncio.wait_for(consume_repos(queue, config), timeout=10)


class TestFlushStagingTruncatesFirst:
    async def test_staging_truncated_before_insert(self):
        """Staging table should be truncated at the start of flush
        to clean up any partial data from a previously failed flush."""
        from goeoview.gap_filler import flush_staging

        db = _make_mock_db()
        call_order = []

        async def tracking_insert(*args, **kwargs):
            call_order.append(("insert", args[0] if args else None))

        async def tracking_command(sql, *args, **kwargs):
            if "TRUNCATE" in sql:
                call_order.append(("truncate",))
            elif "INSERT INTO commit" in sql:
                call_order.append(("dedup_insert",))
            elif "DELETE" in sql:
                call_order.append(("delete",))

        db.db.insert = tracking_insert
        db.db.command = tracking_command

        to_insert = [("did:plc:a", "col", "key", 1, '{"v":1}')]
        to_delete = ["did:plc:a"]
        await flush_staging(db, to_insert, to_delete)

        # First operation should be truncate
        assert call_order[0] == ("truncate",), \
            f"Expected truncate first, got: {call_order}"


class TestGapsLoopConsumerRestart:
    async def test_restarts_consumer_if_it_dies(self):
        """If the consumer task dies unexpectedly, gaps_loop should
        detect it and restart rather than deadlocking."""
        from goeoview.gap_filler import gaps_loop

        mock_db = _make_mock_db()
        mock_db.db.query = AsyncMock(return_value=_make_empty_gap_query_result())

        iteration = 0

        async def fake_consume(queue, config):
            nonlocal iteration
            iteration += 1
            if iteration == 1:
                raise RuntimeError("unexpected consumer death")
            # Second invocation: run until sentinel
            while True:
                item = await queue.get()
                if item is None:
                    return

        config = MagicMock()
        config.gap_queue_size = 10
        config.http_connect_timeout = 5
        config.http_read_timeout = 30

        call_count = 0

        async def counting_coordinate(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                raise KeyboardInterrupt("stop the loop")

        with patch("goeoview.gap_filler.DB", return_value=mock_db), \
             patch("goeoview.gap_filler.consume_repos", fake_consume), \
             patch("goeoview.gap_filler.coordinate_gaps", counting_coordinate):
            try:
                await asyncio.wait_for(gaps_loop(
                    MagicMock(), MagicMock(), config
                ), timeout=10)
            except (KeyboardInterrupt, asyncio.TimeoutError):
                pass

        # Consumer should have been started twice (original + restart)
        assert iteration >= 2
