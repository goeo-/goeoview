import asyncio
import itertools
import ssl
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, UTC
from collections import defaultdict

import aiohttp

from .db import DB
from .relay import parse_repo, RepoDownloadException
from .safe_transport import SafeResolver
from .logger import get_logger

logger = get_logger("goeoview.gap_filler")


STAGING_DDL = """\
CREATE TABLE IF NOT EXISTS staging
(
    did String,
    collection LowCardinality(String),
    key String,
    rev UInt64,
    value String
)
ENGINE = Memory
"""


async def download_one(did, did_resolver, session, config, pds_host=None):
    """Download a single repo. Returns (user, car_bytes, response_headers).

    Raises RepoDownloadException on any failure.
    """
    user = await did_resolver.resolve(did)

    repo_pds = pds_host or user.pds

    if repo_pds is None or not repo_pds.startswith("https://"):
        raise RepoDownloadException(user, stage="pds_validation")

    url = f"{repo_pds}/xrpc/com.atproto.sync.getRepo?did={user.did}"

    db = DB()
    try:
        async with session.get(url) as response:
            headers = dict(response.headers)

            if response.status != 200:
                await response.read()
                if response.status in (400, 401, 403):
                    await db.db.insert(
                        "bad_did",
                        [(user.did, response.status)],
                        column_names=["did", "status_code"],
                    )
                raise RepoDownloadException(
                    user,
                    response.status,
                    headers,
                    stage="http_response",
                )

            chunks = []
            total = 0
            async for chunk in response.content.iter_any():
                total += len(chunk)
                if total > config.max_repo_size:
                    raise RepoDownloadException(
                        user,
                        stage="stream_body",
                        cause=ValueError(
                            f"repo exceeded max size {config.max_repo_size} bytes"
                        ),
                    )
                chunks.append(chunk)
            body = b"".join(chunks)
    except RepoDownloadException:
        raise
    except (
        aiohttp.ClientError, asyncio.TimeoutError, ssl.SSLError, OSError,
    ) as e:
        raise RepoDownloadException(user, stage="request", cause=e) from e

    return user, body, headers


def adjust_target(headers, current_target, max_target):
    """Adjust download concurrency based on rate-limit response headers."""
    try:
        remaining = int(headers.get("ratelimit-remaining", ""))
        limit = int(headers.get("ratelimit-limit", ""))
    except (ValueError, TypeError):
        # No rate limit headers — ramp up freely
        return min(current_target + 1, max_target)

    if limit <= 0:
        return current_target

    ratio = remaining / limit
    if ratio < 0.1:
        return 1
    if ratio > 0.5:
        return min(current_target + 1, max_target)
    return current_target


async def pds_worker(pds_host, dids, queue, did_resolver, session, config):
    """Download repos for a single PDS, respecting rate limits."""
    target = 1
    in_flight = set()
    total_assigned = len(dids)
    dids = iter(dids)
    consecutive_5xx = 0
    max_consecutive_5xx = 10
    completed = 0
    failed = 0

    for did in itertools.islice(dids, target):
        in_flight.add(asyncio.create_task(
            download_one(did, did_resolver, session, config, pds_host=pds_host),
            name=f"download_one:{pds_host}:{did}",
        ))

    while in_flight:
        done, in_flight = await asyncio.wait(
            in_flight, return_when=asyncio.FIRST_COMPLETED
        )

        for task in done:
            try:
                user, car, headers = await task
                await queue.put((user, car))
                old_target = target
                target = adjust_target(headers, target, config.gap_max_pds_concurrency)
                completed += 1
                if target != old_target:
                    logger.info(
                        "PDS %s concurrency %d→%d (remaining=%s limit=%s)",
                        pds_host, old_target, target,
                        headers.get("ratelimit-remaining", "?"),
                        headers.get("ratelimit-limit", "?"),
                    )
                elif completed % 100 == 0:
                    logger.info(
                        "PDS %s: %d done, target=%d, in_flight=%d (remaining=%s limit=%s)",
                        pds_host, completed, target, len(in_flight),
                        headers.get("ratelimit-remaining", "?"),
                        headers.get("ratelimit-limit", "?"),
                    )
                consecutive_5xx = 0
            except RepoDownloadException as e:
                if e.status_code == 429:
                    try:
                        reset_at = int(e.headers.get("ratelimit-reset", "0"))
                        wait = max(0, reset_at - int(datetime.now(UTC).timestamp()))
                        logger.info("PDS %s rate limited, sleeping %ds", pds_host, wait)
                        await asyncio.sleep(wait)
                    except (ValueError, TypeError):
                        await asyncio.sleep(30)
                    target = 1
                else:
                    failed += 1
                    logger.warning("failed to download from %s: %s", pds_host, e)
                    if e.status_code and e.status_code >= 500:
                        consecutive_5xx += 1
                        if consecutive_5xx >= max_consecutive_5xx:
                            logger.warning(
                                "PDS %s: %d consecutive 5xx errors, skipping remaining DIDs",
                                pds_host, consecutive_5xx,
                            )
                            for pending in in_flight:
                                pending.cancel()
                            for pending in in_flight:
                                try:
                                    await pending
                                except (asyncio.CancelledError, Exception):
                                    pass
                            return
                        backoff = min(2 ** consecutive_5xx, 60)
                        await asyncio.sleep(backoff)
                        target = 1
            except Exception:
                failed += 1
                logger.exception(
                    "unexpected download task failure for %s task=%s",
                    pds_host,
                    task.get_name(),
                )

        # Spawn up to target
        while len(in_flight) < target:
            try:
                did = next(dids)
            except StopIteration:
                break
            in_flight.add(asyncio.create_task(
                download_one(did, did_resolver, session, config, pds_host=pds_host),
                name=f"download_one:{pds_host}:{did}",
            ))

    logger.info(
        "PDS %s finished: %d/%d downloaded, %d failed",
        pds_host, completed, total_assigned, failed,
    )


async def flush_staging(db, to_insert, to_delete):
    """Flush parsed repos through the staging table with dedup."""
    if not to_insert and not to_delete:
        return

    logger.info("flushing %d rows for %d DIDs", len(to_insert), len(to_delete))

    # 0. Clean up any partial data from a previously failed flush
    await db.db.command("TRUNCATE TABLE staging")

    # 1. Bulk insert into staging
    if to_insert:
        await db.db.insert("staging", to_insert)

    # 2. Dedup insert: changed and new records
    await db.db.command("""
        INSERT INTO commit (did, collection, key, rev, value)
        SELECT s.did, s.collection, s.key, s.rev, s.value
        FROM staging AS s
        LEFT JOIN (
            SELECT did, collection, key, argMax(value, rev) AS value
            FROM commit
            WHERE did IN (SELECT DISTINCT did FROM staging)
            GROUP BY did, collection, key
        ) AS cur
        ON s.did = cur.did AND s.collection = cur.collection AND s.key = cur.key
        WHERE cur.value IS NULL OR cur.value != s.value
    """)

    # 3. Deletion detection: records in commit but not in repo
    await db.db.command("""
        INSERT INTO commit (did, collection, key, rev, value)
        SELECT cur.did, cur.collection, cur.key, head.rev, 'null'
        FROM (
            SELECT did, collection, key, argMax(value, rev) AS value
            FROM commit
            WHERE did IN (SELECT DISTINCT did FROM staging)
            GROUP BY did, collection, key
            HAVING value != 'null'
        ) AS cur
        JOIN (
            SELECT did, max(rev) AS rev FROM staging GROUP BY did
        ) AS head ON cur.did = head.did
        LEFT JOIN staging AS s
        ON cur.did = s.did AND cur.collection = s.collection AND cur.key = s.key
        WHERE s.did IS NULL
    """)

    # 4. Delete processed gaps
    if to_delete:
        await db.db.command(
            "DELETE FROM gap WHERE did IN %(dids)s",
            {"dids": to_delete}
        )

    # 5. Truncate staging
    await db.db.command("TRUNCATE TABLE staging")

    logger.info("flush complete")


async def _wait_flush(flush_task):
    """Await a pending flush task if one exists."""
    if flush_task is not None:
        await flush_task


def _collect_parse_results(done_tasks, to_insert, to_delete):
    """Collect results from completed parse tasks into buffers."""
    for task in done_tasks:
        try:
            did, commits = task.result()
            logger.info("parsed repo %s: %d records", did, len(commits))
            to_insert.extend(commits)
            to_delete.append(did)
        except Exception as e:
            logger.warning("failed to parse repo: %s", e)


async def consume_repos(queue, config):
    """Pull repos from queue, parse in process pool, batch insert."""
    loop = asyncio.get_running_loop()
    db = DB()

    to_insert = []
    to_delete = []
    last_flush = datetime.now(UTC)
    flush_task = None
    in_flight = set()

    def _should_flush():
        now = datetime.now(UTC)
        return (
            len(to_delete) > 5000
            or len(to_insert) > 500000
            or ((to_insert or to_delete) and (now - last_flush).total_seconds() > 10)
        )

    async def _do_flush():
        nonlocal to_insert, to_delete, flush_task, last_flush
        try:
            await _wait_flush(flush_task)
        except Exception:
            # Previous flush failed — its batch is lost but those DIDs
            # remain in the gap table and will retry next cycle.
            logger.exception("background flush failed")
        flush_task = asyncio.create_task(
            flush_staging(db, to_insert, to_delete)
        )
        to_insert = []
        to_delete = []
        last_flush = datetime.now(UTC)

    import time as _time

    _t_loop = _t_harvest = _t_flush = _t_wait = _t_queue = _t_submit = 0.0
    _n_loops = 0

    with ProcessPoolExecutor(config.gap_parse_workers) as pool:
        while True:
            _loop_start = _time.monotonic()
            _n_loops += 1

            # Harvest completed parses without blocking
            _t0 = _time.monotonic()
            done = {t for t in in_flight if t.done()}
            if done:
                in_flight -= done
                _collect_parse_results(done, to_insert, to_delete)
            _t_harvest += _time.monotonic() - _t0

            # Check flush thresholds
            _t0 = _time.monotonic()
            if _should_flush():
                await _do_flush()
            _t_flush += _time.monotonic() - _t0

            # If at parse capacity, wait for one to finish
            if len(in_flight) >= config.gap_parse_workers:
                _t0 = _time.monotonic()
                done, in_flight = await asyncio.wait(
                    in_flight, return_when=asyncio.FIRST_COMPLETED
                )
                _t_wait += _time.monotonic() - _t0
                _collect_parse_results(done, to_insert, to_delete)
                if _n_loops % 100 == 0:
                    logger.info(
                        "consumer timing (%d loops): harvest=%.2fs flush=%.2fs wait=%.2fs queue=%.2fs submit=%.2fs in_flight=%d qsize=%d",
                        _n_loops, _t_harvest, _t_flush, _t_wait, _t_queue, _t_submit,
                        len(in_flight), queue.qsize(),
                    )
                    _t_harvest = _t_flush = _t_wait = _t_queue = _t_submit = 0.0
                continue

            # Pull from queue
            _t0 = _time.monotonic()
            try:
                item = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                _t_queue += _time.monotonic() - _t0
                logger.info(
                    "consumer queue timeout: in_flight=%d qsize=%d harvest=%.2fs flush=%.2fs wait=%.2fs queue=%.2fs",
                    len(in_flight), queue.qsize(), _t_harvest, _t_flush, _t_wait, _t_queue,
                )
                _t_harvest = _t_flush = _t_wait = _t_queue = _t_submit = 0.0
                # Wait for at least one parse if any are in flight
                if in_flight:
                    done, in_flight = await asyncio.wait(
                        in_flight, return_when=asyncio.FIRST_COMPLETED
                    )
                    _collect_parse_results(done, to_insert, to_delete)
                continue
            _t_queue += _time.monotonic() - _t0

            if item is None:
                # Drain remaining parses
                if in_flight:
                    done, _ = await asyncio.wait(in_flight)
                    _collect_parse_results(done, to_insert, to_delete)
                try:
                    await _wait_flush(flush_task)
                except Exception:
                    logger.exception("background flush failed during shutdown")
                if to_insert or to_delete:
                    try:
                        await flush_staging(db, to_insert, to_delete)
                    except Exception:
                        logger.exception("final flush failed during shutdown")
                logger.info("consumer received shutdown sentinel")
                return

            _t0 = _time.monotonic()
            user, car = item
            in_flight.add(
                loop.run_in_executor(pool, parse_repo, user, car)
            )
            _t_submit += _time.monotonic() - _t0


async def coordinate_gaps(queue, did_resolver, session, config,
                          plc_state_store=None):
    """Query gaps, resolve DIDs, group by PDS, spawn per-PDS workers."""
    db = DB()
    ret = await db.db.query(
        "SELECT DISTINCT did FROM gap "
        "LEFT JOIN bad_did ON gap.did = bad_did.did "
        "WHERE bad_did.did = ''"
    )
    try:
        dids = list(ret.result_columns[0])
    except IndexError:
        logger.info("no gaps")
        return

    logger.info("found %d DIDs with gaps", len(dids))

    # Bulk-resolve did:plc: DIDs via PLC state store
    pds_groups = defaultdict(list)
    remaining = []

    if plc_state_store is not None:
        plc_dids = [d for d in dids if d.startswith("did:plc:")]
        other_dids = [d for d in dids if not d.startswith("did:plc:")]

        if plc_dids:
            pds_map = await plc_state_store.get_pds_bulk(plc_dids)
            for did in plc_dids:
                pds = pds_map.get(did)
                if pds and pds.startswith("https://"):
                    pds_groups[pds].append(did)
                else:
                    remaining.append(did)
            logger.info("bulk-resolved %d/%d did:plc: DIDs", len(pds_map), len(plc_dids))

        remaining.extend(other_dids)
    else:
        remaining = dids

    # Fall back to resolver for did:web: and any did:plc: not in the store
    for did in remaining:
        try:
            user = await did_resolver.resolve(did)
            if user.pds and user.pds.startswith("https://"):
                pds_groups[user.pds].append(did)
            else:
                logger.warning("no valid PDS for %s", did)
        except Exception as e:
            logger.warning("failed to resolve %s: %s", did, e)

    logger.info("grouped into %d PDS hosts", len(pds_groups))

    # Process PDS groups with bounded concurrency
    semaphore = asyncio.Semaphore(config.gap_max_total_workers)
    pds_items = list(pds_groups.items())

    async def run_worker(pds_host, pds_dids):
        async with semaphore:
            return await pds_worker(pds_host, pds_dids, queue, did_resolver,
                                    session, config)

    workers = [
        asyncio.create_task(
            run_worker(pds_host, pds_dids),
            name=f"pds_worker:{pds_host}"
        )
        for pds_host, pds_dids in pds_items
    ]

    if workers:
        results = await asyncio.gather(*workers, return_exceptions=True)
        for (pds_host, _), result in zip(pds_items, results):
            if isinstance(result, Exception):
                logger.error("pds_worker %s failed: %s", pds_host, result)


async def gaps_loop(did_resolver, http_clients, config, plc_state_store=None):
    """Main entry point: run gap filling continuously."""
    # Ensure staging table exists (Memory engine loses it on restart)
    db = DB()
    await db.db.command(STAGING_DDL)

    queue = asyncio.Queue(maxsize=config.gap_queue_size)
    consumer_task = asyncio.create_task(
        consume_repos(queue, config), name="gap_consumer"
    )

    connector = aiohttp.TCPConnector(
        limit=1000, limit_per_host=0, resolver=SafeResolver(),
    )
    timeout = aiohttp.ClientTimeout(
        connect=config.http_connect_timeout,
        sock_read=config.http_read_timeout,
    )
    async with aiohttp.ClientSession(
        connector=connector, timeout=timeout
    ) as session:
        try:
            while True:
                if consumer_task.done():
                    # Consumer died unexpectedly — log and restart it
                    exc = consumer_task.exception() if not consumer_task.cancelled() else None
                    logger.error("consumer task died, restarting: %s", exc)
                    # Drain the old queue so stale items don't pile up
                    queue = asyncio.Queue(maxsize=config.gap_queue_size)
                    consumer_task = asyncio.create_task(
                        consume_repos(queue, config), name="gap_consumer"
                    )
                await coordinate_gaps(queue, did_resolver, session, config,
                                      plc_state_store)
                await asyncio.sleep(1)
        finally:
            # Graceful shutdown
            await queue.put(None)
            await consumer_task
