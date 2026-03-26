import asyncio
import itertools
import ssl
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, UTC
from collections import defaultdict

import httpx

from .db import DB
from .relay import parse_repo, RepoDownloadException
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


async def download_one(did, did_resolver, http_clients, config):
    """Download a single repo. Returns (user, car_bytes, response_headers).

    Raises RepoDownloadException on any failure.
    """
    user = await did_resolver.resolve(did)

    if user.pds is None or not user.pds.startswith("https://"):
        raise RepoDownloadException(user)

    db = DB()
    try:
        async with http_clients.repo().stream(
            "GET", f"{user.pds}/xrpc/com.atproto.sync.getRepo?did={user.did}"
        ) as response:
            headers = dict(response.headers)

            if response.status_code != 200:
                await response.aread()
                if response.status_code in (400, 401, 403):
                    await db.db.insert("bad_did", [(user.did, response.status_code)])
                raise RepoDownloadException(user, response.status_code, headers)

            chunks = []
            total = 0
            async for chunk in response.aiter_bytes():
                total += len(chunk)
                if total > config.max_repo_size:
                    raise RepoDownloadException(user)
                chunks.append(chunk)
            body = b"".join(chunks)
    except RepoDownloadException:
        raise
    except (
        httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadError, httpx.ReadTimeout,
        httpx.InvalidURL, httpx.UnsupportedProtocol, httpx.RemoteProtocolError,
        ssl.SSLError, ValueError
    ):
        raise RepoDownloadException(user)

    return user, body, headers


def adjust_target(headers, current_target, max_target):
    """Adjust download concurrency based on rate-limit response headers."""
    try:
        remaining = int(headers.get("ratelimit-remaining", ""))
        limit = int(headers.get("ratelimit-limit", ""))
    except (ValueError, TypeError):
        return current_target

    if limit <= 0:
        return current_target

    ratio = remaining / limit
    if ratio < 0.1:
        return 1
    if ratio > 0.5:
        return min(current_target + 1, max_target)
    return current_target


async def pds_worker(pds_host, dids, queue, did_resolver, http_clients, config):
    """Download repos for a single PDS, respecting rate limits."""
    target = 1
    in_flight = set()
    dids = iter(dids)

    for did in itertools.islice(dids, target):
        in_flight.add(asyncio.create_task(
            download_one(did, did_resolver, http_clients, config)
        ))

    while in_flight:
        done, in_flight = await asyncio.wait(
            in_flight, return_when=asyncio.FIRST_COMPLETED
        )

        for task in done:
            try:
                user, car, headers = await task
                await queue.put((user, car))
                target = adjust_target(headers, target, config.gap_max_pds_concurrency)
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
                    logger.warning("failed to download from %s: %s", pds_host, e.args)

        # Spawn up to target
        while len(in_flight) < target:
            try:
                did = next(dids)
            except StopIteration:
                break
            in_flight.add(asyncio.create_task(
                download_one(did, did_resolver, http_clients, config)
            ))


async def flush_staging(db, to_insert, to_delete):
    """Flush parsed repos through the staging table with dedup."""
    if not to_insert and not to_delete:
        return

    logger.info("flushing %d rows for %d DIDs", len(to_insert), len(to_delete))

    # 1. Bulk insert into staging
    if to_insert:
        await db.db.insert("staging", to_insert)

    # 2. Dedup insert: changed and new records
    await db.db.command("""
        INSERT INTO commit
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
        INSERT INTO commit
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


async def consume_repos(queue, config):
    """Pull repos from queue, parse in process pool, batch insert."""
    loop = asyncio.get_running_loop()
    db = DB()

    to_insert = []
    to_delete = []
    last_flush = datetime.now(UTC)

    with ProcessPoolExecutor(config.gap_parse_workers) as pool:
        while True:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                now = datetime.now(UTC)
                if (to_insert or to_delete) and (now - last_flush).total_seconds() > 10:
                    await flush_staging(db, to_insert, to_delete)
                    to_insert.clear()
                    to_delete.clear()
                    last_flush = now
                continue

            if item is None:
                if to_insert or to_delete:
                    await flush_staging(db, to_insert, to_delete)
                logger.info("consumer received shutdown sentinel")
                return

            user, car = item

            try:
                did, commits = await loop.run_in_executor(
                    pool, parse_repo, user, car
                )
                logger.info("parsed repo %s: %d records", did, len(commits))
                to_insert.extend(commits)
                to_delete.append(did)
            except Exception as e:
                logger.warning("failed to parse repo: %s", e)

            now = datetime.now(UTC)
            if (
                len(to_delete) > 1000
                or len(to_insert) > 100000
                or ((to_insert or to_delete) and (now - last_flush).total_seconds() > 10)
            ):
                await flush_staging(db, to_insert, to_delete)
                to_insert.clear()
                to_delete.clear()
                last_flush = now


async def coordinate_gaps(queue, did_resolver, http_clients, config,
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
                                    http_clients, config)

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

    try:
        while True:
            await coordinate_gaps(queue, did_resolver, http_clients, config,
                                  plc_state_store)
            await asyncio.sleep(1)
    finally:
        # Graceful shutdown
        await queue.put(None)
        await consumer_task
