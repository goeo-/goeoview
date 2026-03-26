import asyncio
import fcntl
import hashlib
from pathlib import Path
from time import perf_counter
from collections import defaultdict, deque
from datetime import UTC, datetime, timedelta
from multiprocessing import Pool

import cbrrr
import httpx
import orjson
from httpx_ws import aconnect_ws
from httpx_ws._exceptions import WebSocketNetworkError

from .db import DB
from .helpers import InvalidSignature, b32e, parse_cid, parse_pubkey, ub64d
from .logger import get_logger

logger = get_logger("goeoview.plc")


STREAM_RETRYABLE_REASONS = ("OutdatedCursor", "FutureCursor", "ConsumerTooSlow")
STREAM_RETRY_INTERVAL = 300


def should_fallback_to_export(exc: Exception) -> bool:
    text = str(exc)
    return any(reason in text for reason in STREAM_RETRYABLE_REASONS)


def extract_rotation_keys(operation):
    if "rotationKeys" in operation:
        return operation["rotationKeys"]
    if "recoveryKey" in operation:
        return [operation["recoveryKey"], operation["signingKey"]]
    return []


def extract_handle(operation):
    if "alsoKnownAs" in operation:
        for aka in operation["alsoKnownAs"]:
            if aka.startswith("at://"):
                return aka[5:]
    elif "handle" in operation:
        return operation["handle"]
    return None


def extract_services(operation):
    services = operation.get("services", {})
    return (
        services.get("atproto_pds", {}).get("endpoint") or operation.get("service"),
        services.get("atproto_labeler", {}).get("endpoint"),
        services.get("bsky_chat", {}).get("endpoint"),
        services.get("bsky_fg", {}).get("endpoint"),
    )


def extract_verification_keys(operation):
    verification_methods = operation.get("verificationMethods", {})
    return (
        verification_methods.get("atproto") or operation.get("signingKey"),
        verification_methods.get("atproto_label"),
    )


def parse_plc_op(record, prev_ops):
    did = record["did"]
    operation = record["operation"]
    cid = record["cid"]
    created_at = datetime.fromisoformat(record["createdAt"])

    if any(x[1] == cid for x in prev_ops):
        return None

    cid_h = parse_cid(cid)
    cbor = cbrrr.encode_dag_cbor(operation)
    cbor_hash = hashlib.sha256(cbor).digest()
    if cid_h != cbor_hash:
        raise Exception(f"PLC CID hash mismatch: did={did} cid={cid}")

    to_verify_sig = ub64d(operation.pop("sig"))
    to_verify = cbrrr.encode_dag_cbor(operation)

    valid_keys = None
    if operation["prev"] is None:
        if not any(x in operation for x in ("rotationKeys", "recoveryKey")):
            raise Exception(f"PLC genesis op missing rotation keys: did={did} cid={cid}")

        if len(prev_ops) > 1 or (len(prev_ops) == 1 and prev_ops[0][1] != cid):
            raise Exception(f"PLC prev ops exist but prev is null: did={did} cid={cid} prev_ops={len(prev_ops)}")

        real_did = "did:plc:" + b32e(cid_h[:15])
        if did != real_did:
            raise Exception(f"PLC genesis DID mismatch: did={did} expected={real_did} cid={cid}")
        valid_keys = (
            operation["rotationKeys"]
            if "rotationKeys" in operation
            else [operation["recoveryKey"], operation["signingKey"]]
        )
    else:
        prev_cid = operation["prev"]
        undone_rows = []
        for row in prev_ops:
            r_created_at, r_cid, r_rotation_keys, r_signed_by = row
            if r_cid == prev_cid:
                if len(undone_rows) > 0:
                    time_delta = created_at - undone_rows[-1][0].replace(tzinfo=UTC)
                    if time_delta > timedelta(hours=72):
                        raise Exception(
                            f"PLC undo exceeded 72 hour window: did={did} "
                            f"time_delta={time_delta} created_at={created_at} "
                            f"undone_created_at={undone_rows[-1][0]} "
                            f"undone={len(undone_rows)} cid={cid} prev_cid={prev_cid}"
                        )
                    valid_keys = r_rotation_keys[: undone_rows[-1][3]]
                else:
                    valid_keys = r_rotation_keys
            undone_rows.append(row)

    if valid_keys is None:
        raise Exception(f"PLC op references unknown prev for {did}")
    if not valid_keys:
        raise Exception(f"PLC identity {did} has no valid rotation keys")

    for i, key in enumerate(valid_keys):
        if not key.startswith("did:key:"):
            raise Exception(f"invalid PLC rotation key type: did={did} key={key}")
        parsed_key = parse_pubkey(key[8:])
        try:
            parsed_key.verify(signature=to_verify_sig, data=to_verify)
        except InvalidSignature:
            continue

        atproto_key, labeler_key = extract_verification_keys(operation)
        rotation_keys = extract_rotation_keys(operation)
        handle = extract_handle(operation)
        pds, labeler, chat, feedgen = extract_services(operation)

        return (
            created_at,
            did,
            cid,
            rotation_keys,
            handle,
            pds,
            labeler,
            chat,
            feedgen,
            atproto_key,
            labeler_key,
            i,
        )

    raise Exception(f"couldn't verify PLC op: did={did} cid={cid} keys={len(valid_keys)}")


async def fetch_prev_records(db: DB, records):
    prev_records = defaultdict(deque)
    if not records:
        return prev_records

    res = await db.db.query(
        "SELECT did, created_at, cid, rotation_keys, signed_by FROM plc_op WHERE did IN {dids:Array(String)} ORDER BY created_at DESC",
        {"dids": [x["did"] for x in records]},
    )

    for rec in res.result_rows:
        prev_records[rec[0]].append(rec[1:])

    return prev_records


def validate_records(records, prev_records, pool):
    lines = deque(records)
    rows = []

    while lines:
        dids = set()
        batch = []

        while lines:
            rec_did = lines[0]["did"]
            if rec_did in dids:
                break
            dids.add(rec_did)
            batch.append(lines.popleft())

        output = pool.starmap(
            parse_plc_op,
            [(rec, prev_records[rec["did"]]) for rec in batch],
        )

        for row in output:
            if row is not None:
                rows.append(row)
                prev_records[row[1]].appendleft((row[0], row[2], row[3], row[11]))

    return rows


async def persist_plc_rows(db, rows, plc_state_store, cursor=None):
    if rows:
        await db.db.insert("plc_op", rows)
        await plc_state_store.upsert_many(rows)
    if cursor is not None:
        await plc_state_store.set_cursor(cursor)


async def backfill_export(db, pool, cursor, config, http_clients, plc_state_store):
    while True:
        cycle_started = perf_counter()
        request_started = cycle_started
        res = await http_clients.plc().get(
            "https://plc.directory/export",
            params={"count": config.plc_poller_count, "after": cursor},
        )
        http_elapsed = perf_counter() - request_started
        sleep_time = max(config.plc_export_target_interval - http_elapsed, 0.0)

        if res.status_code == 429:
            logger.warning(
                "PLC export rate limited; sleeping %.3fs",
                sleep_time,
            )
            await asyncio.sleep(max(sleep_time, 0.0))
            continue
        if res.status_code != 200:
            raise Exception(f"PLC export failed with {res.status_code}")

        records = [orjson.loads(x) for x in res.content.splitlines() if x.strip()]
        if not records:
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            return cursor

        prev_started = perf_counter()
        prev_records = await fetch_prev_records(db, records)
        prev_elapsed = perf_counter() - prev_started

        validate_started = perf_counter()
        rows = validate_records(records, prev_records, pool)
        validate_elapsed = perf_counter() - validate_started

        cursor = records[-1].get("seq", records[-1]["createdAt"])

        persist_started = perf_counter()
        await persist_plc_rows(db, rows, plc_state_store, cursor)
        persist_elapsed = perf_counter() - persist_started
        cycle_elapsed = perf_counter() - cycle_started
        effective_rate = len(records) / cycle_elapsed if cycle_elapsed > 0 else 0.0

        logger.info(
            (
                "PLC export ingest: records=%s rows=%s cursor=%s mode=%s "
                "timing=http=%.3fs prev=%.3fs validate=%.3fs persist=%.3fs total=%.3fs "
                "sleep=%.3fs rate=%.1f/s"
            ),
            len(records),
            len(rows),
            cursor,
            "seq" if isinstance(cursor, int) else "timestamp",
            http_elapsed,
            prev_elapsed,
            validate_elapsed,
            persist_elapsed,
            cycle_elapsed,
            sleep_time,
            effective_rate,
        )

        if sleep_time > 0:
            await asyncio.sleep(sleep_time)

        if len(records) < config.plc_poller_count:
            return cursor


STREAM_LOG_BATCH_SIZE = 1000
STREAM_LOG_INTERVAL = 5.0


async def stream_export(db, pool, cursor, config, plc_state_store):
    ws_client = httpx.AsyncClient(timeout=httpx.Timeout(config.ws_connect_timeout))
    try:
        async with aconnect_ws(f"wss://plc.directory/export/stream?cursor={cursor}", client=ws_client) as ws:
            logger.info("PLC stream connected at cursor=%s", cursor)
            batch_records = 0
            batch_rows = 0
            batch_started = perf_counter()
            batch_prev = 0.0
            batch_validate = 0.0
            batch_persist = 0.0

            def flush_batch():
                nonlocal batch_records, batch_rows, batch_started
                nonlocal batch_prev, batch_validate, batch_persist
                if batch_records == 0:
                    return
                batch_elapsed = perf_counter() - batch_started
                rate = batch_records / batch_elapsed if batch_elapsed > 0 else 0.0
                logger.info(
                    (
                        "PLC stream ingest: records=%s rows=%s cursor=%s "
                        "timing=prev=%.3fs validate=%.3fs persist=%.3fs total=%.3fs "
                        "rate=%.1f/s"
                    ),
                    batch_records,
                    batch_rows,
                    cursor,
                    batch_prev,
                    batch_validate,
                    batch_persist,
                    batch_elapsed,
                    rate,
                )
                batch_records = 0
                batch_rows = 0
                batch_started = perf_counter()
                batch_prev = 0.0
                batch_validate = 0.0
                batch_persist = 0.0

            try:
                while True:
                    message = await ws.receive_text()
                    record = orjson.loads(message)

                    prev_started = perf_counter()
                    prev_records = await fetch_prev_records(db, [record])
                    batch_prev += perf_counter() - prev_started

                    validate_started = perf_counter()
                    rows = validate_records([record], prev_records, pool)
                    batch_validate += perf_counter() - validate_started

                    cursor = record["seq"]

                    persist_started = perf_counter()
                    await persist_plc_rows(db, rows, plc_state_store, cursor)
                    batch_persist += perf_counter() - persist_started

                    batch_records += 1
                    batch_rows += len(rows)

                    if (
                        batch_records >= STREAM_LOG_BATCH_SIZE
                        or perf_counter() - batch_started >= STREAM_LOG_INTERVAL
                    ):
                        flush_batch()
            finally:
                flush_batch()
    finally:
        await ws_client.aclose()


async def poll(config, http_clients, plc_state_store):
    lock_path = Path(config.plc_state_db_path).parent / "plc_poll.lock"
    lock_file = open(lock_path, "w")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        raise Exception(f"PLC poller already running (lock: {lock_path})")

    db = DB()
    cursor = await plc_state_store.get_cursor()
    pool_size = config.plc_catchup_workers if cursor == 0 else 2
    seq_stream_attempts = 0

    with Pool(pool_size) as pool:
        while True:
            try:
                cursor = await backfill_export(db, pool, cursor, config, http_clients, plc_state_store)
                if isinstance(cursor, int):
                    seq_stream_attempts += 1
                    should_try_stream = (
                        seq_stream_attempts == 1
                        or seq_stream_attempts % STREAM_RETRY_INTERVAL == 0
                    )
                    if not should_try_stream:
                        logger.info(
                            "PLC export seq catch-up continues at cursor=%s",
                            cursor,
                        )
                        await asyncio.sleep(1)
                        continue

                    logger.info(
                        "PLC export seq catch-up reached cursor=%s; attempting stream",
                        cursor,
                    )
                    try:
                        cursor = await stream_export(db, pool, cursor, config, plc_state_store)
                        seq_stream_attempts = 0
                    except Exception as exc:
                        if should_fallback_to_export(exc):
                            logger.warning(
                                "PLC stream rejected cursor=%s (%s); continuing with export",
                                cursor,
                                exc,
                            )
                            await asyncio.sleep(1)
                            continue
                        raise
                else:
                    logger.info(
                        "PLC export polling continues with timestamp cursor=%s",
                        cursor,
                    )
                    seq_stream_attempts = 0
                    await asyncio.sleep(1)
            except WebSocketNetworkError as exc:
                logger.warning("PLC stream disconnected: %s", exc)
                cursor = await plc_state_store.get_cursor()
                seq_stream_attempts = 0
                await asyncio.sleep(1)
            except Exception as exc:
                logger.warning("PLC ingest error: %s", exc)
                cursor = await plc_state_store.get_cursor()
                await asyncio.sleep(1)
