import cbrrr
import orjson

import httpx
from httpx_ws import aconnect_ws
from httpx_ws._exceptions import WebSocketNetworkError

from datetime import datetime, UTC, timedelta
from collections import deque
from concurrent.futures import ProcessPoolExecutor
import asyncio
import json

from . import carfile
from . import mst
from .db import DB
from .helpers import sb32d, parse_pubkey, InvalidSignature
from .logger import get_logger
from .firehose_worker import validate_commit

logger = get_logger("goeoview.relay")

INTENDED_SKEW = timedelta(seconds=0)


class FirehoseProtocolError(Exception):
    pass


class CommitValidationError(FirehoseProtocolError):
    pass

packet_types = [
    ({"t": "#commit", "op": 1}, "commit"),
    ({"t": "#identity", "op": 1}, "identity"),
    # {'did': 'did:plc:sccklc6hjkz5dsz7zkwpefm3', 'seq': 790281030, 'time': '2024-07-15T18:20:46.949Z', 'handle': 'ginmgr.bsky.social'}
    ({"t": "#account", "op": 1}, "account"),
    # {'did': 'did:plc:sccklc6hjkz5dsz7zkwpefm3', 'seq': 790281043, 'time': '2024-07-15T18:20:46.950Z', 'active': True}
    ({"t": "#sync", "op": 1}, "sync"),
    ({"t": "#handle", "op": 1}, "handle"),
    # {'did': 'did:plc:pvlc2irmgup7lbs4neg2kum2', 'seq': 790283367, 'time': '2024-07-15T18:21:21.071Z', 'handle': 'ochachao.bsky.social'}
    ({"t": "#migrate", "op": 1}, "migrate"),
    ({"t": "#tombstone", "op": 1}, "tombstone"),
    ({"t": "#info", "op": 1}, "info"),
    ({"op": -1}, "error"),
]
packet_types = {cbrrr.encode_dag_cbor(x): y for x, y in packet_types}

async def insert_commits(db: DB, memory: deque, firehose_url, last_seq):
    dids = [x[0] for x in memory]
    ret = await db.db.query(
        "select did, max(rev) from commit where did in {dids:Array(String)} group by did",
        {"dids": dids}
    )
    revs = {x[0]: x[1] for x in ret.result_rows}
    del dids

    commits = []
    hook_rows = []
    gaps = []

    while memory:
        did, collection, key, rev, value, since, action = memory.popleft()

        if did not in revs:
            # we have no commits for this repo
            gaps.append((did, 0, since))
        elif since is None:
            # why did this repo reset? do we care?
            logger.warning("repo reset with null since: %s %s/%s", did, collection, key)
        elif revs[did] < since:
            # since is newer than our last commit, we have a gap
            gaps.append((did, revs[did], since))

        revs[did] = rev
        commits.append((did, collection, key, rev, value))
        hook_rows.append({
            "did": did,
            "collection": collection,
            "rkey": key,
            "rev": rev,
            "value": value,
            "since": since,
            "action": action,
        })

    await db.db.insert("gap", gaps)
    await db.db.insert("commit", commits)
    await db.db.insert("cursor", [(firehose_url, last_seq)])

    memory.clear()
    logger.info("inserted %s commits through seq=%s", len(commits), last_seq)
    return hook_rows

def parse_firehose_message_type(message):
    for pld in packet_types:
        if message.startswith(pld):
            return packet_types[pld], message[len(pld) :]
    try:
        parts = list(cbrrr.decode_multi_dag_cbor_in_violation_of_the_spec(message))
    except Exception:
        parts = message[:64]
    raise FirehoseProtocolError(f"unknown message type: {parts}")


def _decode_sync_header(message_bytes):
    decoded = cbrrr.decode_dag_cbor(message_bytes)
    return {"seq": decoded["seq"], "did": decoded["did"], "rev": decoded["rev"], "time": decoded["time"]}


def _decode_commit_header(message_bytes):
    """Lightweight CBOR decode that extracts just seq, repo, time.

    Returns a dict with those keys, or None if the message is tooBig.
    """
    decoded = cbrrr.decode_dag_cbor(message_bytes)
    if decoded["tooBig"]:
        return None
    return {"seq": decoded["seq"], "repo": decoded["repo"], "time": decoded["time"]}

async def _try_historical_key(message, repo, commit_time):
    """Check if PLC has a key rotation after commit_time; if so, try the older key."""
    db = DB()
    ret = await db.db.query(
        "SELECT atproto_key FROM plc_op "
        "WHERE did = {did:String} AND created_at <= parseDateTime64BestEffort({t:String}) "
        "ORDER BY created_at DESC LIMIT 1",
        {"did": repo, "t": commit_time},
    )
    if ret.row_count == 0:
        return None
    historical_key = ret.first_row[0]
    if historical_key is None:
        return None
    # plc_op stores full did:key: prefix, strip it
    if historical_key.startswith("did:key:"):
        historical_key = historical_key[8:]
    return validate_commit(message, historical_key)


async def _handle_commit_serial(message, did_resolver, quarantine):
    """Signature-retry path: resolve DID with increasing retries and validate."""
    decoded = cbrrr.decode_dag_cbor(message)
    repo = decoded["repo"]
    seq = decoded["seq"]
    commit_time = decoded["time"]

    user = await did_resolver.resolve(repo)
    if user.atproto_key is None:
        logger.warning("no key for %s", user.did)
        await quarantine.save(repo, seq, message,
                              reason="no atproto key after retries",
                              stage="serial_retry",
                              error_type="key_missing",
                              extra={"retries": 0})
        return []

    result = validate_commit(message, user.atproto_key)

    if result["ok"]:
        return result["rows"]

    if result.get("fatal"):
        raise CommitValidationError(result["error"])

    # Signature failed — check if PLC is ahead of this commit
    if result.get("retry"):
        historical_result = await _try_historical_key(message, repo, commit_time)
        if historical_result is not None and historical_result["ok"]:
            logger.info("verified %s with historical key (commit_time=%s)", repo, commit_time)
            return historical_result["rows"]
        elif historical_result is not None and historical_result.get("fatal"):
            raise CommitValidationError(historical_result["error"])

    # Fall through to existing retry loop (PLC might be behind)
    retries = 1
    while True:
        user = await did_resolver.resolve(repo, retries=retries)
        if user.atproto_key is None or retries > 5:
            logger.warning("no key for %s", user.did)
            await quarantine.save(repo, seq, message,
                                  reason="no atproto key after retries",
                                  stage="serial_retry",
                                  error_type="key_missing",
                                  extra={"retries": retries})
            return []

        result = validate_commit(message, user.atproto_key)

        if result["ok"]:
            logger.info("verified %s after retries=%s", user.did, retries)
            for i in range(retries):
                did_resolver.resolve.cache_invalidate(user.did, retries=i)
            return result["rows"]

        if result.get("fatal"):
            raise CommitValidationError(result["error"])

        if result.get("retry"):
            retries += 1
            continue

        # non-fatal error (e.g. mst proof failure)
        logger.warning("commit error for %s: %s", repo, result.get("error"))
        await quarantine.save(repo, seq, message,
                              reason=result.get("error", "unknown"),
                              stage="serial_retry",
                              error_type="validation_error",
                              extra={"retries": retries})
        return []

async def _process_batch(batch, memory, did_resolver, config, loop, pool, _validate_commit, quarantine):
    """Process a batch of commits: resolve DIDs, validate in parallel, collect rows.

    batch: list of (message_bytes, seq, repo, recvtime) tuples.
    Results are applied in batch order to preserve per-repo commit ordering.
    """
    # Resolve all DIDs concurrently
    resolve_tasks = [did_resolver.resolve(repo) for _, _, repo, _ in batch]
    users = await asyncio.gather(*resolve_tasks, return_exceptions=True)

    # Classify each batch item: worker-eligible, bridgy-skip, or resolve-failed
    # action[i] = "worker" | "skip" | "resolve_failed"
    action = []
    worker_inputs = []
    worker_batch_indices = []
    for i, (message_bytes, seq, repo, recvtime) in enumerate(batch):
        user = users[i]
        if isinstance(user, Exception):
            logger.warning("DID resolve failed for %s: %s", repo, user)
            action.append("resolve_failed")
            continue
        if user.pds is not None and user.pds.startswith("https://atproto.brid.gy"):
            action.append("skip")
            continue
        action.append("worker")
        worker_inputs.append((message_bytes, user.atproto_key))
        worker_batch_indices.append(i)

    # Dispatch validation for worker-eligible items
    if pool is not None:
        worker_results = await asyncio.gather(*[
            loop.run_in_executor(pool, _validate_commit, msg, key)
            for msg, key in worker_inputs
        ])
    else:
        worker_results = [_validate_commit(msg, key) for msg, key in worker_inputs]

    # Build index from batch position to worker result
    worker_result_map = dict(zip(worker_batch_indices, worker_results))

    # Process all items in batch order to preserve per-repo commit ordering
    for i, (message_bytes, seq, repo, recvtime) in enumerate(batch):
        if action[i] == "skip":
            continue

        if action[i] == "resolve_failed":
            rows = await _handle_commit_serial(message_bytes, did_resolver, quarantine)
            memory.extend(rows)
            continue

        result = worker_result_map[i]
        if result["ok"]:
            memory.extend(result["rows"])
        elif result.get("fatal"):
            raise CommitValidationError(result["error"])
        elif result.get("retry"):
            rows = await _handle_commit_serial(message_bytes, did_resolver, quarantine)
            memory.extend(rows)
        else:
            logger.warning("commit error for %s seq=%s: %s", repo, seq, result.get("error"))
            await quarantine.save(repo, seq, message_bytes,
                                  reason=result.get("error", "unknown"),
                                  stage="worker",
                                  error_type="validation_error")

async def firehose(firehose_url, did_resolver, config, quarantine, pool, registry):
    db = DB()
    memory = deque()

    res = await db.db.query("SELECT val FROM cursor WHERE url = {url:String} ORDER BY val DESC LIMIT 1", {"url": firehose_url})

    if res.row_count == 0:
        fh_cursor = 1
    else:
        fh_cursor = res.first_row[0]

    logger.info("starting %s with cursor %s", firehose_url, fh_cursor)
    last_seen_seq = fh_cursor - 1  # relay re-sends cursor seq on reconnect
    last_safe_seq = fh_cursor  # for cursor persistence — only advances after successful batch

    loop = asyncio.get_running_loop()
    batch = []

    ws_client = httpx.AsyncClient(timeout=httpx.Timeout(config.ws_connect_timeout))
    try:
        async with aconnect_ws(
            f"wss://{firehose_url}/xrpc/com.atproto.sync.subscribeRepos?cursor={fh_cursor}",
            client=ws_client,
        ) as ws:
            while True:
                try:
                    message = await asyncio.wait_for(
                        ws.receive_bytes(),
                        timeout=config.firehose_batch_timeout if batch else None,
                    )
                except asyncio.TimeoutError:
                    if batch:
                        await _process_batch(batch, memory, did_resolver, config, loop, pool, validate_commit, quarantine)
                        last_safe_seq = last_seen_seq
                        batch.clear()
                        if len(memory) >= config.firehose_insert_every:
                            hook_rows = await insert_commits(db, memory, firehose_url, last_safe_seq)
                            registry.dispatch(hook_rows)
                    continue
                recvtime = datetime.now(UTC)

                message_type, message = parse_firehose_message_type(message)

                if message_type == "error":
                    raise FirehoseProtocolError(
                        f"relay returned error {cbrrr.decode_dag_cbor(message)}"
                    )

                if message_type == "sync":
                    sync_header = _decode_sync_header(message)
                    if sync_header["seq"] <= last_seen_seq:
                        raise CommitValidationError(
                            f"non-monotonic seq {sync_header['seq']} after {last_seen_seq}"
                        )
                    last_seen_seq = sync_header["seq"]
                    rev_int = int.from_bytes(sb32d(sync_header["rev"]))
                    await db.db.insert("gap", [(sync_header["did"], 0, rev_int)])
                    logger.info("sync for %s rev=%s seq=%s, inserted gap", sync_header["did"], sync_header["rev"], sync_header["seq"])
                    continue

                if message_type != "commit":
                    continue

                header = _decode_commit_header(message)
                if header is None:
                    continue

                if header["seq"] <= last_seen_seq:
                    raise CommitValidationError(
                        f"non-monotonic seq {header['seq']} after {last_seen_seq}"
                    )
                last_seen_seq = header["seq"]

                sendtime = datetime.fromisoformat(header["time"])
                delta = recvtime - sendtime
                sleep_time = (INTENDED_SKEW - delta).total_seconds()
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

                if last_seen_seq % config.firehose_print_clock_skew_every == 0:
                    logger.info(
                        "clock skew %s%s",
                        delta,
                        f" sleeping {sleep_time}" if sleep_time > 0 else "",
                    )

                batch.append((message, header["seq"], header["repo"], recvtime))

                if len(batch) >= config.firehose_batch_size:
                    await _process_batch(batch, memory, did_resolver, config, loop, pool, validate_commit, quarantine)
                    last_safe_seq = last_seen_seq
                    batch.clear()

                    if len(memory) >= config.firehose_insert_every:
                        hook_rows = await insert_commits(db, memory, firehose_url, last_safe_seq)
                        registry.dispatch(hook_rows)
    finally:
        try:
            if batch:
                await _process_batch(batch, memory, did_resolver, config, loop, pool, validate_commit, quarantine)
                last_safe_seq = last_seen_seq
                batch.clear()
        except Exception:
            pass  # best-effort flush; don't mask the original exception
        if memory:
            hook_rows = await insert_commits(db, memory, firehose_url, last_safe_seq)
            registry.dispatch(hook_rows)
        await ws_client.aclose()

async def start_firehose(firehose_url, did_resolver, config):
    from .quarantine import QuarantineStore
    from .hooks import HookRegistry

    quarantine = QuarantineStore(config.quarantine_db_path)
    registry = HookRegistry()
    registry.load("hooks")
    watcher_task = asyncio.create_task(registry.watch(), name="hook-watcher")
    pool = ProcessPoolExecutor(config.firehose_workers) if config.firehose_workers > 1 else None
    try:
        while True:
            try:
                await firehose(firehose_url, did_resolver, config, quarantine, pool, registry)

            except WebSocketNetworkError:
                logger.warning("websocket error, retrying")
                await asyncio.sleep(1)
    finally:
        watcher_task.cancel()
        quarantine.close()
        if pool is not None:
            pool.shutdown(wait=True, cancel_futures=True)

def parse_repo(user, car):
    # runs in subprocess — use logging directly, not the module logger
    import logging
    log = logging.getLogger("goeoview.parse_repo")

    try:
        hdr, pld = carfile.parse(car)
    except ValueError:
        log.warning("could not parse repo %s", user.did)
        return user.did, []

    if len(hdr["roots"]) != 1:
        raise ValueError(f"repo CAR must have exactly one root, got {len(hdr['roots'])}")
    root = pld[hdr["roots"][0]]

    if 'rev' not in root:
        log.warning("no rev for %s", user.did)
    new_rev = int.from_bytes(sb32d(root["rev"]))

    if root["did"] != user.did:
        raise ValueError(f"repo DID mismatch: expected {user.did}, got {root['did']}")

    to_verify = root.copy()
    to_verify_sig = to_verify.pop("sig")
    to_verify = cbrrr.encode_dag_cbor(to_verify)

    try:
        parse_pubkey(user.atproto_key).verify(to_verify_sig, to_verify)
    except InvalidSignature:
        log.warning("invalid signature for %s", user.did)
        raise

    to_insert = []
    records, _, _ = mst.mst_dfs_dump(pld, set(), root["data"])
    for record in records:
        rpath = record[0].decode()
        collection, rkey = rpath.split("/")

        if record[1] not in pld:
            log.warning("missing block for %s %s/%s", user.did, collection, rkey)

        good_cids = cbrrr.decode_dag_cbor(
            cbrrr.encode_dag_cbor(pld[record[1]]),
            atjson_mode=True
        )
        try:
            repo_val = orjson.dumps(good_cids).decode()
        except TypeError:
            repo_val = json.dumps(good_cids)

        to_insert.append((user.did, collection, rkey, new_rev, repo_val))

    return user.did, to_insert

class RepoDownloadException(Exception):
    def __init__(self, user, status_code=None, headers=None):
        self.user = user
        self.status_code = status_code
        self.headers = headers or {}
        super().__init__(user, status_code)



