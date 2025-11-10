import cbrrr
import orjson

import httpx
from httpx_ws import aconnect_ws
from httpx_ws._exceptions import WebSocketNetworkError

from datetime import datetime, UTC, timedelta
from collections import deque
from concurrent.futures import ProcessPoolExecutor
import asyncio
import os
import ssl
import json
import random

from . import did
from . import carfile
from . import mst
from .db import DB
from .helpers import sb32d, parse_pubkey, InvalidSignature

PRINT_CLOCK_SKEW_EVERY = 100
INSERT_EVERY = 1000
INTENDED_SKEW = timedelta(seconds=0)

packet_types = [
    ({"t": "#commit", "op": 1}, "commit"),
    ({"t": "#identity", "op": 1}, "identity"),
    # {'did': 'did:plc:sccklc6hjkz5dsz7zkwpefm3', 'seq': 790281030, 'time': '2024-07-15T18:20:46.949Z', 'handle': 'ginmgr.bsky.social'}
    ({"t": "#account", "op": 1}, "account"),
    # {'did': 'did:plc:sccklc6hjkz5dsz7zkwpefm3', 'seq': 790281043, 'time': '2024-07-15T18:20:46.950Z', 'active': True}
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
    gaps = []

    while memory:
        did, collection, key, rev, value, since = memory.popleft()

        if did not in revs:
            # we have no commits for this repo
            gaps.append((did, 0, since))
        elif since is None:
            # why did this repo reset? do we care?
            print((did, collection, key, rev, value))
        elif revs[did] < since:
            # since is newer than our last commit, we have a gap
            gaps.append((did, revs[did], since))
        
        revs[did] = rev
        commits.append((did, collection, key, rev, value))

    await db.db.insert("gap", gaps)
    await db.db.insert("commit", commits)
    await db.db.insert("cursor", [(firehose_url, last_seq)])

    memory.clear()
    print("inserted", last_seq)

def parse_firehose_message_type(message):
    for pld in packet_types:
        if message.startswith(pld):
            return packet_types[pld], message[len(pld) :]
    else:
        raise Exception("unknown message type")
    
async def handle_commit(memory, last_seq, message, recvtime):
    decoded = cbrrr.decode_dag_cbor(message)
    if decoded["tooBig"]:
        return last_seq

    sendtime = datetime.fromisoformat(decoded["time"])
    delta = recvtime - sendtime
    sleep_time = (INTENDED_SKEW - delta).total_seconds()
    if sleep_time > 0:
        await asyncio.sleep(sleep_time)
    
    if last_seq % PRINT_CLOCK_SKEW_EVERY == 0:
        print(f"clock skew {delta}" + (f" sleeping {sleep_time}" if sleep_time > 0 else ""))

    blocks = decoded.pop("blocks")

    assert decoded["seq"] > last_seq
    last_seq = decoded["seq"]

    hdr, pld = carfile.parse(blocks)

    assert len(hdr["roots"]) == 1
    assert hdr["roots"][0] == decoded["commit"]

    root = pld[hdr["roots"][0]]

    assert root["rev"] == decoded["rev"]
    assert root["did"] == decoded["repo"]

    to_verify = root.copy()
    to_verify_sig = to_verify.pop("sig")
    to_verify = cbrrr.encode_dag_cbor(to_verify)

    retries = 0
    while True:
        user = await did.resolve(decoded["repo"], retries=retries)
        if user.atproto_key is None or retries > 5:
            print("no key for", user.did)
            os.makedirs("./unverified", exist_ok=True)
            with open(f"./unverified/{decoded['repo']}-{decoded['seq']}", "wb") as file:
                file.write(message)
            break
        else:
            try:
                parse_pubkey(user.atproto_key).verify(to_verify_sig, to_verify)
            except InvalidSignature:
                print("invalid signature", user.did)
                retries += 1
            else:
                if retries > 0:
                    print("verified", user.did, "after retries", retries)
                    for i in range(retries):
                        did.resolve.cache_invalidate(user.did, retries=i)

                break

    if user.pds is not None and user.pds.startswith("https://atproto.brid.gy"):
        return last_seq

    for op in decoded["ops"]:
        op_cid = op.get("cid")
        res = mst.mst_dfs(pld, set(), root["data"], op["path"], op_cid)
        if not res[0]:
            raise Exception("could not verify signature", hdr, pld, decoded)

        collection, rkey = op["path"].split("/")

        next = (
            root["did"],
            collection,
            rkey,
            int.from_bytes(sb32d(decoded["rev"])),
            orjson.dumps(
                cbrrr.decode_dag_cbor(
                    cbrrr.encode_dag_cbor(pld.get(op_cid)), atjson_mode=True
                )
            ),
            (
                int.from_bytes(sb32d(decoded["since"]))
                if decoded["since"] is not None
                else None
            ),
        )
        memory.append(next)

    return last_seq

async def firehose(firehose_url):
    db = DB()
    memory = deque()

    res = await db.db.query("SELECT val FROM cursor WHERE url = {url:String} ORDER BY val DESC LIMIT 1", {"url": firehose_url})

    if res.row_count == 0:
        fh_cursor = 1
    else:
        fh_cursor = res.first_row[0]
    
    print(f"starting {firehose_url} with cursor {fh_cursor}")
    last_seq = fh_cursor

    async with aconnect_ws(
        f"wss://{firehose_url}/xrpc/com.atproto.sync.subscribeRepos?cursor={fh_cursor}"
    ) as ws:
        while True:
            message = await ws.receive_bytes()
            recvtime = datetime.now(UTC)

            message_type, message = parse_firehose_message_type(message)

            if message_type == "error":
                raise Exception("error:", cbrrr.decode_dag_cbor(message))

            if message_type != "commit":
                continue

            last_seq = await handle_commit(memory, last_seq, message, recvtime)

            if len(memory) >= INSERT_EVERY:
                await insert_commits(db, memory, firehose_url, last_seq)
                assert len(memory) == 0

async def start_firehose(firehose_url):
    while True:
        try:
            await firehose(firehose_url)

        except WebSocketNetworkError:
            print("retrying")

def parse_repo(user, car, db_recs):
    try:
        hdr, pld = carfile.parse(car)
    except ValueError:
        print("could not parse", user.did, car)
        return

    assert len(hdr["roots"]) == 1
    root = pld[hdr["roots"][0]]

    if 'rev' not in root:
        print('no rev', user.did)
    new_rev = int.from_bytes(sb32d(root["rev"]))

    assert root["did"] == user.did

    to_verify = root.copy()
    to_verify_sig = to_verify.pop("sig")
    to_verify = cbrrr.encode_dag_cbor(to_verify)

    try:
        parse_pubkey(user.atproto_key).verify(to_verify_sig, to_verify)
    except InvalidSignature:
        print("invalid signature", user)
        raise
    print("verified", user.handle)

    to_insert = []
    print("parsing", user.handle)
    records, _, _ = mst.mst_dfs_dump(pld, set(), root["data"])
    print("parsed", user.handle)
    for record in records:
        rpath = record[0].decode()
        collection, rkey = rpath.split("/")

        if record[1] not in pld:
            print('could not find', user, collection, rkey, record[1], root["rev"])

        good_cids = cbrrr.decode_dag_cbor(
            cbrrr.encode_dag_cbor(pld[record[1]]),
            atjson_mode=True
        )
        try:
            repo_val = orjson.dumps(good_cids).decode()
        except TypeError:
            repo_val = json.dumps(good_cids)

        try:
            db_val = db_recs.pop(rpath)
            if db_val != repo_val:
                print("diff", db_val, repo_val)
                to_insert.append((user.did, collection, rkey, new_rev, repo_val))

        except KeyError:
            to_insert.append((user.did, collection, rkey, new_rev, repo_val))
    
    for deleted in db_recs:
        to_insert.append((user.did, *deleted.split("/"), new_rev, "null"))
    
    return user.did, to_insert

client = httpx.AsyncClient()
class RepoDownloadException(Exception):
    pass


class Memory:
    __slots__ = ["memory", "mem_sz"]
    def __init__(self):
        self.memory = []
        self.mem_sz = 0

    def push(self, user, car, commits):
        self.memory.append((user, car, commits))
        self.mem_sz += len(car)
    
    def pop(self):
        user, car, commits = self.memory.pop()
        self.mem_sz -= len(car)
        return user, car, commits

async def download_repo(db, repo):
    resolve_did = asyncio.create_task(did.resolve(repo))
    get_did_commits = asyncio.create_task(db.db.query(
        "SELECT collection, key, argMax(value, rev) "
        "FROM commit WHERE did = {did:String} group by (collection, key)",
        {"did": repo}
    ))
    user, commits = await asyncio.gather(resolve_did, get_did_commits)

    if user.pds is None or not user.pds.startswith("https://"):
        raise RepoDownloadException(user)
    
    try:
        ret = await client.get(f"{user.pds}/xrpc/com.atproto.sync.getRepo?did={user.did}")
    except (
        httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadError, httpx.ReadTimeout,
        httpx.InvalidURL, httpx.UnsupportedProtocol, httpx.RemoteProtocolError, ssl.SSLError, ValueError
    ):
        raise RepoDownloadException(user)

    if ret.status_code != 200:
        if ret.status_code in (400, 401, 403):
            await db.db.insert("bad_did", [(user.did, ret.status_code)])
            # await db.db.command("DELETE FROM gap WHERE did = %(did)s", {"did": user.did})
        raise RepoDownloadException(user, ret)
    
    return user, ret.content, {f"{x[0]}/{x[1]}": x[2] for x in commits.result_rows}


async def fill_memory_with_repos(memory: Memory):
    db = DB()
    ret = await db.db.query("SELECT DISTINCT did FROM gap LEFT JOIN bad_did ON gap.did = bad_did.did WHERE bad_did.did = ''")
    try:
        dids = list(ret.result_columns[0])
    except IndexError:
        print("no gaps")
        return

    random.shuffle(dids)
    tasks_cnt = min(128, len(dids))
    dl_tasks = [asyncio.create_task(download_repo(db, dids.pop())) for _ in range(tasks_cnt)]
    
    while dl_tasks:
        # print(dids)
        completed, pending = await asyncio.wait(
            dl_tasks,
            return_when=asyncio.FIRST_COMPLETED
        )

        for repo in completed:
            while memory.mem_sz > 1024 * 1024 * 512:
                print("already have 512mb of repos, waiting")
                await asyncio.sleep(1)

            try:
                pending.add(asyncio.create_task(download_repo(db, dids.pop()))) 
            except IndexError:
                print("no more dids, downloads done")
            
            try:
                user, car, commits = await repo

            except RepoDownloadException as e:
                print("failed to download", *e.args)
                continue

            print("pushing new repo", user.handle)

            memory.push(user, car, commits)

        dl_tasks = pending

async def parse_and_insert(memory: Memory):
    loop = asyncio.get_running_loop()
    db = DB()

    pool_size = 16
    if len(memory.memory) < pool_size:
        await asyncio.sleep(5)
        pool_size = min(16, len(memory.memory))
    
    if pool_size == 0:
        print("no repos to parse")
        return

    with ProcessPoolExecutor(pool_size) as p:
        tasks = {loop.run_in_executor(p, parse_repo, *memory.pop()) for _ in range(pool_size)}
        to_insert = []
        to_delete = []
        last_insert = datetime.now()
        last_new_repo = datetime.now()

        while tasks:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

            for task in done:
                try:
                    repo, commits = await task
                    print('got:', repo, commits)
                    to_insert.extend(commits)
                    to_delete.append(repo)
                except Exception as e:
                    print("failed to parse", e)

                while True:
                    now = datetime.now()
                    to_delete_len = len(to_delete)
                    to_insert_len = len(to_insert)

                    print(to_delete_len, to_insert_len, (now - last_insert).total_seconds())
                    if to_delete_len > 1000 or to_insert_len > 100000 or ((to_insert or to_delete) and (now - last_insert).total_seconds() > 10):
                        print("inserting", to_insert)
                        await db.db.insert("commit", to_insert)
                        print("deleting", to_delete)
                        await db.db.command("DELETE FROM gap WHERE did in %(dids)s", {"dids": to_delete})
                        to_insert.clear()
                        to_delete.clear()
                        last_insert = now

                    try:
                        pending.add(loop.run_in_executor(p, parse_repo, *memory.pop()))
                    except IndexError:
                        print("no more repos, ", end='')
                        if (now - last_new_repo).total_seconds() > 5:
                            print("done")
                            break
                        print('waiting')
                        await asyncio.sleep(1)
                    else:
                        last_new_repo = now
                        break
                
            tasks = pending

        if to_insert or to_delete:
            print("inserting", to_insert)
            await db.db.insert("commit", to_insert)
            print("deleting", to_delete)
            await db.db.command("DELETE FROM gap WHERE did in %(dids)s", {"dids": to_delete})

async def download_gaps():
    memory = Memory()
    tasks = [
        asyncio.create_task(fill_memory_with_repos(memory)),
        asyncio.create_task(parse_and_insert(memory))
    ]
    a, b = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

    print([await x for x in a])
    print([await x for x in b])

async def gaps_loop():
    while True:
        await download_gaps()
        await asyncio.sleep(1)     

# discover gaps
# call com.atproto.sync.listRepos on relay (/pdses) to get all repos like
# {
#     "did": "did:plc:f5ejbog3ostso47er2y5ugbl",
#     "head": "bafyreig5addsngr6mh4huzu3tp2kkngevrpem253d44763rsgvzzo5pnca",
#     "rev": "3kxirl2rbe726",
#     "active": True
# }
# compare rev to db,
# if rev is newer, add gap dbrev -> rev
# if not in db, add gap 0 -> rev (assuming we're listening for new commits)

async def discover_gaps(url):
    print("doing gaps for", url)
    db = DB()
    cursor = None

    while True:
        params = {"limit": 100_000}
        if cursor is not None:
            params["cursor"] = cursor
        print(params)
        res = await client.get(f"https://{url}/xrpc/com.atproto.sync.listRepos", params=params)
        res = res.json()

        if 'repos' not in res or len(res['repos']) == 0:
            print('done?', res, cursor)
            break

        if 'cursor' not in res:
            print('no cursor?', res, 'last:', cursor)
            break

        cursor = res["cursor"]

        dids = [repo["did"] for repo in res["repos"]]
        print(f'got {len(dids)} dids, {"did:web:yamarten.github.io" in dids}')
        '''
        del res

        unknown_dids = set()
        for idx in range(0, len(dids), 2000):
            ret = await db.db.query(
                "select distinct(did) from commit where did in {dids:Array(String)}",
                {"dids": dids[idx : idx + 2000]}
            )

            dids_from_relay = set(dids[idx : idx + 2000])
            dids_in_db = set(ret.result_columns[0])

            unknown_dids = unknown_dids.union(dids_from_relay - dids_in_db)
        
        print(f'inserting {len(unknown_dids)} gaps')
        await db.db.insert('gap2', [(did, 0, 0) for did in unknown_dids])
        '''
