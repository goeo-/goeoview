import httpx
import cbrrr
import orjson

import asyncio
import hashlib
from datetime import datetime, timedelta, UTC
from time import perf_counter
from multiprocessing import Pool
from collections import defaultdict, deque

from .db import DB
from .helpers import b32e, ub64d, parse_pubkey, parse_cid, InvalidSignature

client = httpx.AsyncClient()

WEBHOOK = 'https://discord.com/api/webhooks/1220404233980084254/' \
          '7Q-ka14WnAqkZyMgLKLIi9832B66JALeTA5lDTOGQgtZEe1IUNxesi7Vxk3zzdPBi6fo?wait=true'

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
    assert cid_h == cbor_hash

    to_verify_sig = ub64d(operation.pop("sig"))
    to_verify = cbrrr.encode_dag_cbor(operation)

    valid_keys = None
    if operation["prev"] is None:
        assert any(x in operation for x in ("rotationKeys", "recoveryKey"))

        if len(prev_ops) > 1 or (len(prev_ops) == 1 and prev_ops[0][1] != cid):
            print(len(prev_ops), prev_ops[0][1] != cid, prev_ops[0][1], cid)
            raise Exception("prev ops exist for did but prev none")

        real_did = "did:plc:" + b32e(cid_h[:15])
        assert did == real_did
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
                    time_delta = created_at - r_created_at.replace(tzinfo=UTC)
                    assert time_delta <= timedelta(hours=72)
                    httpx.post(
                        WEBHOOK,
                        data={
                            "username": "plc undo",
                            "avatar_url": "https://avatars.githubusercontent.com/u/133827929",
                            "content": f"something is up with did {did}, it was undone after {time_delta}"
                        }
                    )

                    valid_keys = r_rotation_keys[:undone_rows[-1][3]]
                else:
                    valid_keys = r_rotation_keys

            undone_rows.append(row)
    for i, key in enumerate(valid_keys):
        assert key.startswith("did:key:")
        key = key[8:]
        key = parse_pubkey(key)
        try:
            key.verify(signature=to_verify_sig, data=to_verify)
        except InvalidSignature:
            continue

        verification_methods = operation.get("verificationMethods", {})
        atproto_key = verification_methods.get("atproto")
        labeler_key = verification_methods.get("atproto_label")

        atproto_key = atproto_key or operation.get("signingKey")

        rotation_keys = []
        if "rotationKeys" in operation:
            rotation_keys = operation["rotationKeys"]
        elif "recoveryKey" in operation:
            rotation_keys = [operation["recoveryKey"], operation["signingKey"]]

        handle = None
        if "alsoKnownAs" in operation:
            for aka in operation["alsoKnownAs"]:
                if aka.startswith("at://"):
                    handle = aka[5:]
                    break
        elif "handle" in operation:
            handle = operation["handle"]

        services = operation.get("services", {})
        pds = services.get("atproto_pds", {}).get("endpoint") or operation.get(
            "service"
        )
        labeler = services.get("atproto_labeler", {}).get("endpoint")
        chat = services.get("bsky_chat", {}).get("endpoint")
        feedgen = services.get("bsky_fg", {}).get("endpoint")

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

    raise Exception("couldn't verify op", record, valid_keys, prev_ops)


async def poll():
    db = DB()
    ret = await db.db.query(
        "SELECT created_at FROM plc_op ORDER BY created_at DESC LIMIT 1;"
    )

    after = ret.first_row[0] if ret.row_count > 0 else datetime.fromisoformat("1970-01-01T00:00:00.000Z")
    last_time = perf_counter()

    none_count = 0
    pool_size = 32 if datetime.now(UTC) - after.replace(tzinfo=UTC) > timedelta(hours=24) else 2
    with Pool(pool_size) as p:
        while True:
            if isinstance(after, datetime):
                after = after - timedelta(milliseconds=1)
                after = after.isoformat().split("+")[0] + "Z"

            try:
                res = await client.get(
                    "https://plc.directory/export",
                    params={"count": 1000, "after": after},
                )
            except Exception as e:
                if type(e).__module__ == "httpx":
                    print(e)
                    await asyncio.sleep(1)
                    continue
            assert res.status_code == 200

            body = res.content
            assert len(body) != 0

            lines = deque([orjson.loads(x) for x in body.split(b"\n")])
            assert len(lines) > 0

            prev_records = defaultdict(deque)

            res = await db.db.query(
                "SELECT did, created_at, cid, rotation_keys, signed_by FROM plc_op WHERE did IN {dids:Array(String)} ORDER BY created_at DESC",
                {"dids": [x["did"] for x in lines]},
            )

            for rec in res.result_rows:
                prev_records[rec[0]].append(rec[1:])

            to_insert = []

            while lines:
                dids = set()
                records = []

                while lines:
                    rec_did = lines[0]["did"]
                    if rec_did in dids:
                        break
                    dids.add(rec_did)
                    records.append(lines.popleft())

                print("processing", len(records))

                output = p.starmap(
                    parse_plc_op,
                    [(rec, prev_records[rec["did"]]) for rec in records],
                )

                for x in output:
                    if x is not None:
                        to_insert.append(x)
                        prev_records[x[1]].appendleft((x[0], x[2], x[3], x[11]))

            if len(to_insert) > 0:
                print("inserting", len(to_insert))

                await db.db.insert("plc_op", to_insert)

                after = to_insert[-1][0]
                none_count = 0
            else:
                none_count += 1

            new_time = perf_counter()
            delta = new_time - last_time
            wait_total = min(0.667 * 1.1**none_count, 3)
            sleep_time = max(wait_total - delta, 0)
            print(
                "after",
                after,
                "delta",
                delta,
                "sleep_time",
                sleep_time,
                "wait_total",
                delta + sleep_time,
            )
            await asyncio.sleep(sleep_time)
            last_time = new_time
