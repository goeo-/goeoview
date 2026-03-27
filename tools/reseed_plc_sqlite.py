"""Reseed plc_latest SQLite table from ClickHouse plc_op into a fresh db.

Streams all plc_op rows in natural order and uses PLCStateStore.upsert_many
to keep the latest operation per DID (same logic as the live ingester).
"""

import asyncio
import json
import urllib.request
from datetime import datetime, UTC
from time import perf_counter

from goeoview.plc_store import PLCStateStore

CLICKHOUSE_URL = "http://127.0.0.1:18123/"
SQLITE_PATH = "data/plc_state.db"
BATCH_SIZE = 50_000

# Stream all rows in table order (created_at, did, cid).
# upsert_many's ON CONFLICT keeps the latest per DID.
QUERY = """\
SELECT
    created_at, did, cid, rotation_keys, handle,
    pds, labeler, chat, feedgen, atproto_key, labeler_key, signed_by
FROM goeoview.plc_op
ORDER BY created_at, did, cid
FORMAT JSONEachRow
"""

OLD_CURSOR = "88076789"


def parse_row(row):
    """Convert a ClickHouse JSON row to the tuple format upsert_many expects."""
    return (
        datetime.fromisoformat(row["created_at"]),
        row["did"],
        row["cid"],
        row["rotation_keys"],  # already a list
        row["handle"],
        row["pds"],
        row["labeler"],
        row["chat"],
        row["feedgen"],
        row["atproto_key"],
        row["labeler_key"],
        row["signed_by"],
    )


async def main():
    store = PLCStateStore(SQLITE_PATH)
    store._conn.execute("PRAGMA synchronous=OFF")
    await store.set_cursor(OLD_CURSOR)
    print(f"Fresh db, cursor set to {OLD_CURSOR}", flush=True)

    started = perf_counter()
    req = urllib.request.Request(CLICKHOUSE_URL, data=QUERY.encode())
    resp = urllib.request.urlopen(req)

    batch = []
    total = 0

    for line in resp:
        batch.append(parse_row(json.loads(line)))

        if len(batch) >= BATCH_SIZE:
            await store.upsert_many(batch)
            total += len(batch)
            elapsed = perf_counter() - started
            print(f"  {total:,} rows ({total / elapsed:,.0f}/s)", flush=True)
            batch = []

    if batch:
        await store.upsert_many(batch)
        total += len(batch)

    elapsed = perf_counter() - started
    print(f"Done: {total:,} rows in {elapsed:.1f}s ({total / elapsed:,.0f}/s)", flush=True)

    stats = await store.get_stats(include_count=True)
    print(f"Verify: {stats}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
