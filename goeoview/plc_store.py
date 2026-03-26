from __future__ import annotations

import asyncio
import sqlite3
from threading import Lock


def parse_cursor_value(value: str | None):
    if value is None:
        return 0
    try:
        return int(value)
    except ValueError:
        return value


class PLCStateStore:
    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS plc_latest (
                did TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                cid TEXT NOT NULL,
                rotation_keys TEXT NOT NULL,
                handle TEXT,
                pds TEXT,
                labeler TEXT,
                chat TEXT,
                feedgen TEXT,
                atproto_key TEXT,
                labeler_key TEXT,
                signed_by INTEGER
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS plc_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        self._conn.commit()

    def _upsert_many_sync(self, rows) -> None:
        if not rows:
            return
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO plc_latest (
                    created_at, did, cid, rotation_keys, handle, pds, labeler,
                    chat, feedgen, atproto_key, labeler_key, signed_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(did) DO UPDATE SET
                    created_at=excluded.created_at,
                    cid=excluded.cid,
                    rotation_keys=excluded.rotation_keys,
                    handle=excluded.handle,
                    pds=excluded.pds,
                    labeler=excluded.labeler,
                    chat=excluded.chat,
                    feedgen=excluded.feedgen,
                    atproto_key=excluded.atproto_key,
                    labeler_key=excluded.labeler_key,
                    signed_by=excluded.signed_by
                WHERE excluded.created_at >= plc_latest.created_at
                """,
                [
                    (
                        row[0].isoformat(),
                        row[1],
                        row[2],
                        "\x1f".join(row[3]),
                        row[4],
                        row[5],
                        row[6],
                        row[7],
                        row[8],
                        row[9],
                        row[10],
                        row[11],
                    )
                    for row in rows
                ],
            )
            latest_created_at = max(row[0] for row in rows).isoformat()
            self._conn.execute(
                """
                INSERT INTO plc_meta (key, value) VALUES ('latest_created_at', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (latest_created_at,),
            )
            self._conn.commit()

    async def upsert_many(self, rows) -> None:
        if rows:
            await asyncio.to_thread(self._upsert_many_sync, rows)

    def _get_latest_sync(self, did: str):
        with self._lock:
            row = self._conn.execute(
                """
                SELECT did, pds, handle, labeler, chat, feedgen, atproto_key, labeler_key
                FROM plc_latest
                WHERE did = ?
                """,
                (did,),
            ).fetchone()
        if row is None:
            return None
        return {
            "did": row[0],
            "pds": row[1],
            "handle": row[2],
            "labeler": row[3],
            "chat": row[4],
            "feedgen": row[5],
            "atproto_key": row[6],
            "labeler_key": row[7],
        }

    async def get_latest(self, did: str):
        return await asyncio.to_thread(self._get_latest_sync, did)

    def _get_pds_bulk_sync(self, dids):
        result = {}
        batch_size = 900  # stay under SQLite variable limit
        with self._lock:
            for i in range(0, len(dids), batch_size):
                batch = dids[i:i + batch_size]
                placeholders = ",".join("?" for _ in batch)
                rows = self._conn.execute(
                    f"SELECT did, pds FROM plc_latest WHERE did IN ({placeholders})",
                    batch,
                ).fetchall()
                for row in rows:
                    result[row[0]] = row[1]
        return result

    async def get_pds_bulk(self, dids):
        """Return {did: pds_url} for all DIDs found in the store."""
        return await asyncio.to_thread(self._get_pds_bulk_sync, dids)

    def _get_cursor_sync(self):
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM plc_meta WHERE key = 'cursor'"
            ).fetchone()
        return parse_cursor_value(row[0] if row is not None else None)

    async def get_cursor(self):
        return await asyncio.to_thread(self._get_cursor_sync)

    def _set_cursor_sync(self, cursor) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO plc_meta (key, value) VALUES ('cursor', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (str(cursor),),
            )
            self._conn.commit()

    async def set_cursor(self, cursor) -> None:
        await asyncio.to_thread(self._set_cursor_sync, cursor)

    def _get_stats_sync(self, include_count: bool):
        with self._lock:
            latest_row = self._conn.execute(
                "SELECT value FROM plc_meta WHERE key = 'latest_created_at'"
            ).fetchone()
            cursor_row = self._conn.execute(
                "SELECT value FROM plc_meta WHERE key = 'cursor'"
            ).fetchone()
            count_row = None
            if include_count:
                count_row = self._conn.execute(
                    "SELECT count(*) FROM plc_latest"
                ).fetchone()
        return {
            "path": self.path,
            "cursor": parse_cursor_value(cursor_row[0] if cursor_row is not None else None),
            "latest_count": int(count_row[0]) if count_row is not None else None,
            "latest_created_at": latest_row[0] if latest_row is not None else None,
        }

    async def get_stats(self, include_count: bool = False):
        if not include_count:
            return self._get_stats_sync(False)
        return await asyncio.to_thread(self._get_stats_sync, True)
