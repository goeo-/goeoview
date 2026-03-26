"""SQLite-backed quarantine store for unprocessable firehose commits."""

import asyncio
import json
import sqlite3
from datetime import datetime, UTC
from threading import Lock


class QuarantineStore:
    def __init__(self, path):
        self.path = path
        self._lock = Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS quarantined (
                repo TEXT NOT NULL,
                seq INTEGER NOT NULL,
                message BLOB NOT NULL,
                reason TEXT NOT NULL,
                stage TEXT NOT NULL,
                error_type TEXT,
                extra TEXT,
                created_at TEXT NOT NULL,
                PRIMARY KEY (repo, seq)
            )
            """
        )
        self._conn.commit()

    def _save_sync(self, repo, seq, message_bytes, reason, stage, error_type, extra):
        extra_json = json.dumps(extra) if extra else None
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO quarantined (repo, seq, message, reason, stage, error_type, extra, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(repo, seq) DO UPDATE SET
                    message=excluded.message,
                    reason=excluded.reason,
                    stage=excluded.stage,
                    error_type=excluded.error_type,
                    extra=excluded.extra,
                    created_at=excluded.created_at
                """,
                (repo, seq, message_bytes, reason, stage, error_type, extra_json,
                 datetime.now(UTC).isoformat()),
            )
            self._conn.commit()

    async def save(self, repo, seq, message_bytes, reason, stage, error_type=None, extra=None):
        await asyncio.to_thread(self._save_sync, repo, seq, message_bytes, reason, stage, error_type, extra)

    def close(self):
        with self._lock:
            self._conn.close()

    def count(self):
        with self._lock:
            row = self._conn.execute("SELECT count(*) FROM quarantined").fetchone()
        return row[0]

    def recent(self, limit=20):
        with self._lock:
            rows = self._conn.execute(
                "SELECT repo, seq, reason, stage, error_type, extra, created_at "
                "FROM quarantined ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [
            {
                "repo": r[0], "seq": r[1], "reason": r[2], "stage": r[3],
                "error_type": r[4],
                "extra": json.loads(r[5]) if r[5] else None,
                "created_at": r[6],
            }
            for r in rows
        ]
