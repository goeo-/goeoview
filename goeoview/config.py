from __future__ import annotations

import os
from pathlib import Path


class ConfigError(Exception):
    pass


class Config:
    def __init__(self) -> None:
        self.clickhouse_host = os.getenv("CLICKHOUSE_HOST", "127.0.0.1")
        self.clickhouse_db = os.getenv("CLICKHOUSE_DB", "goeoview")
        self.clickhouse_port = self._parse_int("CLICKHOUSE_PORT", "8123", min_val=1)

        self.relay_host = os.getenv("RELAY_HOST", "bsky.network")
        self.plc_state_db_path = os.getenv("PLC_STATE_DB_PATH", "./data/plc_state.db")
        self.quarantine_db_path = os.getenv("QUARANTINE_DB_PATH", "./data/quarantine.db")

        self.firehose_insert_every = self._parse_int(
            "FIREHOSE_INSERT_EVERY", "1000", min_val=1
        )
        self.firehose_print_clock_skew_every = self._parse_int(
            "FIREHOSE_PRINT_CLOCK_SKEW_EVERY", "100", min_val=1
        )
        self.firehose_workers = self._parse_int(
            "FIREHOSE_WORKERS", "1", min_val=1
        )
        self.firehose_batch_size = self._parse_int(
            "FIREHOSE_BATCH_SIZE", "128", min_val=1
        )
        self.firehose_batch_timeout = self._parse_float(
            "FIREHOSE_BATCH_TIMEOUT", "0.5", min_val=0.05
        )
        self.plc_poller_count = self._parse_int("PLC_POLLER_COUNT", "1000", min_val=1)
        self.plc_catchup_workers = self._parse_int(
            "PLC_CATCHUP_WORKERS", "32", min_val=1
        )
        self.plc_export_target_interval = self._parse_float(
            "PLC_EXPORT_TARGET_INTERVAL", "0.667", min_val=0.1
        )
        self.http_connect_timeout = self._parse_float(
            "HTTP_CONNECT_TIMEOUT", "5.0", min_val=0.1
        )
        self.http_read_timeout = self._parse_float(
            "HTTP_READ_TIMEOUT", "30.0", min_val=0.1
        )
        self.max_repo_size = self._parse_int(
            "MAX_REPO_SIZE", "134217728", min_val=1024  # 128MB
        )
        self.gap_queue_size = self._parse_int(
            "GAP_QUEUE_SIZE", "64", min_val=1
        )
        self.gap_max_pds_concurrency = self._parse_int(
            "GAP_MAX_PDS_CONCURRENCY", "10", min_val=1
        )
        self.gap_parse_workers = self._parse_int(
            "GAP_PARSE_WORKERS", str(os.cpu_count() or 4), min_val=1
        )
        self.gap_max_total_workers = self._parse_int(
            "GAP_MAX_TOTAL_WORKERS", "128", min_val=1
        )
        self.ws_connect_timeout = self._parse_float(
            "WS_CONNECT_TIMEOUT", "10.0", min_val=1.0
        )

        Path(self.plc_state_db_path).parent.mkdir(parents=True, exist_ok=True)
        Path(self.quarantine_db_path).parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _parse_int(name: str, default: str, min_val: int | None = None) -> int:
        raw = os.getenv(name, default)
        try:
            value = int(raw)
        except ValueError as exc:
            raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc
        if min_val is not None and value < min_val:
            raise ConfigError(f"{name} must be >= {min_val}, got {value}")
        return value

    @staticmethod
    def _parse_float(name: str, default: str, min_val: float | None = None) -> float:
        raw = os.getenv(name, default)
        try:
            value = float(raw)
        except ValueError as exc:
            raise ConfigError(f"{name} must be a number, got {raw!r}") from exc
        if min_val is not None and value < min_val:
            raise ConfigError(f"{name} must be >= {min_val}, got {value}")
        return value
