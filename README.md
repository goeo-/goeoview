# goeoview

Small AT Protocol ingest/indexing service focused on:

- mirroring PLC operations
- validating firehose commits
- backfilling repo gaps
- storing analytics-friendly data in ClickHouse

Runtime PLC storage is split:

- SQLite `plc_latest` is the online current-state store used for `did:plc` validation
- ClickHouse `plc_op` is the historical PLC archive

## Running

Install runtime dependencies:

```bash
pip install -r requirements.txt
```

Install test dependencies:

```bash
pip install -r requirements-dev.txt
```

Run one or more services:

```bash
python main.py poll
python main.py plc-status
python main.py poll firehose
python main.py gaps
```

## Configuration

Environment variables:

- `CLICKHOUSE_HOST` default `127.0.0.1`
- `CLICKHOUSE_DB` default `goeoview`
- `CLICKHOUSE_PORT` default `8123`
- `RELAY_HOST` default `bsky.network`
- `FIREHOSE_INSERT_EVERY` default `1000`
- `FIREHOSE_PRINT_CLOCK_SKEW_EVERY` default `100`
- `FIREHOSE_WORKERS` default `1`
- `FIREHOSE_BATCH_SIZE` default `128`
- `FIREHOSE_BATCH_TIMEOUT` default `0.5`
- `PLC_POLLER_COUNT` default `1000`
- `PLC_CATCHUP_WORKERS` default `32`
- `PLC_EXPORT_TARGET_INTERVAL` default `0.667`
- `HTTP_CONNECT_TIMEOUT` default `5.0`
- `HTTP_READ_TIMEOUT` default `30.0`
- `MAX_REPO_SIZE` default `134217728` (128MB)
- `GAP_QUEUE_SIZE` default `64`
- `GAP_MAX_PDS_CONCURRENCY` default `10`
- `GAP_PARSE_WORKERS` default CPU count
- `GAP_MAX_TOTAL_WORKERS` default `128`
- `WS_CONNECT_TIMEOUT` default `10.0`
- `BLOCKED_PDS` default `atproto.brid.gy,.stream.place` — comma-separated list of PDS hostnames to skip (suffix matching with leading dot)
- `PLC_STATE_DB_PATH` default `./data/plc_state.db`
- `QUARANTINE_DB_PATH` default `./data/quarantine.db`
