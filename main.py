import asyncio
import argparse
import sys

from goeoview.config import Config, ConfigError
from goeoview.db import DB
from goeoview.did import DIDResolver
from goeoview.http_client import HTTPClientManager
from goeoview.logger import get_logger
from goeoview.plc import poll
from goeoview.plc_store import PLCStateStore
from goeoview.relay import start_firehose
from goeoview.gap_filler import gaps_loop

logger = get_logger("goeoview.main")

def parse_args(config) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "commands",
        nargs="+",
        choices=["poll", "firehose", "gaps", "plc-status"],
    )
    parser.add_argument("--relay", default=config.relay_host)
    parser.add_argument("--db-host", default=config.clickhouse_host)
    parser.add_argument("--db-name", default=config.clickhouse_db)
    parser.add_argument("--db-port", type=int, default=config.clickhouse_port)
    return parser.parse_args()


async def main() -> int:
    try:
        config = Config()
    except ConfigError as exc:
        print(exc, file=sys.stderr)
        return 2

    args = parse_args(config)

    plc_state_store = PLCStateStore(config.plc_state_db_path)

    if args.commands == ["plc-status"]:
        stats = await plc_state_store.get_stats(include_count=False)
        print(f"PLC state DB: {stats['path']}")
        print(f"PLC cursor: {stats['cursor']}")
        if stats["latest_count"] is not None:
            print(f"PLC latest rows: {stats['latest_count']}")
        print(f"PLC latest created_at: {stats['latest_created_at']}")
        return 0

    http_clients = HTTPClientManager(config.http_connect_timeout, config.http_read_timeout)
    did_resolver = DIDResolver(http_clients, plc_state_store)

    db = DB(args.db_host, args.db_name, args.db_port)
    await db.start()

    tasks = []
    if "poll" in args.commands:
        tasks.append(asyncio.create_task(poll(config, http_clients, plc_state_store), name="poll"))
    if "firehose" in args.commands:
        tasks.append(asyncio.create_task(start_firehose(args.relay, did_resolver, config), name="firehose"))
    if "gaps" in args.commands:
        tasks.append(asyncio.create_task(gaps_loop(did_resolver, http_clients, config, plc_state_store), name="gaps"))
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        for task in done:
            exc = task.exception()
            if exc is not None:
                logger.error("task %s failed: %s", task.get_name(), exc)
                for pending_task in pending:
                    pending_task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                return 1
        return 0
    finally:
        await http_clients.close()

if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
