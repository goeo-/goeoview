import asyncio
import sys

from at_relay.db import DB
from at_relay.plc import poll
from at_relay.relay import start_firehose, gaps_loop, discover_gaps


async def main():
    db = DB("127.0.0.1", "at_relay")
    await db.start()

    tasks = []

    assert len(sys.argv) > 1

    if "poll" in sys.argv:
        tasks.append(asyncio.create_task(poll()))
    if "firehose" in sys.argv:
        tasks.append(asyncio.create_task(start_firehose("bsky.network")))
    if "gaps" in sys.argv:
        tasks.append(asyncio.create_task(gaps_loop()))
    if "discover" in sys.argv:
        tasks.append(asyncio.create_task(discover_gaps("bsky.network")))

    a, b = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

    print([await x for x in a])
    print([await x for x in b])

if __name__ == "__main__":
    asyncio.run(main())
