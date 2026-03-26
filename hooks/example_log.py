"""Example hook: logs new posts to the console."""

from goeoview.logger import get_logger

logger = get_logger("hooks.example_log")


async def on_post(commits, db):
    for commit in commits:
        logger.info(
            "new post: %s %s/%s",
            commit["did"], commit["collection"], commit["rkey"],
        )


def register(hook):
    hook(
        callback=on_post,
        collections=["app.bsky.feed.post"],
        ops=["create"],
    )
