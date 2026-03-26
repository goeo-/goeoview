import pytest


@pytest.mark.asyncio
async def test_plc_store_stats_include_cursor_and_count(tmp_path):
    from goeoview.plc_store import PLCStateStore

    store = PLCStateStore(str(tmp_path / "plc_state.db"))
    await store.set_cursor(123)
    stats = await store.get_stats(include_count=True)

    assert stats["path"].endswith("plc_state.db")
    assert stats["cursor"] == 123
    assert stats["latest_count"] == 0


@pytest.mark.asyncio
async def test_plc_store_stats_preserve_timestamp_cursor(tmp_path):
    from goeoview.plc_store import PLCStateStore

    store = PLCStateStore(str(tmp_path / "plc_state.db"))
    await store.set_cursor("2025-04-03T17:34:59.442Z")
    stats = await store.get_stats()

    assert stats["cursor"] == "2025-04-03T17:34:59.442Z"
    assert stats["latest_count"] is None
