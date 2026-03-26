from datetime import UTC, datetime

import pytest


@pytest.mark.asyncio
async def test_plc_store_upserts_and_reads_latest(tmp_path, monkeypatch):
    monkeypatch.setenv("PLC_STATE_DB_PATH", str(tmp_path / "plc_state.db"))

    from goeoview.plc_store import PLCStateStore

    store = PLCStateStore(str(tmp_path / "plc_state.db"))
    rows = [
        (
            datetime(2024, 1, 1, tzinfo=UTC),
            "did:plc:test123",
            "cid1",
            ["did:key:a"],
            "alice.test",
            "https://pds.example.com",
            None,
            None,
            None,
            "zKey1",
            None,
            0,
        )
    ]

    await store.upsert_many(rows)
    user = await store.get_latest("did:plc:test123")
    assert user["did"] == "did:plc:test123"
    assert user["handle"] == "alice.test"
    assert user["atproto_key"] == "zKey1"
