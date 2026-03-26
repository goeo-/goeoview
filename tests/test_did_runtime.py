from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from goeoview.did import DIDResolver, AtprotoUser
from goeoview.plc_store import PLCStateStore
from goeoview.http_client import HTTPClientManager


@pytest.mark.asyncio
async def test_resolve_plc_uses_plc_state_store_first(tmp_path):
    store = PLCStateStore(str(tmp_path / "plc_state.db"))
    store._get_latest_sync = MagicMock(return_value={
        "did": "did:plc:test123",
        "pds": "https://pds.example.com",
        "handle": "alice.test",
        "labeler": None,
        "chat": None,
        "feedgen": None,
        "atproto_key": "did:key:zTest123",
        "labeler_key": None,
    })

    http = MagicMock(spec=HTTPClientManager)
    resolver = DIDResolver(http, store)

    user = await resolver.resolve("did:plc:test123")

    assert user.did == "did:plc:test123"
    assert user.handle == "alice.test"
    assert user.pds == "https://pds.example.com"


@pytest.mark.asyncio
async def test_resolve_plc_falls_back_to_live_directory_after_db_retries(tmp_path):
    store = PLCStateStore(str(tmp_path / "plc_state.db"))
    # store has no data — get_latest returns None

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "id": "did:plc:test123",
        "alsoKnownAs": ["at://alice.test"],
        "service": [
            {
                "id": "#atproto_pds",
                "type": "AtprotoPersonalDataServer",
                "serviceEndpoint": "https://pds.example.com",
            }
        ],
        "verificationMethod": [
            {
                "id": "did:plc:test123#atproto",
                "type": "Multikey",
                "controller": "did:plc:test123",
                "publicKeyMultibase": "zTest123",
            }
        ],
    }

    mock_client = MagicMock()
    mock_client.get = AsyncMock(return_value=mock_response)

    http = MagicMock(spec=HTTPClientManager)
    http.did.return_value = mock_client

    resolver = DIDResolver(http, store)

    with patch("asyncio.sleep", new=AsyncMock()):
        user = await resolver.resolve("did:plc:test123")

    assert user.did == "did:plc:test123"
    assert user.handle == "alice.test"
    assert user.pds == "https://pds.example.com"
