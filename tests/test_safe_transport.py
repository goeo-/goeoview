import socket
from unittest.mock import AsyncMock, patch

import aiohttp
import aiohttp.abc
import aiohttp.resolver
import pytest

from goeoview.safe_transport import SafeResolver, safe_get


# --- SafeResolver tests ---

@pytest.fixture
async def resolver():
    return SafeResolver()


@pytest.mark.asyncio
async def test_resolver_rejects_loopback(resolver):
    with patch.object(
        resolver._inner, "resolve",
        return_value=[{"hostname": "evil.com", "host": "127.0.0.1", "port": 80, "family": socket.AF_INET, "proto": 0, "flags": socket.AI_NUMERICHOST}],
    ):
        with pytest.raises(ValueError, match="non-public"):
            await resolver.resolve("evil.com", 80)


@pytest.mark.asyncio
async def test_resolver_rejects_private_10(resolver):
    with patch.object(
        resolver._inner, "resolve",
        return_value=[{"hostname": "evil.com", "host": "10.0.0.1", "port": 80, "family": socket.AF_INET, "proto": 0, "flags": socket.AI_NUMERICHOST}],
    ):
        with pytest.raises(ValueError, match="non-public"):
            await resolver.resolve("evil.com", 80)


@pytest.mark.asyncio
async def test_resolver_rejects_private_172(resolver):
    with patch.object(
        resolver._inner, "resolve",
        return_value=[{"hostname": "evil.com", "host": "172.16.0.1", "port": 80, "family": socket.AF_INET, "proto": 0, "flags": socket.AI_NUMERICHOST}],
    ):
        with pytest.raises(ValueError, match="non-public"):
            await resolver.resolve("evil.com", 80)


@pytest.mark.asyncio
async def test_resolver_rejects_private_192(resolver):
    with patch.object(
        resolver._inner, "resolve",
        return_value=[{"hostname": "evil.com", "host": "192.168.1.1", "port": 80, "family": socket.AF_INET, "proto": 0, "flags": socket.AI_NUMERICHOST}],
    ):
        with pytest.raises(ValueError, match="non-public"):
            await resolver.resolve("evil.com", 80)


@pytest.mark.asyncio
async def test_resolver_rejects_link_local(resolver):
    with patch.object(
        resolver._inner, "resolve",
        return_value=[{"hostname": "evil.com", "host": "169.254.169.254", "port": 80, "family": socket.AF_INET, "proto": 0, "flags": socket.AI_NUMERICHOST}],
    ):
        with pytest.raises(ValueError, match="non-public"):
            await resolver.resolve("evil.com", 80)


@pytest.mark.asyncio
async def test_resolver_rejects_mixed_public_and_private(resolver):
    with patch.object(
        resolver._inner, "resolve",
        return_value=[
            {"hostname": "evil.com", "host": "1.1.1.1", "port": 80, "family": socket.AF_INET, "proto": 0, "flags": socket.AI_NUMERICHOST},
            {"hostname": "evil.com", "host": "127.0.0.1", "port": 80, "family": socket.AF_INET, "proto": 0, "flags": socket.AI_NUMERICHOST},
        ],
    ):
        with pytest.raises(ValueError, match="non-public"):
            await resolver.resolve("evil.com", 80)


@pytest.mark.asyncio
async def test_resolver_allows_public_ip(resolver):
    with patch.object(
        resolver._inner, "resolve",
        return_value=[{"hostname": "ok.com", "host": "1.1.1.1", "port": 80, "family": socket.AF_INET, "proto": 0, "flags": socket.AI_NUMERICHOST}],
    ):
        result = await resolver.resolve("ok.com", 80)
        assert result[0]["host"] == "1.1.1.1"


# --- safe_get tests ---

@pytest.mark.asyncio
async def test_safe_get_returns_response():
    async def fake_iter():
        yield b'{"ok":true}'

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.headers = {"content-type": "application/json"}
    mock_response.content = AsyncMock()
    mock_response.content.iter_any = fake_iter
    mock_response.release = AsyncMock()

    session = AsyncMock()
    session.get = AsyncMock(return_value=mock_response)

    result = await safe_get(session, "https://example.com/data")
    assert result.status == 200
    assert result.body == b'{"ok":true}'
    assert result.json() == {"ok": True}


@pytest.mark.asyncio
async def test_safe_get_follows_redirects():
    redirect_response = AsyncMock()
    redirect_response.status = 302
    redirect_response.headers = {"location": "https://example.com/final"}
    redirect_response.release = AsyncMock()

    async def final_iter():
        yield b"done"

    final_response = AsyncMock()
    final_response.status = 200
    final_response.headers = {}
    final_response.content = AsyncMock()
    final_response.content.iter_any = final_iter
    final_response.release = AsyncMock()

    session = AsyncMock()
    session.get = AsyncMock(side_effect=[redirect_response, final_response])

    result = await safe_get(session, "https://example.com/start")
    assert result.status == 200
    assert result.body == b"done"
    assert session.get.call_count == 2


@pytest.mark.asyncio
async def test_safe_get_rejects_too_many_redirects():
    redirect_response = AsyncMock()
    redirect_response.status = 302
    redirect_response.headers = {"location": "https://example.com/loop"}
    redirect_response.release = AsyncMock()

    session = AsyncMock()
    session.get = AsyncMock(return_value=redirect_response)

    with pytest.raises(ValueError, match="too many redirects"):
        await safe_get(session, "https://example.com/loop", max_redirects=3)

    assert session.get.call_count == 4  # initial + 3 redirects


@pytest.mark.asyncio
async def test_safe_get_rejects_oversized_response():
    async def big_iter():
        for _ in range(100):
            yield b"x" * 10000

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.headers = {}
    mock_response.content = AsyncMock()
    mock_response.content.iter_any = big_iter
    mock_response.release = AsyncMock()

    session = AsyncMock()
    session.get = AsyncMock(return_value=mock_response)

    with pytest.raises(ValueError, match="exceeds limit"):
        await safe_get(session, "https://evil.com/big", max_response_bytes=50000)
