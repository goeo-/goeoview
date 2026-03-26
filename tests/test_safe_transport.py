import pytest
import socket
from unittest.mock import patch, MagicMock
from goeoview.safe_transport import validate_url_host


def _mock_getaddrinfo(ip):
    """Return a mock getaddrinfo that resolves any host to the given IP."""
    def fake(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, '', (ip, port or 80))]
    return fake


def test_rejects_loopback():
    with patch("goeoview.safe_transport.socket.getaddrinfo", _mock_getaddrinfo("127.0.0.1")):
        with pytest.raises(ValueError, match="non-public"):
            validate_url_host("http://evil.com/foo")


def test_rejects_private_10():
    with patch("goeoview.safe_transport.socket.getaddrinfo", _mock_getaddrinfo("10.0.0.1")):
        with pytest.raises(ValueError, match="non-public"):
            validate_url_host("http://evil.com/foo")


def test_rejects_private_172():
    with patch("goeoview.safe_transport.socket.getaddrinfo", _mock_getaddrinfo("172.16.0.1")):
        with pytest.raises(ValueError, match="non-public"):
            validate_url_host("http://evil.com/foo")


def test_rejects_private_192():
    with patch("goeoview.safe_transport.socket.getaddrinfo", _mock_getaddrinfo("192.168.1.1")):
        with pytest.raises(ValueError, match="non-public"):
            validate_url_host("http://evil.com/foo")


def test_rejects_link_local():
    with patch("goeoview.safe_transport.socket.getaddrinfo", _mock_getaddrinfo("169.254.169.254")):
        with pytest.raises(ValueError, match="non-public"):
            validate_url_host("http://evil.com/metadata")


def test_rejects_ipv6_loopback():
    def fake(host, port, *args, **kwargs):
        return [(socket.AF_INET6, socket.SOCK_STREAM, 0, '', ('::1', port or 80, 0, 0))]
    with patch("goeoview.safe_transport.socket.getaddrinfo", fake):
        with pytest.raises(ValueError, match="non-public"):
            validate_url_host("http://evil.com/foo")


def test_allows_public_ip():
    with patch("goeoview.safe_transport.socket.getaddrinfo", _mock_getaddrinfo("1.1.1.1")):
        validate_url_host("https://cloudflare.com/foo")  # should not raise


def test_rejects_unresolvable():
    def fake(host, port, *args, **kwargs):
        raise socket.gaierror("Name resolution failed")
    with patch("goeoview.safe_transport.socket.getaddrinfo", fake):
        with pytest.raises(ValueError, match="DNS resolution failed"):
            validate_url_host("http://nonexistent.invalid/foo")


def test_rejects_empty_result():
    def fake(host, port, *args, **kwargs):
        return []
    with patch("goeoview.safe_transport.socket.getaddrinfo", fake):
        with pytest.raises(ValueError, match="no addresses"):
            validate_url_host("http://empty.invalid/foo")


@pytest.mark.asyncio
async def test_safe_get_rejects_oversized_response():
    """safe_get must abort before buffering oversized responses."""
    from contextlib import asynccontextmanager
    from goeoview.safe_transport import safe_get

    class FakeResponse:
        status_code = 200
        has_redirect_location = False
        headers = {}
        async def aiter_bytes(self):
            for _ in range(100):
                yield b"x" * 10000  # 1MB total

    @asynccontextmanager
    async def fake_stream(method, url):
        yield FakeResponse()

    mock_client = MagicMock()
    mock_client.stream = fake_stream

    with patch("goeoview.safe_transport.validate_url_host"):
        with pytest.raises(ValueError, match="exceeds limit"):
            await safe_get(mock_client, "https://evil.com/big", max_response_bytes=50000)


def test_rejects_mixed_public_and_private():
    """A host that resolves to both public and private IPs must be rejected."""
    def fake(host, port, *args, **kwargs):
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 0, '', ("1.1.1.1", port or 80)),
            (socket.AF_INET, socket.SOCK_STREAM, 0, '', ("127.0.0.1", port or 80)),
        ]
    with patch("goeoview.safe_transport.socket.getaddrinfo", fake):
        with pytest.raises(ValueError, match="non-public"):
            validate_url_host("http://evil.com/foo")
