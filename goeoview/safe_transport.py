from __future__ import annotations

import asyncio
import ipaddress
import socket
from typing import List
from urllib.parse import urlparse

import aiohttp
import aiohttp.abc
import aiohttp.resolver
import httpx


def validate_url_host(url: str) -> None:
    """Resolve the hostname from a URL and reject if any resolved IP is non-public."""
    parsed = urlparse(url)
    hostname = parsed.hostname
    port = parsed.port

    if not hostname:
        raise ValueError(f"no hostname in URL: {url}")

    try:
        results = socket.getaddrinfo(hostname, port)
    except socket.gaierror as exc:
        raise ValueError(f"DNS resolution failed for {hostname}: {exc}") from exc

    if not results:
        raise ValueError(f"{hostname} resolved to no addresses")

    for family, _type, _proto, _canonname, sockaddr in results:
        addr = ipaddress.ip_address(sockaddr[0])
        if not addr.is_global:
            raise ValueError(
                f"non-public IP {addr} for host {hostname}"
            )


class SafeResolver(aiohttp.abc.AbstractResolver):
    """DNS resolver that rejects non-public IPs (SSRF protection).

    Wraps aiohttp's ThreadedResolver and validates every resolved address.
    Used with TCPConnector's built-in DNS cache, so validation cost is
    amortized across requests to the same host.
    """

    def __init__(self) -> None:
        self._inner = aiohttp.resolver.ThreadedResolver()

    async def resolve(
        self,
        host: str,
        port: int = 0,
        family: socket.AddressFamily = socket.AF_INET,
    ) -> List[aiohttp.abc.ResolveResult]:
        results = await self._inner.resolve(host, port, family)
        for result in results:
            addr = ipaddress.ip_address(result["host"])
            if not addr.is_global:
                raise ValueError(
                    f"non-public IP {addr} for host {host}"
                )
        return results

    async def close(self) -> None:
        await self._inner.close()


async def async_validate_url_host(url: str, timeout: float = 3.0) -> None:
    try:
        await asyncio.wait_for(
            asyncio.to_thread(validate_url_host, url),
            timeout=timeout,
        )
    except asyncio.TimeoutError as exc:
        hostname = urlparse(url).hostname or "<unknown>"
        raise ValueError(
            f"DNS validation timed out for {hostname} after {timeout}s"
        ) from exc


class SafeTransport(httpx.AsyncHTTPTransport):
    """AsyncHTTPTransport that validates all destination IPs are public."""

    def __init__(self, *args, validation_timeout: float = 3.0, **kwargs):
        super().__init__(*args, **kwargs)
        self._validation_timeout = validation_timeout

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        await async_validate_url_host(
            str(request.url), timeout=self._validation_timeout
        )
        return await super().handle_async_request(request)


async def safe_get(
    client: httpx.AsyncClient,
    url: str,
    *,
    max_redirects: int = 5,
    max_response_bytes: int = 1_048_576,
    validation_timeout: float = 3.0,
) -> httpx.Response:
    """Follow redirects manually, validating each hop against SSRF.

    Uses streaming reads to enforce max_response_bytes before buffering.
    """
    for _ in range(max_redirects + 1):
        await async_validate_url_host(url, timeout=validation_timeout)
        async with client.stream("GET", url) as response:
            if response.has_redirect_location:
                location = response.headers["location"]
                url = str(httpx.URL(url).join(location))
                continue

            chunks = []
            total = 0
            async for chunk in response.aiter_bytes():
                total += len(chunk)
                if total > max_response_bytes:
                    raise ValueError(
                        f"response body exceeds limit ({max_response_bytes} bytes)"
                    )
                chunks.append(chunk)

            # Build a response-like object with .content, .status_code, .headers, .json()
            return _BufferedResponse(
                status_code=response.status_code,
                headers=response.headers,
                content=b"".join(chunks),
            )

    raise ValueError(f"too many redirects (>{max_redirects})")


class _BufferedResponse:
    """Minimal response wrapper for safe_get results.

    Not a real httpx.Response — only supports status_code, headers,
    content, text, and json(). Don't pass this to code expecting the
    full httpx response interface.
    """

    def __init__(self, status_code, headers, content):
        self.status_code = status_code
        self.headers = headers
        self.content = content

    @property
    def text(self):
        return self.content.decode()

    def json(self):
        import orjson
        return orjson.loads(self.content)
