from __future__ import annotations

import ipaddress
import socket
from typing import List

import aiohttp
import aiohttp.abc
import aiohttp.resolver
import orjson
from yarl import URL


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


class SafeGetResponse:
    """Response from safe_get with .status, .headers, .body, .text, .json()."""

    __slots__ = ["status", "headers", "body"]

    def __init__(self, status, headers, body):
        self.status = status
        self.headers = headers
        self.body = body

    @property
    def text(self):
        return self.body.decode()

    def json(self):
        return orjson.loads(self.body)


async def safe_get(
    session: aiohttp.ClientSession,
    url: str,
    *,
    max_redirects: int = 5,
    max_response_bytes: int = 1_048_576,
) -> SafeGetResponse:
    """Follow redirects manually with SSRF validation via the session's connector.

    The session must use a TCPConnector with SafeResolver so every connection
    (including redirect targets) is validated at DNS resolution time.

    Enforces max_response_bytes via streaming reads.
    """
    for _ in range(max_redirects + 1):
        response = await session.get(url, allow_redirects=False)
        if response.status in (301, 302, 303, 307, 308):
            location = response.headers.get("location")
            await response.release()
            if location is None:
                raise ValueError("redirect with no location header")
            url = str(URL(url).join(URL(location)))
            continue

        try:
            chunks = []
            total = 0
            async for chunk in response.content.iter_any():
                total += len(chunk)
                if total > max_response_bytes:
                    raise ValueError(
                        f"response body exceeds limit ({max_response_bytes} bytes)"
                    )
                chunks.append(chunk)

            return SafeGetResponse(
                status=response.status,
                headers=response.headers,
                body=b"".join(chunks),
            )
        finally:
            await response.release()

    raise ValueError(f"too many redirects (>{max_redirects})")
