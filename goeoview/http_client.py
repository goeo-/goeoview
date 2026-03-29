from __future__ import annotations

import httpx

from .safe_transport import SafeTransport


class HTTPClientManager:
    def __init__(
        self,
        connect_timeout: float = 5.0,
        read_timeout: float = 30.0,
        dns_validation_timeout: float = 3.0,
    ) -> None:
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._dns_validation_timeout = dns_validation_timeout
        self._clients: dict[str, httpx.AsyncClient] = {}

    def _timeout(self) -> httpx.Timeout:
        return httpx.Timeout(self._read_timeout, connect=self._connect_timeout)

    def _build_trusted(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=self._timeout(),
            follow_redirects=True,
        )

    def _build_untrusted(self) -> httpx.AsyncClient:
        limits = httpx.Limits(
            max_connections=1000,
            max_keepalive_connections=200,
        )
        return httpx.AsyncClient(
            transport=SafeTransport(
                limits=limits,
                validation_timeout=self._dns_validation_timeout,
            ),
            timeout=self._timeout(),
            follow_redirects=False,
        )

    def plc(self) -> httpx.AsyncClient:
        return self._clients.setdefault("plc", self._build_trusted())

    def did(self) -> httpx.AsyncClient:
        return self._clients.setdefault("did", self._build_untrusted())

    def repo(self) -> httpx.AsyncClient:
        return self._clients.setdefault("repo", self._build_untrusted())

    async def close(self) -> None:
        clients = list(self._clients.values())
        self._clients.clear()
        for client in clients:
            await client.aclose()
