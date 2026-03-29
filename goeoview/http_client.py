from __future__ import annotations

import aiohttp

from .safe_transport import SafeResolver


class HTTPClientManager:
    def __init__(self, config) -> None:
        self._config = config
        self._sessions: dict[str, aiohttp.ClientSession] = {}

    def _timeout(self) -> aiohttp.ClientTimeout:
        return aiohttp.ClientTimeout(
            connect=self._config.http_connect_timeout,
            sock_read=self._config.http_read_timeout,
        )

    def _build_trusted(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(timeout=self._timeout())

    def _build_untrusted(self) -> aiohttp.ClientSession:
        connector = aiohttp.TCPConnector(
            limit=1000,
            limit_per_host=1,
            resolver=SafeResolver(),
        )
        return aiohttp.ClientSession(
            connector=connector,
            timeout=self._timeout(),
        )

    def trusted(self) -> aiohttp.ClientSession:
        if "trusted" not in self._sessions:
            self._sessions["trusted"] = self._build_trusted()
        return self._sessions["trusted"]

    def untrusted(self) -> aiohttp.ClientSession:
        if "untrusted" not in self._sessions:
            self._sessions["untrusted"] = self._build_untrusted()
        return self._sessions["untrusted"]

    async def close(self) -> None:
        sessions = list(self._sessions.values())
        self._sessions.clear()
        for session in sessions:
            await session.close()
