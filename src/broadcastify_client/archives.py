"""Archive retrieval client with cache integration."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Protocol

import httpx

from .cache import CacheBackend, InMemoryCache
from .config import CacheConfig
from .errors import ResponseParsingError
from .http import AsyncHttpClientProtocol
from .models import ArchiveResult, Call


class ArchiveParser(Protocol):
    """Protocol responsible for parsing archive responses into typed results."""

    def parse(self, response: httpx.Response) -> ArchiveResult:  # pragma: no cover - protocol
        """Convert *response* into an ArchiveResult."""

        ...


class ArchiveClient:
    """Retrieves archived Broadcastify calls with caching support."""

    def __init__(
        self,
        http_client: AsyncHttpClientProtocol,
        parser: ArchiveParser,
        *,
        cache: CacheBackend[tuple[int, int, int], ArchiveResult] | None = None,
        cache_config: CacheConfig | None = None,
    ) -> None:
        """Create an archive client using *http_client* and *parser*."""

        self._http_client = http_client
        self._parser = parser
        self._cache = cache or InMemoryCache(cache_config)

    async def get_archived_calls(
        self, system_id: int, talkgroup_id: int, time_block: int
    ) -> ArchiveResult:
        """Return archived calls for the provided identifiers."""

        key = (system_id, talkgroup_id, time_block)
        cached = await self._cache.get(key)
        if cached is not None:
            return cached
        response = await self._http_client.get(
            "/archives/",
            params={"systemId": system_id, "talkgroupId": talkgroup_id, "time": time_block},
        )
        try:
            result = self._parser.parse(response)
        except Exception as exc:  # pragma: no cover - defensive path
            raise ResponseParsingError(str(exc)) from exc
        await self._cache.set(key, result)
        return result


class JsonArchiveParser(ArchiveParser):
    """Parse JSON archive payloads following the documented schema."""

    def parse(self, response: httpx.Response) -> ArchiveResult:
        """Parse an HTTP JSON response into an ArchiveResult."""

        payload = response.json()
        calls_data = payload.get("calls", [])
        calls = [
            Call(
                call_id=int(item["id"]),
                system_id=int(item["system_id"]),
                talkgroup_id=int(item["talkgroup_id"]),
                received_at=datetime.fromisoformat(item["received_at"]),
                frequency_hz=item.get("frequency_hz"),
                metadata=item.get("metadata", {}),
                raw=item,
            )
            for item in calls_data
        ]
        window = payload.get("window", {})
        window_start = datetime.fromisoformat(window["start"])
        window_end = datetime.fromisoformat(window["end"])
        fetched_at = datetime.now(UTC)
        return ArchiveResult(
            calls=calls,
            fetched_at=fetched_at,
            window_start=window_start,
            window_end=window_end,
            cache_hit=False,
        )
