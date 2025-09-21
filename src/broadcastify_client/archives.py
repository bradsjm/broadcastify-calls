"""Archive retrieval client with cache integration."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Protocol

import httpx

from .cache import CacheBackend, InMemoryCache
from .config import CacheConfig
from .errors import ResponseParsingError
from .http import AsyncHttpClientProtocol
from .metadata import parse_call_metadata
from .models import ArchiveCallEnvelope, ArchiveResult, Call, TimeWindow
from .schemas import ArchiveCallEntry, ArchiveCallsResponse

logger = logging.getLogger(__name__)


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
            logger.debug("Archive cache hit for key %s", key)
            return cached.mark_cache_hit()
        logger.info(
            "Fetching archived calls for system %s talkgroup %s block %s",
            system_id,
            talkgroup_id,
            time_block,
        )
        response = await self._http_client.get(
            "/calls/apis/archivecall.php",
            params={
                "group": f"{system_id}-{talkgroup_id}",
                "s": str(time_block),
            },
        )
        try:
            result = self._parser.parse(response)
        except Exception as exc:  # pragma: no cover - defensive path
            logger.exception(
                "Failed to parse archive response for system %s talkgroup %s block %s",
                system_id,
                talkgroup_id,
                time_block,
            )
            raise ResponseParsingError(str(exc)) from exc
        await self._cache.set(key, result)
        logger.debug("Stored archive response in cache for key %s", key)
        return result


class JsonArchiveParser(ArchiveParser):
    """Parse JSON archive payloads following the documented schema."""

    def parse(self, response: httpx.Response) -> ArchiveResult:
        """Parse an HTTP JSON response into an ArchiveResult."""
        try:
            payload = response.json()
        except ValueError as exc:  # pragma: no cover - defensive path
            raise ResponseParsingError("Archive payload is not valid JSON") from exc

        envelope = ArchiveCallsResponse.model_validate(payload)
        fetched_at = datetime.now(UTC)
        calls = [self._to_envelope(item, fetched_at) for item in envelope.calls]

        window = TimeWindow(
            start=datetime.fromtimestamp(envelope.start, UTC),
            end=datetime.fromtimestamp(envelope.end, UTC),
        )

        typed_payload = MappingProxyType(envelope.model_dump(by_alias=True))
        return ArchiveResult(
            calls=calls,
            window=window,
            fetched_at=fetched_at,
            cache_hit=False,
            raw=typed_payload,
        )

    def _to_envelope(
        self, entry: ArchiveCallEntry, retrieved_at: datetime
    ) -> ArchiveCallEnvelope:
        metadata = parse_call_metadata(entry.metadata)
        call = Call(
            call_id=entry.id,
            system_id=entry.system_id,
            talkgroup_id=entry.call_tg,
            received_at=datetime.fromtimestamp(entry.ts, UTC),
            frequency_mhz=entry.call_freq,
            metadata=metadata,
            ttl_seconds=entry.call_ttl,
        )
        raw_payload = MappingProxyType(entry.model_dump(by_alias=True))
        return ArchiveCallEnvelope(call=call, retrieved_at=retrieved_at, raw_payload=raw_payload)
