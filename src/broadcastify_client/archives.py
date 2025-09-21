"""Archive retrieval client with cache integration."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, Protocol, cast

import httpx

from .cache import CacheBackend, InMemoryCache
from .config import CacheConfig
from .errors import ResponseParsingError
from .http import AsyncHttpClientProtocol
from .models import ArchiveResult, Call, TimeWindow

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
            "/archives/",
            params={"systemId": system_id, "talkgroupId": talkgroup_id, "time": time_block},
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
            payload = cast(dict[str, Any], response.json())
        except ValueError as exc:  # pragma: no cover - defensive path
            raise ResponseParsingError("Archive payload is not valid JSON") from exc

        calls_data = payload.get("calls", [])
        if not isinstance(calls_data, list):
            raise ResponseParsingError("Archive payload 'calls' must be a list")

        typed_calls_data = cast(list[dict[str, Any]], calls_data)
        calls = [self._parse_call(item) for item in typed_calls_data]
        window_payload = payload.get("window")
        if isinstance(window_payload, dict):
            typed_window = cast(dict[str, Any], window_payload)
            start_value = cast(object | None, typed_window.get("start"))
            end_value = cast(object | None, typed_window.get("end"))
        else:
            start_value = cast(object | None, payload.get("start"))
            end_value = cast(object | None, payload.get("end"))

        if start_value is None or end_value is None:
            raise ResponseParsingError("Archive payload missing window boundaries")

        window = TimeWindow(
            start=_parse_datetime(start_value),
            end=_parse_datetime(end_value),
        )

        fetched_at = datetime.now(UTC)
        typed_payload = MappingProxyType(dict(payload))
        return ArchiveResult(
            calls=calls,
            window=window,
            fetched_at=fetched_at,
            cache_hit=False,
            raw=typed_payload,
        )

    def _parse_call(self, item: Mapping[str, Any]) -> Call:
        try:
            call_id = int(item["id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ResponseParsingError("Call entry missing 'id'") from exc

        system_identifier = item.get("system_id") or item.get("systemId")
        talkgroup_identifier = item.get("talkgroup_id") or item.get("talkgroupId")
        system_id = _coerce_optional_int(system_identifier)
        talkgroup_id = _coerce_optional_int(talkgroup_identifier)
        if system_id is None or talkgroup_id is None:
            raise ResponseParsingError("Call entry missing system/talkgroup identifiers")

        received_at_value: object | None = item.get("received_at")
        if received_at_value is None:
            received_at_value = item.get("start_time")
        if received_at_value is None:
            received_at_value = item.get("start")
        if received_at_value is None:
            raise ResponseParsingError("Call entry missing 'received_at' timestamp")

        frequency_source = item.get("frequency_hz") or item.get("call_freq")
        frequency_hz = _coerce_optional_float(frequency_source)
        ttl_source = item.get("call_ttl") or item.get("ttl")
        ttl_seconds = _coerce_optional_float(ttl_source)
        metadata_mapping = _normalize_metadata(item.get("metadata"))
        raw_payload = MappingProxyType({str(key): value for key, value in item.items()})

        return Call(
            call_id=call_id,
            system_id=system_id,
            talkgroup_id=talkgroup_id,
            received_at=_parse_datetime(received_at_value),
            frequency_hz=frequency_hz,
            metadata=metadata_mapping,
            ttl_seconds=ttl_seconds,
            raw=raw_payload,
        )


def _parse_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            try:
                return datetime.fromtimestamp(float(value), tz=UTC)
            except ValueError as exc:
                raise ResponseParsingError(f"Unsupported datetime format: {value!r}") from exc
    raise ResponseParsingError(f"Unsupported datetime value: {value!r}")


def _normalize_metadata(value: object) -> Mapping[str, str]:
    if value is None:
        return MappingProxyType({})
    if not isinstance(value, Mapping):
        raise ResponseParsingError("Call metadata must be an object")
    typed_value = cast(Mapping[str, object], value)
    normalized = {str(key): str(val) for key, val in typed_value.items()}
    return MappingProxyType(normalized)


def _coerce_optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ResponseParsingError("Boolean cannot represent identifier")
    if isinstance(value, (int, float, str)):
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise ResponseParsingError(f"Invalid integer value: {value!r}") from exc
    raise ResponseParsingError(f"Unsupported integer value: {value!r}")


def _coerce_optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ResponseParsingError("Boolean cannot represent float value")
    if isinstance(value, (int, float, str)):
        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise ResponseParsingError(f"Invalid float value: {value!r}") from exc
    raise ResponseParsingError(f"Unsupported float value: {value!r}")
