"""Cache provider contracts and a default in-memory implementation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, TypeVar

from .config import CacheConfig
from .errors import CacheError

KeyT_contra = TypeVar("KeyT_contra", contravariant=True)
ValueT = TypeVar("ValueT")


class CacheBackend(Protocol[KeyT_contra, ValueT]):
    """Protocol implemented by async cache backends."""

    async def get(self, key: KeyT_contra) -> ValueT | None:  # pragma: no cover - protocol
        """Return a cached value for *key* if present."""
        ...

    async def set(self, key: KeyT_contra, value: ValueT) -> None:  # pragma: no cover - protocol
        """Store *value* for *key* in the cache."""
        ...


@dataclass(slots=True)
class _CacheEntry[ValueT]:
    """Internal cache entry storing a value and its expiry timestamp."""

    value: ValueT
    expires_at: datetime


class InMemoryCache(CacheBackend[KeyT_contra, ValueT]):
    """Simple in-memory cache honoring a time-to-live per entry."""

    def __init__(self, config: CacheConfig | None = None) -> None:
        """Create an in-memory cache with optional *config*."""
        self._config = config or CacheConfig()
        self._store: dict[KeyT_contra, _CacheEntry[ValueT]] = {}

    async def get(self, key: KeyT_contra) -> ValueT | None:
        """Return the cached value when it has not expired."""
        entry = self._store.get(key)
        if entry is None:
            return None
        if datetime.now(tz=UTC) >= entry.expires_at:
            self._store.pop(key, None)
            return None
        return entry.value

    async def set(self, key: KeyT_contra, value: ValueT) -> None:
        """Store *value* in the cache honoring the configured TTL."""
        ttl = self._config.ttl
        if ttl.total_seconds() <= 0:
            raise CacheError("Cache TTL must be positive")
        expires_at = datetime.now(tz=UTC) + ttl
        self._store[key] = _CacheEntry(value=value, expires_at=expires_at)
