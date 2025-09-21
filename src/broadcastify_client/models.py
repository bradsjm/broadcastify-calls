"""Typed data models for Broadcastify call, audio, and transcription events."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from types import MappingProxyType


def _empty_mapping() -> Mapping[str, object]:
    """Return an immutable empty mapping for default payload storage."""

    return MappingProxyType({})


@dataclass(frozen=True, slots=True)
class SessionToken:
    """Represents an authenticated Broadcastify session token."""

    token: str
    issued_at: datetime | None = None
    expires_at: datetime | None = None

    def is_expired(self, *, as_of: datetime | None = None) -> bool:
        """Return True when the session token is expired relative to *as_of*."""

        if self.expires_at is None:
            return False
        reference = as_of or datetime.now(self.expires_at.tzinfo or UTC)
        return reference >= self.expires_at

    @property
    def value(self) -> str:
        """Compatibility alias for the underlying token string."""

        return self.token


@dataclass(frozen=True, slots=True)
class Call:
    """Represents metadata for a Broadcastify call."""

    call_id: int
    system_id: int
    talkgroup_id: int
    received_at: datetime
    frequency_hz: float | None
    metadata: Mapping[str, str]
    ttl_seconds: float | None = None
    raw: Mapping[str, object] = field(default_factory=_empty_mapping)


@dataclass(frozen=True, slots=True)
class TimeWindow:
    """Represents the time window covered by an archive query."""

    start: datetime
    end: datetime


@dataclass(frozen=True, slots=True)
class ArchiveResult:
    """Represents archived call metadata for a time window."""

    calls: Sequence[Call]
    window: TimeWindow
    fetched_at: datetime
    cache_hit: bool
    raw: Mapping[str, object] = field(default_factory=_empty_mapping)

    def mark_cache_hit(self) -> ArchiveResult:
        """Return a copy of this result flagged as a cache hit."""

        if self.cache_hit:
            return self
        return replace(self, cache_hit=True)


@dataclass(frozen=True, slots=True)
class CallEvent:
    """Event emitted when a new call is available."""

    call: Call
    cursor: float | None
    received_at: datetime
    shard_key: tuple[int, int]
    raw_payload: Mapping[str, object] = field(default_factory=_empty_mapping)


@dataclass(frozen=True, slots=True)
class AudioChunkEvent:
    """Event carrying an audio chunk for downstream consumers."""

    call_id: int
    sequence: int
    start_offset: float
    end_offset: float
    payload: bytes
    content_type: str
    finished: bool


@dataclass(frozen=True, slots=True)
class TranscriptionPartial:
    """Represents an intermediate transcription update for a call."""

    call_id: int
    chunk_index: int
    start_time: float
    end_time: float
    text: str
    confidence: float | None


@dataclass(frozen=True, slots=True)
class TranscriptionResult:
    """Represents the completed transcription for a call."""

    call_id: int
    text: str
    language: str
    average_logprob: float | None
    segments: Sequence[TranscriptionPartial]
