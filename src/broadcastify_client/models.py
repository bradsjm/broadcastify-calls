"""Typed data models for Broadcastify call and transcription events."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime


def _empty_mapping() -> dict[str, object]:
    """Return a new empty mapping for raw payload storage."""

    return {}


@dataclass(frozen=True, slots=True)
class SessionToken:
    """Represents an authenticated Broadcastify session token."""

    token: str
    expires_at: datetime | None = None

    def is_expired(self, *, as_of: datetime | None = None) -> bool:
        """Return True when the session token is expired relative to *as_of*."""

        if self.expires_at is None:
            return False
        reference = as_of or datetime.now(self.expires_at.tzinfo)
        return reference >= self.expires_at


@dataclass(frozen=True, slots=True)
class Call:
    """Represents metadata for a Broadcastify call."""

    call_id: int
    system_id: int
    talkgroup_id: int
    received_at: datetime
    frequency_hz: float | None
    metadata: Mapping[str, str]
    raw: Mapping[str, object] = field(default_factory=_empty_mapping)


@dataclass(frozen=True, slots=True)
class CallEvent:
    """Event emitted when a new call is available."""

    call: Call
    cursor: int | None


@dataclass(frozen=True, slots=True)
class AudioChunkEvent:
    """Event carrying an audio chunk for downstream consumers."""

    call_id: int
    sequence: int
    data: bytes
    finished: bool


@dataclass(frozen=True, slots=True)
class ArchiveResult:
    """Represents archived call metadata for a time window."""

    calls: Sequence[Call]
    fetched_at: datetime
    window_start: datetime
    window_end: datetime
    cache_hit: bool


@dataclass(frozen=True, slots=True)
class TranscriptionPartial:
    """Represents an intermediate transcription update for a call."""

    call_id: int
    text: str
    confidence: float | None


@dataclass(frozen=True, slots=True)
class TranscriptionResult:
    """Represents the completed transcription for a call."""

    call_id: int
    text: str
    confidence: float | None
    segments: Sequence[Mapping[str, object]]
