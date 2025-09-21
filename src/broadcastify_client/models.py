"""Typed data models for Broadcastify domain concepts."""

from __future__ import annotations

from collections.abc import ItemsView, KeysView, Mapping, Sequence, ValuesView
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from types import MappingProxyType
from typing import TypeVar

type PlaylistId = str
type CallId = str
type SystemId = int
type TalkgroupId = int


def _empty_object_mapping() -> Mapping[str, object]:
    """Return an immutable empty mapping for raw payload defaults."""
    return MappingProxyType({})


def _empty_string_mapping() -> Mapping[str, str]:
    """Return an immutable empty mapping for metadata extras."""
    return MappingProxyType({})


@dataclass(frozen=True, slots=True)
class Extras:
    """Container for provider-specific metadata that lacks a typed representation."""

    values: Mapping[str, str] = field(default_factory=_empty_string_mapping)

    def get(self, key: str, default: str | None = None) -> str | None:
        """Return the value stored under *key* if present."""
        return self.values.get(key, default)

    def __contains__(self, key: object) -> bool:
        """Return ``True`` when *key* exists in the extras mapping."""
        return key in self.values

    def keys(self) -> KeysView[str]:
        """Return a view of available extra keys."""
        return self.values.keys()

    def items(self) -> ItemsView[str, str]:
        """Return a view of extra key/value pairs."""
        return self.values.items()

    def values_view(self) -> ValuesView[str]:
        """Return a view of extra values."""
        return self.values.values()


@dataclass(frozen=True, slots=True)
class PlaylistReference:
    """Reference metadata for the playlist associated with a call."""

    playlist_id: PlaylistId
    name: str | None = None
    description: str | None = None


@dataclass(frozen=True, slots=True)
class AgencyDescriptor:
    """Agency descriptor derived from Broadcastify metadata."""

    name: str | None = None
    service_description: str | None = None
    call_sign: str | None = None


@dataclass(frozen=True, slots=True)
class LocationDescriptor:
    """Geographic descriptor for an agency or talkgroup."""

    city: str | None = None
    county: str | None = None
    state: str | None = None
    country: str | None = None
    latitude: float | None = None
    longitude: float | None = None


@dataclass(frozen=True, slots=True)
class ChannelDescriptor:
    """Channel descriptor describing the talkgroup and service category."""

    talkgroup_name: str | None = None
    service_description: str | None = None
    service_tag: str | None = None
    system_name: str | None = None
    frequency_mhz: float | None = None


@dataclass(frozen=True, slots=True)
class SourceDescriptor:
    """Descriptor for the originating source unit of a call."""

    identifier: int | None = None
    label: str | None = None


@dataclass(frozen=True, slots=True)
class CallMetadata:
    """Structured metadata associated with a call."""

    agency: AgencyDescriptor | None = None
    playlist: PlaylistReference | None = None
    location: LocationDescriptor | None = None
    channel: ChannelDescriptor | None = None
    extras: Extras = field(default_factory=Extras)


@dataclass(frozen=True, slots=True)
class Call:
    """Represents metadata for a Broadcastify call."""

    call_id: CallId
    system_id: SystemId
    system_name: str | None
    talkgroup_id: TalkgroupId
    talkgroup_label: str | None
    talkgroup_description: str | None
    received_at: datetime
    frequency_mhz: float | None
    duration_seconds: float | None
    source: SourceDescriptor
    metadata: CallMetadata
    ttl_seconds: float | None = None


@dataclass(frozen=True, slots=True)
class LiveCallEnvelope:
    """Envelope describing a live call event and its raw payload."""

    call: Call
    cursor: float | None
    received_at: datetime
    shard_key: tuple[SystemId, TalkgroupId]
    raw_payload: Mapping[str, object] = field(default_factory=_empty_object_mapping)


@dataclass(frozen=True, slots=True)
class TimeWindow:
    """Represents the time window covered by an archive query."""

    start: datetime
    end: datetime


@dataclass(frozen=True, slots=True)
class ArchiveCallEnvelope:
    """Envelope describing an archived call record."""

    call: Call
    retrieved_at: datetime
    raw_payload: Mapping[str, object] = field(default_factory=_empty_object_mapping)


@dataclass(frozen=True, slots=True)
class ArchiveResult:
    """Represents archived call metadata for a time window."""

    calls: Sequence[ArchiveCallEnvelope]
    window: TimeWindow
    fetched_at: datetime
    cache_hit: bool
    raw: Mapping[str, object] = field(default_factory=_empty_object_mapping)

    def mark_cache_hit(self) -> ArchiveResult:
        """Return a copy of this result flagged as a cache hit."""
        if self.cache_hit:
            return self
        return replace(self, cache_hit=True)


@dataclass(frozen=True, slots=True)
class AudioChunkEvent:
    """Event carrying an audio chunk for downstream consumers."""

    call_id: CallId
    sequence: int
    start_offset: float
    end_offset: float
    payload: bytes
    content_type: str
    finished: bool


@dataclass(frozen=True, slots=True)
class TranscriptionPartial:
    """Represents an intermediate transcription update for a call."""

    call_id: CallId
    chunk_index: int
    start_time: float
    end_time: float
    text: str
    confidence: float | None


@dataclass(frozen=True, slots=True)
class TranscriptionResult:
    """Represents the completed transcription for a call."""

    call_id: CallId
    text: str
    language: str
    average_logprob: float | None
    segments: Sequence[TranscriptionPartial]


@dataclass(frozen=True, slots=True)
class PlaylistDescriptor:
    """High-level descriptor for a Broadcastify playlist."""

    playlist_id: PlaylistId
    name: str
    is_public: bool
    description: str | None = None
    talkgroup_count: int | None = None
    system_count: int | None = None
    last_updated: datetime | None = None


@dataclass(frozen=True, slots=True)
class PlaylistMember:
    """Metadata describing membership of a talkgroup within a playlist."""

    system_id: SystemId
    talkgroup_id: TalkgroupId
    alpha_tag: str | None = None
    description: str | None = None
    service_tag: str | None = None


@dataclass(frozen=True, slots=True)
class PlaylistSubscriptionState:
    """Snapshot of playlist subscription cursor and members."""

    playlist: PlaylistDescriptor
    members: Sequence[PlaylistMember]
    cursor: float | None = None


@dataclass(frozen=True, slots=True)
class SystemSummary:
    """Summary metadata for a Broadcastify system."""

    system_id: SystemId
    name: str
    description: str | None = None
    city: str | None = None
    county: str | None = None
    state: str | None = None
    country: str | None = None


@dataclass(frozen=True, slots=True)
class TalkgroupSummary:
    """Summary metadata for a Broadcastify talkgroup."""

    talkgroup_id: TalkgroupId
    system_id: SystemId
    alpha_tag: str
    description: str | None = None
    service_tag: str | None = None


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class SearchResultPage[T]:
    """Paginated search results for discovery endpoints."""

    items: Sequence[T]
    total: int
    page: int
    page_size: int

    @property
    def has_more(self) -> bool:
        """Return True when additional pages are available."""
        return (self.page * self.page_size) < self.total


@dataclass(frozen=True, slots=True)
class ProducerRuntimeState:
    """Runtime snapshot for a live producer."""

    topic: str
    queue_depth: int
    cursor: float | None
    last_event_at: datetime | None
    consecutive_failures: int
    rate_limited: bool


@dataclass(frozen=True, slots=True)
class RuntimeMetrics:
    """Aggregated runtime metrics for the Broadcastify client."""

    generated_at: datetime
    producers: Sequence[ProducerRuntimeState]
    consumer_topics: int

    @property
    def total_queue_depth(self) -> int:
        """Return the sum of queue depths across all producers."""
        return sum(state.queue_depth for state in self.producers)


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
