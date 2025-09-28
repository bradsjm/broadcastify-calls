"""Async Broadcastify client library scaffold."""

from __future__ import annotations

from .client import (
    AsyncBroadcastifyClient,
    BroadcastifyClient,
    BroadcastifyClientDependencies,
    LiveSubscriptionHandle,
    PlaylistSubscription,
    SystemSubscription,
    TalkgroupSubscription,
)
from .config import (
    CacheConfig,
    Credentials,
    HttpClientConfig,
    LiveProducerConfig,
    TranscriptionConfig,
    load_credentials_from_environment,
)
from .eventbus import ConsumerCallback, EventBus
from .models import (
    ArchiveResult,
    AudioChunkEvent,
    Call,
    LiveCallEnvelope,
    PlaylistDescriptor,
    PlaylistSubscriptionState,
    ProducerRuntimeState,
    RuntimeMetrics,
    SearchResultPage,
    SessionToken,
    SourceDescriptor,
    SystemSummary,
    TalkgroupSummary,
    TimeWindow,
    TranscriptionResult,
    TranscriptionSegment,
)

__all__ = [
    "ArchiveResult",
    "AsyncBroadcastifyClient",
    "AudioChunkEvent",
    "BroadcastifyClient",
    "BroadcastifyClientDependencies",
    "CacheConfig",
    "Call",
    "ConsumerCallback",
    "Credentials",
    "EventBus",
    "HttpClientConfig",
    "LiveCallEnvelope",
    "LiveProducerConfig",
    "LiveSubscriptionHandle",
    "PlaylistDescriptor",
    "PlaylistSubscription",
    "PlaylistSubscriptionState",
    "ProducerRuntimeState",
    "RuntimeMetrics",
    "SearchResultPage",
    "SessionToken",
    "SourceDescriptor",
    "SystemSubscription",
    "SystemSummary",
    "TalkgroupSubscription",
    "TalkgroupSummary",
    "TimeWindow",
    "TranscriptionConfig",
    "TranscriptionResult",
    "TranscriptionSegment",
    "load_credentials_from_environment",
]
