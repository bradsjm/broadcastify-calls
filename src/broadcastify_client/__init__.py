"""Async Broadcastify client library scaffold."""

from __future__ import annotations

from .audio_segmentation import (
    AudioSegment,
    AudioSegmentationError,
    AudioSegmenter,
)
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
    AudioProcessingConfig,
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
    AudioPayloadEvent,
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
)

__all__ = [
    "ArchiveResult",
    "AsyncBroadcastifyClient",
    "AudioPayloadEvent",
    "AudioProcessingConfig",
    "AudioSegment",
    "AudioSegmentationError",
    "AudioSegmenter",
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
    "load_credentials_from_environment",
]
