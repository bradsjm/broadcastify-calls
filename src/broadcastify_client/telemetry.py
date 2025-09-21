"""Telemetry hook interfaces for structured logging and metrics."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

from .models import CallId


@dataclass(frozen=True, slots=True)
class TelemetrySignal:
    """Base class for telemetry signals."""

    emitted_at: datetime = field(init=False)

    def __post_init__(self) -> None:
        """Stamp the signal with the UTC time it was emitted."""
        object.__setattr__(self, "emitted_at", datetime.now(UTC))


@dataclass(frozen=True, slots=True)
class TelemetryEvent(TelemetrySignal):
    """Represents a discrete telemetry event."""


@dataclass(frozen=True, slots=True)
class TelemetryMetric(TelemetrySignal):
    """Represents a telemetry metric sample."""


@dataclass(frozen=True, slots=True)
class PollMetrics(TelemetryMetric):
    """Metric payload emitted after completing a live poll cycle."""

    topic: str
    dispatched: int
    queue_depth: int
    poll_started_at: datetime | None = None
    poll_completed_at: datetime | None = None
    rate_limited: bool = False


@dataclass(frozen=True, slots=True)
class QueueDepthGauge(TelemetryMetric):
    """Gauge measurement describing queue depth for a topic."""

    topic: str
    depth: int


@dataclass(frozen=True, slots=True)
class AudioErrorEvent(TelemetryEvent):
    """Event emitted when audio download or dispatch encounters an error."""

    call_id: CallId
    system_id: int
    talkgroup_id: int
    error_type: str
    message: str | None = None


@dataclass(frozen=True, slots=True)
class LivePollErrorEvent(TelemetryEvent):
    """Event emitted when a live poll attempt fails."""

    cursor: float | None
    attempt: int
    error_type: str
    message: str | None = None


@dataclass(frozen=True, slots=True)
class LiveDispatchErrorEvent(TelemetryEvent):
    """Event emitted when dispatching a live event to subscribers fails."""

    topic: str
    error_type: str
    message: str | None = None


class TelemetrySink(Protocol):
    """Protocol for emitting structured telemetry signals."""

    def record_event(self, event: TelemetryEvent) -> None:  # pragma: no cover - protocol
        """Record a structured event for diagnostics."""
        ...

    def record_metric(self, metric: TelemetryMetric) -> None:  # pragma: no cover - protocol
        """Record a metric sample."""
        ...


class NullTelemetrySink(TelemetrySink):
    """Telemetry sink that drops all signals."""

    def record_event(self, event: TelemetryEvent) -> None:
        """Drop the event without side effects."""

    def record_metric(self, metric: TelemetryMetric) -> None:
        """Drop the metric without side effects."""
