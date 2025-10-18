"""UI telemetry sink buffering metrics and errors for the Rich dashboard.

This sink is injected into the BroadcastifyClient to receive telemetry signals.
It stores a bounded history of recent events and the latest metric samples so the
UI runtime can render them without expensive synchronization or global state.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Lock

from ..telemetry import (
    AudioErrorEvent,
    LiveDispatchErrorEvent,
    LivePollErrorEvent,
    PollMetrics,
    QueueDepthGauge,
    TelemetryEvent,
    TelemetryMetric,
    TelemetrySink,
)


@dataclass(frozen=True, slots=True)
class UiTelemetrySnapshot:
    """Immutable snapshot of recent telemetry for UI rendering."""

    generated_at: datetime
    last_poll_metrics: list[PollMetrics]
    last_queue_gauges: list[QueueDepthGauge]
    recent_poll_errors: list[LivePollErrorEvent]
    recent_dispatch_errors: list[LiveDispatchErrorEvent]
    recent_audio_errors: list[AudioErrorEvent]


class UiTelemetrySink(TelemetrySink):
    """Thread-safe telemetry sink retaining recent metrics and errors for the UI."""

    def __init__(self, *, history: int = 100) -> None:
        """Initialise the sink with bounded history capacity."""
        self._poll_metrics: deque[PollMetrics] = deque(maxlen=history)
        self._queue_gauges: deque[QueueDepthGauge] = deque(maxlen=history)
        self._poll_errors: deque[LivePollErrorEvent] = deque(maxlen=history)
        self._dispatch_errors: deque[LiveDispatchErrorEvent] = deque(maxlen=history)
        self._audio_errors: deque[AudioErrorEvent] = deque(maxlen=history)
        self._lock = Lock()

    def record_event(self, event: TelemetryEvent) -> None:  # pragma: no cover - exercised via UI
        """Buffer a structured telemetry event for recent error panes."""
        with self._lock:
            if isinstance(event, LivePollErrorEvent):
                self._poll_errors.append(event)
            elif isinstance(event, LiveDispatchErrorEvent):
                self._dispatch_errors.append(event)
            elif isinstance(event, AudioErrorEvent):
                self._audio_errors.append(event)

    def record_metric(self, metric: TelemetryMetric) -> None:  # pragma: no cover - exercised via UI
        """Buffer a metric sample for display in telemetry panels."""
        with self._lock:
            if isinstance(metric, PollMetrics):
                self._poll_metrics.append(metric)
            elif isinstance(metric, QueueDepthGauge):
                self._queue_gauges.append(metric)

    def snapshot(self) -> UiTelemetrySnapshot:
        """Return an immutable snapshot of recent telemetry buffers."""
        with self._lock:
            return UiTelemetrySnapshot(
                generated_at=datetime.now(UTC),
                last_poll_metrics=list(self._poll_metrics),
                last_queue_gauges=list(self._queue_gauges),
                recent_poll_errors=list(self._poll_errors),
                recent_dispatch_errors=list(self._dispatch_errors),
                recent_audio_errors=list(self._audio_errors),
            )
