"""Live call producer that polls Broadcastify for new call events."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from datetime import UTC, datetime
from secrets import SystemRandom
from typing import Protocol

from .config import LiveProducerConfig
from .errors import BroadcastifyError, LiveSessionError
from .models import LiveCallEnvelope, ProducerRuntimeState
from .telemetry import (
    LivePollErrorEvent,
    NullTelemetrySink,
    PollMetrics,
    QueueDepthGauge,
    TelemetrySink,
)

logger = logging.getLogger(__name__)


class CallPoller(Protocol):
    """Protocol producing call events from Broadcastify."""

    async def fetch(
        self, *, cursor: float | None
    ) -> tuple[Iterable[LiveCallEnvelope], float | None]:  # pragma: no cover - protocol
        """Return new call events and the next cursor to persist."""
        ...


class LiveCallProducer:
    """Polls Broadcastify and places call events onto an asyncio queue."""

    def __init__(
        self,
        poller: CallPoller,
        config: LiveProducerConfig,
        *,
        topic: str,
        telemetry: TelemetrySink | None = None,
        queue: asyncio.Queue[LiveCallEnvelope] | None = None,
    ) -> None:
        """Create a producer using *poller* and *config*."""
        self._poller = poller
        self._config = config
        self._topic = topic
        self._telemetry = telemetry or NullTelemetrySink()
        maxsize = 0 if config.queue_maxsize == 0 else int(config.queue_maxsize)
        self._queue = queue or asyncio.Queue(maxsize=maxsize)
        self._cursor = config.initial_position
        self._initial_history = (
            -1 if config.initial_history is None else int(config.initial_history)
        )
        self._history_applied = False
        self._stopped = asyncio.Event()
        self._rate_limiter = (
            _RateLimiter(config.rate_limit_per_minute)
            if config.rate_limit_per_minute and config.rate_limit_per_minute > 0
            else None
        )
        self._metrics_interval = config.metrics_interval
        self._last_poll_started_at: datetime | None = None
        self._last_poll_completed_at: datetime | None = None
        self._last_event_at: datetime | None = None
        self._consecutive_failures = 0
        self._rate_limited_last = False

    @property
    def queue(self) -> asyncio.Queue[LiveCallEnvelope]:
        """Return the queue onto which call events are published."""
        return self._queue

    @property
    def cursor(self) -> float | None:
        """Return the last cursor observed by the producer."""
        return self._cursor

    async def run(self) -> None:
        """Continuously poll for call events until stopped."""
        loop = asyncio.get_running_loop()
        last_metrics_emit = loop.time()
        backoff = self._config.initial_backoff
        consecutive_failures = 0

        while not self._stopped.is_set():
            rate_limited = False
            if self._rate_limiter is not None:
                rate_limited = await self._rate_limiter.acquire()

            try:
                self._last_poll_started_at = datetime.now(UTC)
                events, next_cursor = await self._poller.fetch(cursor=self._cursor)
            except BroadcastifyError as exc:
                consecutive_failures += 1
                self._consecutive_failures = consecutive_failures
                logger.warning(
                    "Live poll failed for cursor %s (attempt %s): %s",
                    self._cursor,
                    consecutive_failures,
                    exc,
                )
                self._telemetry.record_event(
                    LivePollErrorEvent(
                        cursor=self._cursor,
                        attempt=consecutive_failures,
                        error_type=exc.__class__.__name__,
                        message=str(exc),
                    )
                )
                if (
                    self._config.max_retry_attempts is not None
                    and consecutive_failures > self._config.max_retry_attempts
                ):
                    raise LiveSessionError("Live poller exceeded retry budget") from exc
                await asyncio.sleep(min(backoff, self._config.max_backoff))
                backoff = min(backoff * 2.0, self._config.max_backoff)
                continue
            except Exception as exc:  # pragma: no cover - defensive path
                logger.exception("Unexpected failure while fetching live calls")
                self._telemetry.record_event(
                    LivePollErrorEvent(
                        cursor=self._cursor,
                        attempt=consecutive_failures + 1,
                        error_type=exc.__class__.__name__,
                        message=str(exc),
                    )
                )
                raise LiveSessionError("Unexpected failure in live poller") from exc

            consecutive_failures = 0
            self._consecutive_failures = 0
            backoff = self._config.initial_backoff

            dispatched = 0
            async with asyncio.TaskGroup() as task_group:
                events = self._filter_initial_history(events)
                for event in events:
                    dispatched += 1
                    task_group.create_task(self._queue.put(event))
                    self._last_event_at = event.received_at

            self._last_poll_completed_at = datetime.now(UTC)
            self._rate_limited_last = rate_limited
            self._telemetry.record_metric(
                PollMetrics(
                    topic=self._topic,
                    dispatched=dispatched,
                    queue_depth=self._queue.qsize(),
                    poll_started_at=self._last_poll_started_at,
                    poll_completed_at=self._last_poll_completed_at,
                    rate_limited=rate_limited,
                )
            )
            logger.debug("Dispatched %s call event(s) for queue", dispatched)
            if next_cursor is not None:
                self._cursor = next_cursor

            now = loop.time()
            if self._metrics_interval > 0 and (now - last_metrics_emit) >= self._metrics_interval:
                self._telemetry.record_metric(
                    QueueDepthGauge(topic=self._topic, depth=self._queue.qsize())
                )
                last_metrics_emit = now

            if dispatched == 0:
                await asyncio.sleep(
                    _jittered_interval(self._config.poll_interval, self._config.jitter_ratio)
                )

    def _filter_initial_history(
        self, events: Iterable[LiveCallEnvelope]
    ) -> list[LiveCallEnvelope]:
        """Apply the initial history limit to the first batch of events only.

        If ``initial_history`` is 0, we keep only the most recent event(s) in the
        first batch (based on the largest cursor/received_at) and drop older ones.
        For ``initial_history`` > 0, we keep the last N events in the batch.

        Subsequent batches are returned unchanged.
        """
        seq = list(events)
        if self._history_applied:
            return seq
        self._history_applied = True
        if not seq:
            return seq

        def _event_key(ev: LiveCallEnvelope) -> float:
            # Use the original call timestamp to determine recency; backend cursors may be
            # normalised and identical across a batch and are thus not reliable here.
            return ev.call.received_at.timestamp()

        if self._initial_history == 0:
            # Live-only: drop the initial history batch entirely.
            return []
        if self._initial_history < 0:
            return seq

        # Keep the last N events by key order (oldest→newest).
        seq.sort(key=_event_key)
        return seq[-self._initial_history :]

    async def stop(self) -> None:
        """Request termination of the producer loop."""
        self._stopped.set()
        logger.info("LiveCallProducer received stop request")

    def snapshot(self) -> ProducerRuntimeState:
        """Return a runtime snapshot of the producer state."""
        return ProducerRuntimeState(
            topic=self._topic,
            queue_depth=self._queue.qsize(),
            cursor=self._cursor,
            last_event_at=self._last_event_at,
            consecutive_failures=self._consecutive_failures,
            rate_limited=self._rate_limited_last,
        )


def _jittered_interval(poll_interval: float, jitter_ratio: float) -> float:
    if jitter_ratio <= 0:
        return poll_interval
    span = poll_interval * jitter_ratio
    return max(0.0, poll_interval + _JITTER_RANDOM.uniform(-span, span))


class _RateLimiter:
    """Enforces a minimum wall-clock interval between polls based on rate limits."""

    def __init__(self, rate_per_minute: int) -> None:
        self._min_interval = 60.0 / float(rate_per_minute)
        self._last_acquire: float | None = None

    async def acquire(self) -> bool:
        if self._min_interval <= 0:
            return False
        loop = asyncio.get_running_loop()
        now = loop.time()
        waited = False
        if self._last_acquire is not None:
            wait_for = self._min_interval - (now - self._last_acquire)
            if wait_for > 0:
                await asyncio.sleep(wait_for)
                waited = True
        self._last_acquire = loop.time()
        return waited


_JITTER_RANDOM = SystemRandom()
