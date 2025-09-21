"""Live call producer that polls Broadcastify for new call events."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from secrets import SystemRandom
from typing import Protocol

from .config import LiveProducerConfig
from .errors import BroadcastifyError, LiveSessionError
from .models import CallEvent
from .telemetry import NullTelemetrySink, TelemetrySink


class CallPoller(Protocol):
    """Protocol producing call events from Broadcastify."""

    async def fetch(
        self, *, cursor: float | None
    ) -> tuple[Iterable[CallEvent], float | None]:  # pragma: no cover - protocol
        """Return new call events and the next cursor to persist."""

        ...


class LiveCallProducer:
    """Polls Broadcastify and places call events onto an asyncio queue."""

    def __init__(
        self,
        poller: CallPoller,
        config: LiveProducerConfig,
        *,
        telemetry: TelemetrySink | None = None,
        queue: asyncio.Queue[CallEvent] | None = None,
    ) -> None:
        """Create a producer using *poller* and *config*."""

        self._poller = poller
        self._config = config
        self._telemetry = telemetry or NullTelemetrySink()
        maxsize = 0 if config.queue_maxsize == 0 else int(config.queue_maxsize)
        self._queue = queue or asyncio.Queue(maxsize=maxsize)
        self._cursor = config.initial_position
        self._stopped = asyncio.Event()
        self._rate_limiter = (
            _RateLimiter(config.rate_limit_per_minute)
            if config.rate_limit_per_minute and config.rate_limit_per_minute > 0
            else None
        )
        self._metrics_interval = config.metrics_interval

    @property
    def queue(self) -> asyncio.Queue[CallEvent]:
        """Return the queue onto which call events are published."""

        return self._queue

    async def run(self) -> None:
        """Continuously poll for call events until stopped."""

        loop = asyncio.get_running_loop()
        last_metrics_emit = loop.time()
        backoff = self._config.initial_backoff
        consecutive_failures = 0

        while not self._stopped.is_set():
            if self._rate_limiter is not None:
                await self._rate_limiter.acquire()

            try:
                events, next_cursor = await self._poller.fetch(cursor=self._cursor)
            except BroadcastifyError as exc:
                consecutive_failures += 1
                self._telemetry.record_event(
                    "live_producer.poll.error",
                    attributes={
                        "error_type": exc.__class__.__name__,
                        "attempt": consecutive_failures,
                    },
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
                self._telemetry.record_event(
                    "live_producer.poll.error",
                    attributes={"error_type": exc.__class__.__name__},
                )
                raise LiveSessionError("Unexpected failure in live poller") from exc

            consecutive_failures = 0
            backoff = self._config.initial_backoff

            dispatched = 0
            async with asyncio.TaskGroup() as task_group:
                for event in events:
                    dispatched += 1
                    task_group.create_task(self._queue.put(event))

            self._telemetry.record_metric("live_producer.dispatched", float(dispatched))
            if next_cursor is not None:
                self._cursor = next_cursor

            now = loop.time()
            if self._metrics_interval > 0 and (now - last_metrics_emit) >= self._metrics_interval:
                self._telemetry.record_metric(
                    "live_producer.queue_depth",
                    float(self._queue.qsize()),
                )
                last_metrics_emit = now

            if dispatched == 0:
                await asyncio.sleep(
                    _jittered_interval(self._config.poll_interval, self._config.jitter_ratio)
                )

    async def stop(self) -> None:
        """Request termination of the producer loop."""

        self._stopped.set()


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

    async def acquire(self) -> None:
        if self._min_interval <= 0:
            return
        loop = asyncio.get_running_loop()
        now = loop.time()
        if self._last_acquire is not None:
            wait_for = self._min_interval - (now - self._last_acquire)
            if wait_for > 0:
                await asyncio.sleep(wait_for)
        self._last_acquire = loop.time()


_JITTER_RANDOM = SystemRandom()
