"""Live call producer that polls Broadcastify for new call events."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from typing import Protocol

from .config import LiveProducerConfig
from .models import CallEvent
from .telemetry import NullTelemetrySink, TelemetrySink


class CallPoller(Protocol):
    """Protocol producing call events from Broadcastify."""

    async def fetch(
        self, *, cursor: int | None
    ) -> tuple[Iterable[CallEvent], int | None]:  # pragma: no cover - protocol
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
        self._queue = queue or asyncio.Queue(maxsize=config.rate_limit_per_minute or 0)
        self._cursor = config.initial_position
        self._stopped = asyncio.Event()

    @property
    def queue(self) -> asyncio.Queue[CallEvent]:
        """Return the queue onto which call events are published."""

        return self._queue

    async def run(self) -> None:
        """Continuously poll for call events until stopped."""

        while not self._stopped.is_set():
            events, next_cursor = await self._poller.fetch(cursor=self._cursor)
            dispatched = 0
            async with asyncio.TaskGroup() as tg:
                for event in events:
                    dispatched += 1
                    tg.create_task(self._queue.put(event))
            self._telemetry.record_metric("live_producer.dispatched", float(dispatched))
            if next_cursor is not None:
                self._cursor = next_cursor
            if dispatched == 0:
                await asyncio.sleep(self._config.poll_interval)

    async def stop(self) -> None:
        """Request termination of the producer loop."""

        self._stopped.set()
