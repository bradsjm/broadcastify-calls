"""Async event bus abstraction supporting topic subscriptions."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import MutableMapping
from typing import Protocol


class ConsumerCallback(Protocol):
    """Protocol describing consumer callbacks invoked for topic events."""

    async def __call__(self, event: object) -> None:  # pragma: no cover - protocol signature
        """Consume a single event dispatched by the event bus."""

        ...


class EventBus:
    """Simple asyncio-based event bus with topic-aware subscriptions."""

    def __init__(self) -> None:
        """Initialise the event bus without subscribers."""

        self._subscribers: MutableMapping[str, list[ConsumerCallback]] = defaultdict(list)
        self._lock = asyncio.Lock()

    async def subscribe(self, topic: str, callback: ConsumerCallback) -> None:
        """Register *callback* to receive events for *topic*."""

        async with self._lock:
            if callback not in self._subscribers[topic]:
                self._subscribers[topic].append(callback)

    async def unsubscribe(self, topic: str, callback: ConsumerCallback) -> None:
        """Remove *callback* subscription for *topic* if present."""

        async with self._lock:
            callbacks = self._subscribers.get(topic)
            if not callbacks:
                return
            try:
                callbacks.remove(callback)
            except ValueError:
                return
            if not callbacks:
                self._subscribers.pop(topic, None)

    async def publish(self, topic: str, event: object) -> None:
        """Dispatch *event* to all subscribers of *topic*."""

        async with self._lock:
            callbacks = list(self._subscribers.get(topic, ()))
        if not callbacks:
            return
        await asyncio.gather(*(callback(event) for callback in callbacks))

    async def topics(self) -> dict[str, int]:
        """Return a snapshot of topics and subscriber counts."""

        async with self._lock:
            return {topic: len(callbacks) for topic, callbacks in self._subscribers.items()}
