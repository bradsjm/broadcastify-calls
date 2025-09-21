"""Public async client facade for interacting with Broadcastify."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol

from .archives import ArchiveClient, ArchiveParser, JsonArchiveParser
from .auth import AuthenticationBackend, Authenticator, HttpAuthenticationBackend
from .config import Credentials, HttpClientConfig, LiveProducerConfig
from .eventbus import ConsumerCallback, EventBus
from .http import AsyncHttpClientProtocol, BroadcastifyHttpClient
from .live_producer import CallPoller, LiveCallProducer
from .models import ArchiveResult, CallEvent, SessionToken
from .telemetry import NullTelemetrySink, TelemetrySink


class AsyncBroadcastifyClient(Protocol):
    """Public async-facing protocol for Broadcastify operations."""

    async def authenticate(
        self, credentials: Credentials | SessionToken
    ) -> SessionToken:  # pragma: no cover - protocol
        """Authenticate using *credentials* or validate an existing token."""

        ...

    async def logout(self) -> None:  # pragma: no cover - protocol
        """Invalidate the active session."""

        ...

    async def get_archived_calls(
        self, system_id: int, talkgroup_id: int, time_block: int
    ) -> ArchiveResult:  # pragma: no cover - protocol
        """Return archived calls for the provided identifiers."""

        ...

    async def create_live_producer(
        self, system_id: int, talkgroup_id: int, *, position: int | None = None
    ) -> LiveCallProducer:  # pragma: no cover - protocol
        """Create a live call producer for the given talkgroup."""

        ...

    async def register_consumer(
        self, topic: str, callback: ConsumerCallback
    ) -> None:  # pragma: no cover - protocol
        """Register a consumer callback for a topic."""

        ...

    async def start(self) -> None:  # pragma: no cover - protocol
        """Start all managed producers and consumers."""

        ...

    async def shutdown(self) -> None:  # pragma: no cover - protocol
        """Stop producers, consumers, and release resources."""

        ...


class CallPollerFactory(Protocol):
    """Factory responsible for creating call pollers."""

    def create(
        self,
        system_id: int,
        talkgroup_id: int,
        *,
        http_client: AsyncHttpClientProtocol,
        telemetry: TelemetrySink,
    ) -> CallPoller:  # pragma: no cover - factory protocol
        """Return a call poller for the given identifiers."""

        ...


@dataclass
class ProducerHandle:
    """Tracks the association between a producer and its execution task."""

    producer: LiveCallProducer
    task: asyncio.Task[None] | None = None


@dataclass(slots=True)
class BroadcastifyClientDependencies:
    """Optional dependency overrides for :class:`BroadcastifyClient`."""

    http_client: AsyncHttpClientProtocol | None = None
    http_config: HttpClientConfig | None = None
    authentication_backend: AuthenticationBackend | None = None
    authenticator: Authenticator | None = None
    archive_client: ArchiveClient | None = None
    archive_parser: ArchiveParser | None = None
    event_bus: EventBus | None = None
    telemetry: TelemetrySink | None = None
    call_poller_factory: CallPollerFactory | None = None


class _HttpCallPoller(CallPoller):
    """Placeholder HTTP poller returning no events until implemented."""

    def __init__(
        self,
        system_id: int,
        talkgroup_id: int,
        http_client: AsyncHttpClientProtocol,
        telemetry: TelemetrySink,
    ) -> None:
        """Create a placeholder HTTP poller for *system_id* and *talkgroup_id*."""

        self._system_id = system_id
        self._talkgroup_id = talkgroup_id
        self._http_client = http_client
        self._telemetry = telemetry

    async def fetch(self, *, cursor: int | None) -> tuple[list[CallEvent], int | None]:
        """Return no events; future iterations will call the Broadcastify feed."""

        self._telemetry.record_event(
            "live_producer.poll",
            attributes={
                "system_id": self._system_id,
                "talkgroup_id": self._talkgroup_id,
                "cursor": cursor,
            },
        )
        return [], cursor


class BroadcastifyClient(AsyncBroadcastifyClient):
    """Concrete async Broadcastify client coordinating authentication and producers."""

    def __init__(
        self,
        *,
        dependencies: BroadcastifyClientDependencies | None = None,
    ) -> None:
        """Create a Broadcastify client using optional dependency overrides."""

        deps = dependencies or BroadcastifyClientDependencies()
        self._telemetry = deps.telemetry or NullTelemetrySink()
        self._http_config = deps.http_config or HttpClientConfig()
        self._http_client = deps.http_client or BroadcastifyHttpClient(self._http_config)
        backend = deps.authentication_backend or HttpAuthenticationBackend(
            self._http_client, self._http_config
        )
        self._authenticator = deps.authenticator or Authenticator(backend)
        parser = deps.archive_parser or JsonArchiveParser()
        self._archive_client = deps.archive_client or ArchiveClient(self._http_client, parser)
        self._event_bus = deps.event_bus or EventBus()
        self._call_poller_factory = deps.call_poller_factory or _DefaultCallPollerFactory()
        self._producer_handles: list[ProducerHandle] = []
        self._started = False

    async def authenticate(self, credentials: Credentials | SessionToken) -> SessionToken:
        """Authenticate using credentials or validate the provided session token."""

        return await self._authenticator.authenticate(credentials)

    async def logout(self) -> None:
        """Log out of Broadcastify and clear the active session."""

        await self._authenticator.logout()

    async def get_archived_calls(
        self, system_id: int, talkgroup_id: int, time_block: int
    ) -> ArchiveResult:
        """Return archived calls for the given identifiers."""

        return await self._archive_client.get_archived_calls(
            system_id, talkgroup_id, time_block
        )

    async def create_live_producer(
        self, system_id: int, talkgroup_id: int, *, position: int | None = None
    ) -> LiveCallProducer:
        """Create and register a live call producer."""

        config = LiveProducerConfig(initial_position=position)
        poller = self._call_poller_factory.create(
            system_id,
            talkgroup_id,
            http_client=self._http_client,
            telemetry=self._telemetry,
        )
        producer = LiveCallProducer(poller, config, telemetry=self._telemetry)
        handle = ProducerHandle(producer=producer)
        self._producer_handles.append(handle)
        if self._started:
            self._start_producer(handle)
        return producer

    async def register_consumer(self, topic: str, callback: ConsumerCallback) -> None:
        """Register a consumer callback via the event bus."""

        await self._event_bus.subscribe(topic, callback)

    async def start(self) -> None:
        """Start all managed live call producers."""

        if self._started:
            return
        for handle in self._producer_handles:
            if handle.task is None:
                self._start_producer(handle)
        self._started = True

    async def shutdown(self) -> None:
        """Stop producers and release HTTP resources."""

        for handle in self._producer_handles:
            await handle.producer.stop()
        for handle in self._producer_handles:
            if handle.task is not None:
                handle.task.cancel()
        for handle in self._producer_handles:
            if handle.task is None:
                continue
            try:
                await handle.task
            except asyncio.CancelledError:
                pass
        await self._http_client.close()
        self._producer_handles.clear()
        self._started = False

    def _start_producer(self, handle: ProducerHandle) -> None:
        """Start the asynchronous task responsible for running *handle*'s producer."""

        handle.task = asyncio.create_task(handle.producer.run())


class _DefaultCallPollerFactory(CallPollerFactory):
    """Factory creating HTTP-backed call pollers."""

    def create(
        self,
        system_id: int,
        talkgroup_id: int,
        *,
        http_client: AsyncHttpClientProtocol,
        telemetry: TelemetrySink,
    ) -> CallPoller:
        """Return an HTTP-based poller for the given identifiers."""

        return _HttpCallPoller(system_id, talkgroup_id, http_client, telemetry)
