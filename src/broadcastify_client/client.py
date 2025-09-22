"""Public async client facade for interacting with Broadcastify."""

from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Protocol, cast
from uuid import uuid4

from .archives import ArchiveClient, ArchiveParser, JsonArchiveParser
from .audio_consumer import AudioConsumer
from .audio_downloader_http import HttpAudioDownloader
from .auth import AuthenticationBackend, Authenticator, HttpAuthenticationBackend
from .config import Credentials, HttpClientConfig, LiveProducerConfig, TranscriptionConfig
from .errors import AudioDownloadError, LiveSessionError
from .eventbus import ConsumerCallback, EventBus
from .http import AsyncHttpClientProtocol, BroadcastifyHttpClient
from .live_producer import CallPoller, LiveCallProducer
from .metadata import parse_call_metadata
from .models import (
    ArchiveResult,
    AudioChunkEvent,
    Call,
    CallId,
    LiveCallEnvelope,
    PlaylistDescriptor,
    PlaylistId,
    PlaylistSubscriptionState,
    ProducerRuntimeState,
    RuntimeMetrics,
    SearchResultPage,
    SessionToken,
    SourceDescriptor,
    SystemSummary,
    TalkgroupSummary,
)
from .schemas import LiveCallEntry, LiveCallsResponse
from .telemetry import (
    AudioErrorEvent,
    LiveDispatchErrorEvent,
    NullTelemetrySink,
    PollMetrics,
    TelemetrySink,
)
from .transcription import TranscriptionPipeline
from .transcription_openai import OpenAIWhisperBackend

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TalkgroupSubscription:
    """Subscription descriptor targeting a single talkgroup."""

    system_id: int
    talkgroup_id: int


@dataclass(frozen=True, slots=True)
class PlaylistSubscription:
    """Subscription descriptor targeting a playlist feed."""

    playlist_id: PlaylistId


Subscription = TalkgroupSubscription | PlaylistSubscription


@dataclass(slots=True)
class LiveSubscriptionHandle:
    """Public handle for a live subscription."""

    id: str
    topic: str
    subscription: Subscription
    _stop: Callable[[str], Awaitable[None]] = field(repr=False)
    _snapshot: Callable[[str], ProducerRuntimeState] = field(repr=False)

    async def close(self) -> None:
        """Stop the underlying producer."""
        await self._stop(self.id)

    def snapshot(self) -> ProducerRuntimeState:
        """Return the latest runtime snapshot for this subscription."""
        return self._snapshot(self.id)


class PlaylistCatalog(Protocol):
    """Protocol describing playlist discovery and metadata APIs."""

    async def list_playlists(self) -> Sequence[PlaylistDescriptor]:  # pragma: no cover - protocol
        """Return all playlists visible to the current Broadcastify account."""
        ...

    async def describe_playlist(
        self, playlist_id: PlaylistId
    ) -> PlaylistSubscriptionState:  # pragma: no cover - protocol
        """Return subscription metadata for the given `playlist_id`."""
        ...

    async def search_playlists(
        self, query: str, *, limit: int
    ) -> SearchResultPage[PlaylistDescriptor]:  # pragma: no cover - protocol
        """Search playlists by `query`, returning at most `limit` results."""
        ...


class DiscoveryService(Protocol):
    """Protocol describing search and resolution APIs for systems and talkgroups."""

    async def search_systems(
        self, query: str, *, limit: int
    ) -> SearchResultPage[SystemSummary]:  # pragma: no cover - protocol
        """Return system summaries matching `query` constrained by `limit`."""
        ...

    async def search_talkgroups(
        self, query: str, *, limit: int
    ) -> SearchResultPage[TalkgroupSummary]:  # pragma: no cover - protocol
        """Return talkgroups matching `query` constrained by `limit`."""
        ...

    async def resolve_talkgroup(
        self, name: str, region: str
    ) -> TalkgroupSummary | None:  # pragma: no cover - protocol
        """Resolve a talkgroup for `name` within `region`, if available."""
        ...


class AsyncBroadcastifyClient(Protocol):
    """Public async-facing protocol for Broadcastify operations."""

    async def authenticate(
        self, credentials: Credentials | SessionToken
    ) -> SessionToken:  # pragma: no cover - protocol
        """Authenticate using explicit credentials or an existing session token."""
        ...

    async def logout(self) -> None:  # pragma: no cover - protocol
        """Invalidate any active session for this client."""
        ...

    async def get_archived_calls(
        self, system_id: int, talkgroup_id: int, time_block: int
    ) -> ArchiveResult:  # pragma: no cover - protocol
        """Fetch archived calls for the specified identifiers and time block."""
        ...

    async def create_live_producer(
        self, system_id: int, talkgroup_id: int, *, position: float | None = None
    ) -> LiveSubscriptionHandle:  # pragma: no cover - protocol
        """Start a talkgroup live producer and return the tracking handle."""
        ...

    async def create_playlist_producer(
        self, playlist_id: PlaylistId, *, position: float | None = None
    ) -> LiveSubscriptionHandle:  # pragma: no cover - protocol
        """Start a playlist live producer and return the tracking handle."""
        ...

    async def register_consumer(
        self, topic: str, callback: ConsumerCallback
    ) -> None:  # pragma: no cover - protocol
        """Subscribe `callback` to events published on `topic`."""
        ...

    async def unregister_consumer(
        self, topic: str, callback: ConsumerCallback
    ) -> None:  # pragma: no cover - protocol
        """Remove `callback` subscription from `topic` if registered."""
        ...

    async def stop_producer(
        self, handle: LiveSubscriptionHandle
    ) -> None:  # pragma: no cover - protocol
        """Stop the producer identified by `handle`."""
        ...

    async def list_topics(self) -> Mapping[str, int]:  # pragma: no cover - protocol
        """Return known event topics and their subscriber counts."""
        ...

    async def get_runtime_metrics(self) -> RuntimeMetrics:  # pragma: no cover - protocol
        """Return current client runtime metrics."""
        ...

    async def start(self) -> None:  # pragma: no cover - protocol
        """Initialise background tasks required for the client."""
        ...

    async def shutdown(self) -> None:  # pragma: no cover - protocol
        """Stop background tasks and release held resources."""
        ...

    async def list_playlists(self) -> Sequence[PlaylistDescriptor]:  # pragma: no cover - protocol
        """Return all playlists visible to the current Broadcastify account."""
        ...

    async def describe_playlist(
        self, playlist_id: PlaylistId
    ) -> PlaylistSubscriptionState:  # pragma: no cover - protocol
        """Return subscription metadata for the given `playlist_id`."""
        ...

    async def search_playlists(
        self, query: str, *, limit: int = 25
    ) -> SearchResultPage[PlaylistDescriptor]:  # pragma: no cover - protocol
        """Search playlists by `query`, returning at most `limit` results."""
        ...

    async def search_systems(
        self, query: str, *, limit: int = 25
    ) -> SearchResultPage[SystemSummary]:  # pragma: no cover - protocol
        """Return system summaries matching `query` constrained by `limit`."""
        ...

    async def search_talkgroups(
        self, query: str, *, limit: int = 25
    ) -> SearchResultPage[TalkgroupSummary]:  # pragma: no cover - protocol
        """Return talkgroups matching `query` constrained by `limit`."""
        ...

    async def resolve_talkgroup(
        self, name: str, region: str
    ) -> TalkgroupSummary | None:  # pragma: no cover - protocol
        """Resolve a talkgroup for `name` within `region`, if available."""
        ...


class CallPollerFactory(Protocol):
    """Factory responsible for creating call pollers."""

    def create(
        self,
        subscription: Subscription,
        *,
        http_client: AsyncHttpClientProtocol,
        telemetry: TelemetrySink,
    ) -> CallPoller:  # pragma: no cover - factory protocol
        """Construct a call poller bound to `subscription` using provided dependencies."""
        ...


def _create_task_set() -> set[asyncio.Task[None]]:
    """Return a new empty set for tracking asyncio tasks."""
    return set()


@dataclass
class ProducerHandle:
    """Tracks an active producer, its subscription, and execution tasks."""

    id: str
    producer: LiveCallProducer
    subscription: Subscription
    topic: str
    task: asyncio.Task[None] | None = None
    dispatch_task: asyncio.Task[None] | None = None
    audio_consumer: AudioConsumer | None = None
    audio_queue: asyncio.Queue[AudioChunkEvent] | None = None
    audio_dispatch_task: asyncio.Task[None] | None = None
    audio_tasks: set[asyncio.Task[None]] = field(default_factory=_create_task_set)


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
    audio_consumer_factory: Callable[[], AudioConsumer] | None = None
    transcription_config: TranscriptionConfig | None = None
    playlist_catalog: PlaylistCatalog | None = None
    discovery_client: DiscoveryService | None = None


class BroadcastifyClient(AsyncBroadcastifyClient):
    """Concrete async Broadcastify client coordinating authentication and producers."""

    def __init__(
        self,
        *,
        dependencies: BroadcastifyClientDependencies | None = None,
    ) -> None:
        """Wire optional dependency overrides and prepare internal state."""
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
        self._audio_consumer_factory = deps.audio_consumer_factory
        self._playlist_catalog = deps.playlist_catalog
        self._discovery_client = deps.discovery_client
        self._transcription_config = deps.transcription_config or TranscriptionConfig()
        self._transcription_pipeline: TranscriptionPipeline | None = None
        self._producer_handles: dict[str, ProducerHandle] = {}
        self._handles_lock = asyncio.Lock()
        self._started = False
        self._live_topic = "calls.live"
        self._audio_topic = "calls.audio"
        self._transcript_partial_topic = "transcription.partial"
        self._transcript_final_topic = "transcription.complete"
        self._transcription_buffers: dict[CallId, list[AudioChunkEvent]] = {}
        logger.debug("BroadcastifyClient initialised")

    async def authenticate(self, credentials: Credentials | SessionToken) -> SessionToken:
        """Authenticate against Broadcastify and return a validated session token."""
        return await self._authenticator.authenticate(credentials)

    async def logout(self) -> None:
        """Invalidate the active Broadcastify session if one exists."""
        await self._authenticator.logout()

    async def get_archived_calls(
        self, system_id: int, talkgroup_id: int, time_block: int
    ) -> ArchiveResult:
        """Retrieve archived calls for the given system, talkgroup, and archive block."""
        return await self._archive_client.get_archived_calls(system_id, talkgroup_id, time_block)

    async def create_live_producer(
        self,
        system_id: int,
        talkgroup_id: int,
        *,
        position: float | None = None,
        initial_history: int | None = None,
    ) -> LiveSubscriptionHandle:
        """Create a talkgroup-based live producer and return a handle for managing it."""
        subscription = TalkgroupSubscription(system_id=system_id, talkgroup_id=talkgroup_id)
        return await self._register_subscription(
            subscription, position=position, initial_history=initial_history
        )

    async def create_playlist_producer(
        self,
        playlist_id: PlaylistId,
        *,
        position: float | None = None,
        initial_history: int | None = None,
    ) -> LiveSubscriptionHandle:
        """Create a playlist-driven live producer and return a handle for managing it."""
        subscription = PlaylistSubscription(playlist_id=playlist_id)
        return await self._register_subscription(
            subscription, position=position, initial_history=initial_history
        )

    async def register_consumer(self, topic: str, callback: ConsumerCallback) -> None:
        """Subscribe *callback* to receive events published on *topic*."""
        await self._event_bus.subscribe(topic, callback)
        logger.debug("Registered consumer for topic %s", topic)

    async def unregister_consumer(self, topic: str, callback: ConsumerCallback) -> None:
        """Unsubscribe *callback* from *topic* if it was previously registered."""
        await self._event_bus.unsubscribe(topic, callback)
        logger.debug("Unregistered consumer for topic %s", topic)

    async def stop_producer(self, handle: LiveSubscriptionHandle) -> None:
        """Stop the live producer represented by *handle* and release its resources."""
        await self._stop_producer_by_id(handle.id)

    async def list_topics(self) -> Mapping[str, int]:
        """Return a mapping of event bus topics to their subscriber counts."""
        return await self._event_bus.topics()

    async def get_runtime_metrics(self) -> RuntimeMetrics:
        """Return a snapshot of runtime metrics for all active producers and consumers."""
        async with self._handles_lock:
            snapshots = [handle.producer.snapshot() for handle in self._producer_handles.values()]
            consumer_topics = await self._event_bus.topics()
        return RuntimeMetrics(
            generated_at=datetime.now(UTC),
            producers=snapshots,
            consumer_topics=len(consumer_topics),
        )

    async def start(self) -> None:
        """Start all registered live producers if the client is not already running."""
        async with self._handles_lock:
            if self._started:
                return
            # Lazily initialise transcription pipeline if enabled
            if self._transcription_config.enabled and self._transcription_pipeline is None:
                if self._transcription_config.provider == "openai":
                    backend = OpenAIWhisperBackend(self._transcription_config)
                else:
                    raise NotImplementedError("Only 'openai' transcription provider is implemented")
                self._transcription_pipeline = TranscriptionPipeline(
                    backend, telemetry=self._telemetry
                )
            for handle in self._producer_handles.values():
                if handle.task is None:
                    self._start_producer(handle)
            self._started = True
        logger.info("BroadcastifyClient started %d producer(s)", len(self._producer_handles))

    async def shutdown(self) -> None:
        """Stop all producers, cancel background tasks, and close the HTTP client."""
        async with self._handles_lock:
            handles = list(self._producer_handles.values())
            self._producer_handles.clear()
            self._started = False
        for handle in handles:
            await handle.producer.stop()
        for handle in handles:
            self._cancel_handle_tasks(handle)
        for handle in handles:
            await self._await_handle_tasks(handle)
        await self._http_client.close()
        logger.info("BroadcastifyClient shutdown complete")

    async def list_playlists(self) -> Sequence[PlaylistDescriptor]:
        """Return playlist descriptors from the configured catalog provider."""
        if self._playlist_catalog is None:
            raise NotImplementedError("Playlist catalog integration not configured")
        return await self._playlist_catalog.list_playlists()

    async def describe_playlist(self, playlist_id: PlaylistId) -> PlaylistSubscriptionState:
        """Return playlist membership and cursor information for *playlist_id*."""
        if self._playlist_catalog is None:
            raise NotImplementedError("Playlist catalog integration not configured")
        return await self._playlist_catalog.describe_playlist(playlist_id)

    async def search_playlists(
        self, query: str, *, limit: int = 25
    ) -> SearchResultPage[PlaylistDescriptor]:
        """Search playlists matching *query*, returning a paginated result set."""
        if self._playlist_catalog is None:
            raise NotImplementedError("Playlist catalog integration not configured")
        return await self._playlist_catalog.search_playlists(query, limit=limit)

    async def search_systems(
        self, query: str, *, limit: int = 25
    ) -> SearchResultPage[SystemSummary]:
        """Search systems matching *query*, returning at most *limit* entries."""
        if self._discovery_client is None:
            raise NotImplementedError("Discovery integration not configured")
        return await self._discovery_client.search_systems(query, limit=limit)

    async def search_talkgroups(
        self, query: str, *, limit: int = 25
    ) -> SearchResultPage[TalkgroupSummary]:
        """Search talkgroups matching *query*, returning at most *limit* entries."""
        if self._discovery_client is None:
            raise NotImplementedError("Discovery integration not configured")
        return await self._discovery_client.search_talkgroups(query, limit=limit)

    async def resolve_talkgroup(self, name: str, region: str) -> TalkgroupSummary | None:
        """Resolve a talkgroup by *name* and *region*, or ``None`` when no match exists."""
        if self._discovery_client is None:
            raise NotImplementedError("Discovery integration not configured")
        return await self._discovery_client.resolve_talkgroup(name, region)

    async def _register_subscription(
        self,
        subscription: Subscription,
        *,
        position: float | None,
        initial_history: int | None = None,
    ) -> LiveSubscriptionHandle:
        handle_id = uuid4().hex
        topic = self._topic_for_subscription(subscription)
        config = LiveProducerConfig(
            initial_position=position,
            initial_history=None if initial_history is None else max(0, int(initial_history)),
        )
        poller = self._call_poller_factory.create(
            subscription,
            http_client=self._http_client,
            telemetry=self._telemetry,
        )
        queue: asyncio.Queue[LiveCallEnvelope] = asyncio.Queue(maxsize=config.queue_maxsize or 0)
        producer = LiveCallProducer(
            poller,
            config,
            topic=topic,
            telemetry=self._telemetry,
            queue=queue,
        )
        audio_consumer = self._audio_consumer_factory() if self._audio_consumer_factory else None
        audio_queue: asyncio.Queue[AudioChunkEvent] | None = None
        if audio_consumer is not None:
            audio_queue = asyncio.Queue(maxsize=config.queue_maxsize or 0)
        elif self._transcription_config.enabled:
            # Default to HTTP downloader when transcription is enabled and no override provided
            downloader = HttpAudioDownloader(self._http_client, self._authenticator)
            audio_consumer = AudioConsumer(downloader, telemetry=self._telemetry)
            audio_queue = asyncio.Queue(maxsize=config.queue_maxsize or 0)
        handle = ProducerHandle(
            id=handle_id,
            producer=producer,
            subscription=subscription,
            topic=topic,
            audio_consumer=audio_consumer,
            audio_queue=audio_queue,
        )
        async with self._handles_lock:
            self._producer_handles[handle_id] = handle
            if self._started:
                self._start_producer(handle)
        logger.info("Registered producer %s for topic %s", handle_id, topic)
        return self._to_public_handle(handle)

    def _start_producer(self, handle: ProducerHandle) -> None:
        handle.task = asyncio.create_task(handle.producer.run())
        handle.dispatch_task = asyncio.create_task(self._dispatch_events(handle))
        if handle.audio_queue is not None and handle.audio_consumer is not None:
            handle.audio_dispatch_task = asyncio.create_task(self._dispatch_audio_chunks(handle))
        logger.debug("Started producer tasks for %s", handle.topic)

    async def _stop_producer_by_id(self, handle_id: str) -> None:
        async with self._handles_lock:
            handle = self._producer_handles.pop(handle_id, None)
        if handle is None:
            return
        await handle.producer.stop()
        self._cancel_handle_tasks(handle)
        await self._await_handle_tasks(handle)
        logger.info("Stopped producer %s", handle_id)

    def _to_public_handle(self, handle: ProducerHandle) -> LiveSubscriptionHandle:
        return LiveSubscriptionHandle(
            id=handle.id,
            topic=handle.topic,
            subscription=handle.subscription,
            _stop=self._stop_producer_by_id,
            _snapshot=self._snapshot_handle,
        )

    def _snapshot_handle(self, handle_id: str) -> ProducerRuntimeState:
        handle = self._producer_handles.get(handle_id)
        if handle is None:
            raise KeyError(f"Unknown handle id {handle_id}")
        return handle.producer.snapshot()

    async def _dispatch_events(self, handle: ProducerHandle) -> None:
        queue = handle.producer.queue
        try:
            while True:
                event = await queue.get()
                try:
                    await self._event_bus.publish(self._live_topic, event)
                    await self._event_bus.publish(handle.topic, event)
                    call_topic = self._call_topic(event.call.system_id, event.call.talkgroup_id)
                    if call_topic != handle.topic:
                        await self._event_bus.publish(call_topic, event)
                    if handle.audio_consumer is not None and handle.audio_queue is not None:
                        audio_task = asyncio.create_task(self._consume_audio(handle, event))

                        def _remove(task: asyncio.Task[None]) -> None:
                            handle.audio_tasks.discard(task)

                        handle.audio_tasks.add(audio_task)
                        audio_task.add_done_callback(_remove)
                except Exception as exc:
                    logger.exception("Failed to publish live event for topic %s", handle.topic)
                    self._telemetry.record_event(
                        LiveDispatchErrorEvent(
                            topic=handle.topic,
                            error_type=exc.__class__.__name__,
                            message=str(exc),
                        )
                    )
                finally:
                    queue.task_done()
        except asyncio.CancelledError:
            return

    async def _consume_audio(self, handle: ProducerHandle, event: LiveCallEnvelope) -> None:
        assert handle.audio_consumer is not None
        assert handle.audio_queue is not None
        try:
            await handle.audio_consumer.consume(event, handle.audio_queue)
        except AudioDownloadError as exc:
            logger.warning(
                "Audio download failed for call %s (system %s, talkgroup %s): %s",
                event.call.call_id,
                event.call.system_id,
                event.call.talkgroup_id,
                exc,
            )
            self._telemetry.record_event(
                AudioErrorEvent(
                    call_id=event.call.call_id,
                    system_id=event.call.system_id,
                    talkgroup_id=event.call.talkgroup_id,
                    error_type=exc.__class__.__name__,
                    message=str(exc),
                )
            )
        except Exception as exc:  # pragma: no cover - defensive path
            logger.exception(
                "Unexpected failure consuming audio for call %s (system %s, talkgroup %s)",
                event.call.call_id,
                event.call.system_id,
                event.call.talkgroup_id,
            )
            self._telemetry.record_event(
                AudioErrorEvent(
                    call_id=event.call.call_id,
                    system_id=event.call.system_id,
                    talkgroup_id=event.call.talkgroup_id,
                    error_type=exc.__class__.__name__,
                    message=str(exc),
                )
            )

    async def _dispatch_audio_chunks(self, handle: ProducerHandle) -> None:
        assert handle.audio_queue is not None
        queue = handle.audio_queue
        try:
            while True:
                chunk = await queue.get()
                try:
                    await self._event_bus.publish(self._audio_topic, chunk)
                    await self._event_bus.publish(f"{self._audio_topic}.{chunk.call_id}", chunk)
                    # Fan-out to transcription pipeline when enabled
                    if (
                        self._transcription_pipeline is not None
                        and self._transcription_config.emit_partial_results
                    ):

                        async def _yield_one(c: AudioChunkEvent) -> AsyncIterator[AudioChunkEvent]:
                            yield c

                        async for partial in self._transcription_pipeline.transcribe_stream(
                            _yield_one(chunk)
                        ):
                            await self._event_bus.publish(self._transcript_partial_topic, partial)

                    # Buffer chunks and publish final transcript when finished
                    if self._transcription_pipeline is not None:
                        buf = self._transcription_buffers.setdefault(chunk.call_id, [])
                        buf.append(chunk)
                        if chunk.finished:
                            call_id: CallId = chunk.call_id

                            pipeline = self._transcription_pipeline
                            assert pipeline is not None

                            async def _finalize(call: CallId, p: TranscriptionPipeline) -> None:
                                chunks = self._transcription_buffers.pop(call, [])

                                async def _iter() -> AsyncIterator[AudioChunkEvent]:
                                    for item in chunks:
                                        yield item

                                try:
                                    result = await p.transcribe_final(_iter())
                                    await self._event_bus.publish(
                                        self._transcript_final_topic, result
                                    )
                                except Exception:
                                    logger.exception("Final transcription failed for call %s", call)

                            finalize_task = asyncio.create_task(_finalize(call_id, pipeline))

                            def _remove(task: asyncio.Task[None]) -> None:
                                handle.audio_tasks.discard(task)

                            handle.audio_tasks.add(finalize_task)
                            finalize_task.add_done_callback(_remove)
                except Exception as exc:
                    logger.exception("Failed to dispatch audio chunk for call %s", chunk.call_id)
                    self._telemetry.record_event(
                        AudioErrorEvent(
                            call_id=chunk.call_id,
                            system_id=-1,
                            talkgroup_id=-1,
                            error_type=exc.__class__.__name__,
                            message=str(exc),
                        )
                    )
                finally:
                    queue.task_done()
        except asyncio.CancelledError:
            return

    def _cancel_handle_tasks(self, handle: ProducerHandle) -> None:
        for task in (handle.task, handle.dispatch_task, handle.audio_dispatch_task):
            if task is not None:
                task.cancel()
        for task in list(handle.audio_tasks):
            task.cancel()

    async def _await_handle_tasks(self, handle: ProducerHandle) -> None:
        for task in (handle.task, handle.dispatch_task, handle.audio_dispatch_task):
            await self._await_optional_task(task)
        for task in list(handle.audio_tasks):
            await self._await_optional_task(task)

    async def _await_optional_task(self, task: asyncio.Task[None] | None) -> None:
        if task is None:
            return
        try:
            await task
        except asyncio.CancelledError:
            pass

    def _topic_for_subscription(self, subscription: Subscription) -> str:
        if isinstance(subscription, TalkgroupSubscription):
            return f"{self._live_topic}.{subscription.system_id}.{subscription.talkgroup_id}"
        return f"{self._live_topic}.playlist.{subscription.playlist_id}"

    def _call_topic(self, system_id: int, talkgroup_id: int) -> str:
        return f"{self._live_topic}.{system_id}.{talkgroup_id}"


class _DefaultCallPollerFactory(CallPollerFactory):
    """Factory creating HTTP-backed call pollers."""

    def create(
        self,
        subscription: Subscription,
        *,
        http_client: AsyncHttpClientProtocol,
        telemetry: TelemetrySink,
    ) -> CallPoller:
        return _HttpCallPoller(subscription, http_client, telemetry)


class _HttpCallPoller(CallPoller):
    """HTTP-backed poller that surfaces Broadcastify live call events."""

    _LIVE_ENDPOINT = "/calls/apis/live-calls"

    def __init__(
        self,
        subscription: Subscription,
        http_client: AsyncHttpClientProtocol,
        telemetry: TelemetrySink,
    ) -> None:
        self._subscription = subscription
        self._http_client = http_client
        self._telemetry = telemetry
        self._session_key = _generate_session_key()
        self._initialised = False

    async def fetch(
        self, *, cursor: float | None
    ) -> tuple[Iterable[LiveCallEnvelope], float | None]:
        position = float(cursor) if cursor is not None else 0.0
        form_payload = self._build_payload(position)
        response = await self._http_client.post_form(self._LIVE_ENDPOINT, data=form_payload)
        try:
            payload = response.json()
        except ValueError as exc:  # pragma: no cover - defensive path
            raise LiveSessionError("Live call payload was not valid JSON") from exc

        envelope = LiveCallsResponse.model_validate(payload)
        if envelope.session_key:
            self._session_key = envelope.session_key

        events = [self._to_call_event(entry, envelope.last_pos) for entry in envelope.calls]
        next_cursor = self._calculate_cursor(envelope, events, position)
        self._initialised = True
        self._telemetry.record_metric(
            PollMetrics(
                topic="poller",
                dispatched=len(events),
                queue_depth=0,
            )
        )
        return events, next_cursor

    def _build_payload(self, position: float) -> dict[str, str]:
        payload = {
            "pos": f"{position:.3f}",
            "doInit": "1" if not self._initialised else "0",
            "sessionKey": self._session_key,
        }
        if isinstance(self._subscription, TalkgroupSubscription):
            payload["groups[]"] = (
                f"{self._subscription.system_id}-{self._subscription.talkgroup_id}"
            )
            payload["systemId"] = str(self._subscription.system_id)
        else:
            payload["playlist_uuid"] = self._subscription.playlist_id
            payload["systemId"] = "0"
        payload.setdefault("sid", "0")
        return payload

    def _calculate_cursor(
        self,
        envelope: LiveCallsResponse,
        events: list[LiveCallEnvelope],
        current: float,
    ) -> float | None:
        candidates: list[float] = [float(envelope.last_pos), current]
        for event in events:
            if event.cursor is not None:
                candidates.append(float(event.cursor))
            candidates.append(event.call.received_at.timestamp() + 1.0)
        return max(candidates) if candidates else current

    def _to_call_event(self, entry: LiveCallEntry, default_cursor: float) -> LiveCallEnvelope:
        call = _parse_live_call(entry)
        cursor = entry.pos if entry.pos is not None else default_cursor
        now = datetime.now(UTC)
        raw_payload = MappingProxyType(entry.model_dump(by_alias=True))
        return LiveCallEnvelope(
            call=call,
            cursor=cursor,
            received_at=now,
            shard_key=(call.system_id, call.talkgroup_id),
            raw_payload=raw_payload,
        )


def _generate_session_key() -> str:
    return secrets.token_hex(16)


def _parse_live_call(entry: LiveCallEntry) -> Call:
    metadata = parse_call_metadata(entry.metadata)
    metadata_payload = entry.metadata

    system_name = _coerce_str(
        entry.grouping,
        metadata_payload.get("talkgroup_group"),
        metadata_payload.get("short_name"),
    )
    talkgroup_label = _coerce_str(
        entry.display,
        metadata_payload.get("talkgroup_tag"),
        metadata_payload.get("talkgroup"),
    )
    talkgroup_description = _coerce_str(
        entry.descr,
        metadata_payload.get("talkgroup_description"),
    )
    duration_seconds = _coerce_float(
        entry.call_duration,
        metadata_payload.get("call_length"),
    )
    source = _extract_source_descriptor(entry)

    return Call(
        call_id=entry.id,
        system_id=entry.system_id,
        system_name=system_name,
        talkgroup_id=entry.call_tg,
        talkgroup_label=talkgroup_label,
        talkgroup_description=talkgroup_description,
        received_at=datetime.fromtimestamp(entry.ts, UTC),
        frequency_mhz=entry.call_freq,
        duration_seconds=duration_seconds,
        source=source,
        metadata=metadata,
        ttl_seconds=entry.call_ttl,
    )


def _extract_source_descriptor(entry: LiveCallEntry) -> SourceDescriptor:
    identifier = entry.call_src
    label = entry.call_src_descr

    # Metadata "srcList" is provider-defined; treat as a sequence of mappings.
    metadata_sources_obj = entry.metadata.get("srcList")
    if isinstance(metadata_sources_obj, Sequence) and metadata_sources_obj:
        seq: Sequence[object] = cast(Sequence[object], metadata_sources_obj)
        candidate_first: object = seq[0]
        if isinstance(candidate_first, Mapping):
            first: Mapping[str, object] = cast(Mapping[str, object], candidate_first)
            if identifier is None:
                raw_identifier_obj = first.get("src")
                try:
                    if isinstance(raw_identifier_obj, (int, str)):
                        identifier = int(raw_identifier_obj)
                    else:
                        identifier = None
                except (TypeError, ValueError):
                    identifier = None
            if label in (None, ""):
                label_candidate_obj = first.get("tag")
                coerced = _coerce_str(label_candidate_obj)
                if coerced is not None:
                    label = coerced

    if label in (None, ""):
        extra_label_obj = cast(object | None, entry.metadata.get("call_src_descr"))
        coerced = _coerce_str(extra_label_obj)
        if coerced is not None:
            label = coerced

    return SourceDescriptor(identifier=identifier, label=_coerce_str(label))


def _coerce_str(*values: object | None) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _coerce_float(*values: object | None) -> float | None:
    for value in values:
        if value is None:
            continue
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(str(value))
        except (TypeError, ValueError):
            continue
    return None
