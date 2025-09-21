"""Public async client facade for interacting with Broadcastify."""

from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, Protocol, cast

from .archives import ArchiveClient, ArchiveParser, JsonArchiveParser
from .audio_consumer import AudioConsumer
from .auth import AuthenticationBackend, Authenticator, HttpAuthenticationBackend
from .config import Credentials, HttpClientConfig, LiveProducerConfig
from .errors import AudioDownloadError, LiveSessionError
from .eventbus import ConsumerCallback, EventBus
from .http import AsyncHttpClientProtocol, BroadcastifyHttpClient
from .live_producer import CallPoller, LiveCallProducer
from .models import ArchiveResult, AudioChunkEvent, Call, CallEvent, SessionToken
from .telemetry import NullTelemetrySink, TelemetrySink

logger = logging.getLogger(__name__)


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
        self, system_id: int, talkgroup_id: int, *, position: float | None = None
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


def _create_task_set() -> set[asyncio.Task[None]]:
    """Return a new empty set for tracking asyncio tasks."""

    return set()


@dataclass
class ProducerHandle:
    """Tracks the association between a producer, its queue, and execution tasks."""

    producer: LiveCallProducer
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


class _HttpCallPoller(CallPoller):
    """HTTP-backed poller that surfaces Broadcastify live call events."""

    _LIVE_ENDPOINT = "/calls/apis/live-calls"

    def __init__(
        self,
        system_id: int,
        talkgroup_id: int,
        http_client: AsyncHttpClientProtocol,
        telemetry: TelemetrySink,
    ) -> None:
        """Create a poller for *system_id* and *talkgroup_id*."""

        self._system_id = system_id
        self._talkgroup_id = talkgroup_id
        self._http_client = http_client
        self._telemetry = telemetry
        self._session_key = _generate_session_key()
        self._initialised = False

    async def fetch(self, *, cursor: float | None) -> tuple[list[CallEvent], float | None]:
        """Return new call events and the updated cursor."""

        position = float(cursor) if cursor is not None else 0.0
        form_payload = {
            "groups[]": f"{self._system_id}-{self._talkgroup_id}",
            "pos": f"{position:.3f}",
            "doInit": "1" if not self._initialised else "0",
            "systemId": "0",
            "sid": "0",
            "sessionKey": self._session_key,
        }

        response = await self._http_client.post_form(self._LIVE_ENDPOINT, data=form_payload)

        try:
            payload = cast(dict[str, Any], response.json())
        except ValueError as exc:  # pragma: no cover - defensive path
            raise LiveSessionError("Live call payload was not valid JSON") from exc

        session_key = payload.get("sessionKey")
        if isinstance(session_key, str) and session_key:
            self._session_key = session_key

        events = self._parse_events(payload)
        next_cursor = self._calculate_cursor(payload, events, position)
        self._initialised = True

        self._telemetry.record_metric("live_producer.poll.events", float(len(events)))
        return events, next_cursor

    def _parse_events(self, payload: Mapping[str, Any]) -> list[CallEvent]:
        calls_payload = payload.get("calls", [])
        if not isinstance(calls_payload, list):
            raise LiveSessionError("Live call payload missing 'calls' array")

        typed_calls = cast(list[Mapping[str, Any]], calls_payload)
        events: list[CallEvent] = []
        now = datetime.now(UTC)
        for entry in typed_calls:
            call = _parse_live_call(entry, self._system_id, self._talkgroup_id)
            cursor = _coerce_optional_float(entry.get("pos") or entry.get("position"))
            events.append(
                CallEvent(
                    call=call,
                    cursor=cursor,
                    received_at=now,
                    shard_key=(self._system_id, self._talkgroup_id),
                    raw_payload=MappingProxyType({str(key): value for key, value in entry.items()}),
                )
            )
        return events

    def _calculate_cursor(
        self,
        payload: Mapping[str, Any],
        events: list[CallEvent],
        current: float,
    ) -> float | None:
        candidates: list[float] = []
        for event in events:
            if event.cursor is not None:
                candidates.append(float(event.cursor))
            candidates.append(event.call.received_at.timestamp() + 1.0)

        last_pos = _coerce_optional_float(payload.get("lastPos") or payload.get("lastpos"))
        if last_pos is not None:
            candidates.append(last_pos)

        candidates.append(current)

        if not candidates:
            return current
        return max(candidates)


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
        self._audio_consumer_factory = deps.audio_consumer_factory
        self._producer_handles: list[ProducerHandle] = []
        self._started = False
        self._live_topic = "calls.live"
        self._audio_topic = "calls.audio"
        logger.debug(
            "BroadcastifyClient initialised with %d pre-registered producers",
            len(self._producer_handles),
        )

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

        return await self._archive_client.get_archived_calls(system_id, talkgroup_id, time_block)

    async def create_live_producer(
        self, system_id: int, talkgroup_id: int, *, position: float | None = None
    ) -> LiveCallProducer:
        """Create and register a live call producer."""

        config = LiveProducerConfig(initial_position=position)
        poller = self._call_poller_factory.create(
            system_id,
            talkgroup_id,
            http_client=self._http_client,
            telemetry=self._telemetry,
        )
        queue: asyncio.Queue[CallEvent] = asyncio.Queue(maxsize=config.queue_maxsize or 0)
        producer = LiveCallProducer(poller, config, telemetry=self._telemetry, queue=queue)
        topic = f"{self._live_topic}.{system_id}.{talkgroup_id}"
        audio_consumer = self._audio_consumer_factory() if self._audio_consumer_factory else None
        audio_queue: asyncio.Queue[AudioChunkEvent] | None = None
        if audio_consumer is not None:
            audio_queue = asyncio.Queue(maxsize=config.queue_maxsize or 0)
        handle = ProducerHandle(
            producer=producer,
            topic=topic,
            audio_consumer=audio_consumer,
            audio_queue=audio_queue,
        )
        self._producer_handles.append(handle)
        if self._started:
            self._start_producer(handle)
        logger.info(
            "Registered live producer for system %s talkgroup %s (position=%s)",
            system_id,
            talkgroup_id,
            position,
        )
        return producer

    async def register_consumer(self, topic: str, callback: ConsumerCallback) -> None:
        """Register a consumer callback via the event bus."""

        await self._event_bus.subscribe(topic, callback)
        logger.debug("Registered consumer for topic %s", topic)

    async def start(self) -> None:
        """Start all managed live call producers."""

        if self._started:
            return
        for handle in self._producer_handles:
            if handle.task is None:
                self._start_producer(handle)
        self._started = True
        logger.info("BroadcastifyClient started %d producer(s)", len(self._producer_handles))

    async def shutdown(self) -> None:
        """Stop producers and release HTTP resources."""

        for handle in self._producer_handles:
            await handle.producer.stop()
        for handle in self._producer_handles:
            self._cancel_handle_tasks(handle)
        for handle in self._producer_handles:
            await self._await_handle_tasks(handle)
        await self._http_client.close()
        self._producer_handles.clear()
        self._started = False
        logger.info("BroadcastifyClient shutdown complete")

    def _start_producer(self, handle: ProducerHandle) -> None:
        """Start the asynchronous task responsible for running *handle*'s producer."""

        handle.task = asyncio.create_task(handle.producer.run())
        handle.dispatch_task = asyncio.create_task(self._dispatch_events(handle))
        if handle.audio_queue is not None and handle.audio_consumer is not None:
            handle.audio_dispatch_task = asyncio.create_task(self._dispatch_audio_chunks(handle))
        logger.debug("Started producer tasks for topic %s", handle.topic)

    async def _dispatch_events(self, handle: ProducerHandle) -> None:
        """Drain events from a producer queue and publish them to event bus topics."""

        queue = handle.producer.queue
        try:
            while True:
                event = await queue.get()
                try:
                    await self._event_bus.publish(self._live_topic, event)
                    await self._event_bus.publish(handle.topic, event)
                    if handle.audio_consumer is not None and handle.audio_queue is not None:
                        audio_task = asyncio.create_task(self._consume_audio(handle, event))

                        def _remove(task: asyncio.Task[None]) -> None:
                            handle.audio_tasks.discard(task)

                        handle.audio_tasks.add(audio_task)
                        audio_task.add_done_callback(_remove)
                except Exception as exc:
                    logger.exception("Failed to publish live event for topic %s", handle.topic)
                    self._telemetry.record_event(
                        "live_producer.dispatch.error",
                        attributes={"error_type": exc.__class__.__name__},
                    )
                finally:
                    queue.task_done()
        except asyncio.CancelledError:
            return

    async def _consume_audio(self, handle: ProducerHandle, event: CallEvent) -> None:
        """Trigger audio download for *event* via the configured consumer."""

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
                "audio_consumer.error",
                attributes={
                    "error_type": exc.__class__.__name__,
                    "call_id": event.call.call_id,
                },
            )
        except Exception as exc:
            logger.exception(
                "Unexpected failure consuming audio for call %s (system %s, talkgroup %s)",
                event.call.call_id,
                event.call.system_id,
                event.call.talkgroup_id,
            )
            self._telemetry.record_event(
                "audio_consumer.error",
                attributes={
                    "error_type": exc.__class__.__name__,
                    "call_id": event.call.call_id,
                },
            )

    async def _dispatch_audio_chunks(self, handle: ProducerHandle) -> None:
        """Publish audio chunks produced for a given handle."""

        assert handle.audio_queue is not None
        queue = handle.audio_queue
        try:
            while True:
                chunk = await queue.get()
                try:
                    await self._event_bus.publish(self._audio_topic, chunk)
                    await self._event_bus.publish(f"{self._audio_topic}.{chunk.call_id}", chunk)
                except Exception as exc:
                    logger.exception(
                        "Failed to dispatch audio chunk for call %s", chunk.call_id
                    )
                    self._telemetry.record_event(
                        "audio_dispatch.error",
                        attributes={"error_type": exc.__class__.__name__},
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


def _generate_session_key() -> str:
    """Return a random session key accepted by the Broadcastify endpoint."""

    return secrets.token_hex(16)


def _parse_live_call(
    entry: Mapping[str, Any],
    fallback_system_id: int,
    fallback_talkgroup_id: int,
) -> Call:
    call_id = _coerce_int(entry.get("id") or entry.get("callId"), "id")
    system_id = _coerce_optional_int(entry.get("system_id") or entry.get("systemId"))
    talkgroup_id = _coerce_optional_int(entry.get("talkgroup_id") or entry.get("talkgroupId"))

    resolved_system_id = system_id if system_id is not None else fallback_system_id
    resolved_talkgroup_id = talkgroup_id if talkgroup_id is not None else fallback_talkgroup_id

    received_at_value: Any = entry.get("received_at")
    if received_at_value is None:
        received_at_value = entry.get("start_time")
    if received_at_value is None:
        received_at_value = entry.get("start")
    if received_at_value is None:
        received_at_value = entry.get("timestamp")
    if received_at_value is None:
        raise LiveSessionError("Live call entry missing receipt timestamp")

    frequency_hz = _coerce_optional_float(
        entry.get("frequency_hz") or entry.get("call_freq") or entry.get("frequencyHz")
    )
    ttl_seconds = _coerce_optional_float(entry.get("call_ttl") or entry.get("ttl"))
    metadata = _normalize_metadata(entry.get("metadata"))

    return Call(
        call_id=call_id,
        system_id=resolved_system_id,
        talkgroup_id=resolved_talkgroup_id,
        received_at=_parse_datetime(received_at_value),
        frequency_hz=frequency_hz,
        metadata=metadata,
        ttl_seconds=ttl_seconds,
        raw=MappingProxyType({str(key): value for key, value in entry.items()}),
    )


def _coerce_int(value: Any, field: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise LiveSessionError(f"Live call entry missing valid '{field}'") from exc


def _coerce_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise LiveSessionError("Boolean cannot represent integer cursor")
    if isinstance(value, (int, float, str)):
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise LiveSessionError(f"Invalid integer value: {value!r}") from exc
    raise LiveSessionError(f"Unsupported integer value: {value!r}")


def _coerce_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise LiveSessionError("Boolean cannot represent float cursor")
    if isinstance(value, (int, float, str)):
        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise LiveSessionError(f"Invalid float value: {value!r}") from exc
    raise LiveSessionError(f"Unsupported float value: {value!r}")


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            try:
                return datetime.fromtimestamp(float(value), tz=UTC)
            except ValueError as exc:
                raise LiveSessionError(f"Unsupported datetime value: {value!r}") from exc
    raise LiveSessionError(f"Unsupported datetime value: {value!r}")


def _normalize_metadata(value: Any) -> Mapping[str, str]:
    if value is None:
        return MappingProxyType({})
    if not isinstance(value, Mapping):
        raise LiveSessionError("Call metadata must be a mapping")
    typed_mapping = cast(Mapping[str, object], value)
    normalized = {str(key): str(val) for key, val in typed_mapping.items()}
    return MappingProxyType(normalized)
