from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterable, Mapping
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import cast

import httpx
import pytest

from broadcastify_client import (
    ArchiveResult,
    AudioChunkEvent,
    BroadcastifyClient,
    Call,
    CallEvent,
    Credentials,
    SessionToken,
    TimeWindow,
)
from broadcastify_client.archives import ArchiveClient
from broadcastify_client.audio_consumer import AudioConsumer
from broadcastify_client.client import (
    BroadcastifyClientDependencies,
    _HttpCallPoller,  # pyright: ignore[reportPrivateUsage]
)
from broadcastify_client.http import AsyncHttpClientProtocol
from broadcastify_client.live_producer import CallPoller
from broadcastify_client.telemetry import NullTelemetrySink, TelemetrySink

TEST_USERNAME = "alice"
TEST_CREDENTIAL_VALUE = "test-secret"
EXPECTED_SESSION_VALUE = "token-alice"
EXPECTED_CALL_ID = "1-2"


class StubAuthenticationBackend:
    def __init__(self) -> None:
        self.login_calls = 0
        self.logout_calls = 0

    async def login(self, credentials: Credentials) -> SessionToken:
        self.login_calls += 1
        return SessionToken(token=f"token-{credentials.username}")

    async def logout(self, token: SessionToken) -> None:
        self.logout_calls += 1


class StubArchiveClient:
    def __init__(self, result: ArchiveResult) -> None:
        self._result = result
        self.calls = 0

    async def get_archived_calls(
        self, system_id: int, talkgroup_id: int, time_block: int
    ) -> ArchiveResult:
        self.calls += 1
        return self._result


class StubHttpClient(AsyncHttpClientProtocol):
    async def post_form(
        self,
        url: str,
        data: Mapping[str, str],
        *,
        headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:  # pragma: no cover - unused in tests
        raise AssertionError("post_form should not be called in tests")

    async def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, object] | None = None,
    ) -> httpx.Response:  # pragma: no cover - unused in tests
        raise AssertionError("get should not be called in tests")

    async def close(self) -> None:
        return None


class StubCallPoller:
    def __init__(self, event: CallEvent) -> None:
        self.event = event
        self.invocations = 0
        self._delivered = False

    async def fetch(self, *, cursor: float | None) -> tuple[Iterable[CallEvent], float | None]:
        self.invocations += 1
        if not self._delivered:
            self._delivered = True
            return [self.event], cursor
        await asyncio.sleep(0)
        return [], cursor


class StubCallPollerFactory:
    def __init__(self, event: CallEvent) -> None:
        self.event = event
        self.pollers: list[StubCallPoller] = []

    def create(
        self,
        system_id: int,
        talkgroup_id: int,
        *,
        http_client: AsyncHttpClientProtocol,
        telemetry: TelemetrySink,
    ) -> CallPoller:
        poller = StubCallPoller(self.event)
        self.pollers.append(poller)
        return poller


class StubAudioDownloader:
    def __init__(self) -> None:
        self.requests: list[str] = []

    async def fetch_audio(self, call: CallEvent) -> AsyncIterator[AudioChunkEvent]:
        self.requests.append(call.call.call_id)

        async def _iterator() -> AsyncIterator[AudioChunkEvent]:
            yield AudioChunkEvent(
                call_id=call.call.call_id,
                sequence=1,
                start_offset=0.0,
                end_offset=1.0,
                payload=b"chunk-1",
                content_type="audio/mpeg",
                finished=False,
            )
            yield AudioChunkEvent(
                call_id=call.call.call_id,
                sequence=2,
                start_offset=1.0,
                end_offset=2.0,
                payload=b"chunk-2",
                content_type="audio/mpeg",
                finished=True,
            )

        return _iterator()


@pytest.mark.asyncio
async def test_authenticate_with_credentials_caches_token() -> None:
    backend = StubAuthenticationBackend()
    now = datetime.now(UTC)
    archive_result = ArchiveResult(
        calls=[],
        window=TimeWindow(start=now, end=now + timedelta(minutes=5)),
        fetched_at=now,
        cache_hit=False,
        raw=MappingProxyType({}),
    )
    archive_client_stub = StubArchiveClient(archive_result)
    archive_client = cast(ArchiveClient, archive_client_stub)
    base_call = Call(
        call_id="1-2",
        system_id=1,
        talkgroup_id=2,
        received_at=datetime.now(UTC),
        frequency_hz=None,
        metadata={},
    )
    poller_factory = StubCallPollerFactory(
        CallEvent(
            call=base_call,
            cursor=None,
            received_at=datetime.now(UTC),
            shard_key=(1, 2),
            raw_payload=MappingProxyType({}),
        )
    )
    dependencies = BroadcastifyClientDependencies(
        authentication_backend=backend,
        archive_client=archive_client,
        http_client=StubHttpClient(),
        call_poller_factory=poller_factory,
    )
    client = BroadcastifyClient(dependencies=dependencies)

    token_first = await client.authenticate(
        Credentials(username=TEST_USERNAME, password=TEST_CREDENTIAL_VALUE)
    )
    token_second = await client.authenticate(
        Credentials(username=TEST_USERNAME, password=TEST_CREDENTIAL_VALUE)
    )

    assert token_first.token == EXPECTED_SESSION_VALUE
    assert token_second.token == EXPECTED_SESSION_VALUE
    assert backend.login_calls == 1
    assert archive_client_stub.calls == 0

    await client.shutdown()


@pytest.mark.asyncio
async def test_get_archived_calls_uses_archive_client() -> None:
    backend = StubAuthenticationBackend()
    now = datetime.now(UTC)
    archive_result = ArchiveResult(
        calls=[],
        window=TimeWindow(start=now, end=now + timedelta(minutes=5)),
        fetched_at=now,
        cache_hit=True,
        raw=MappingProxyType({}),
    )
    archive_client_stub = StubArchiveClient(archive_result)
    archive_client = cast(ArchiveClient, archive_client_stub)
    call_instance = Call(
        call_id="3-4",
        system_id=3,
        talkgroup_id=4,
        received_at=datetime.now(UTC),
        frequency_hz=None,
        metadata={},
    )
    event = CallEvent(
        call=call_instance,
        cursor=5.0,
        received_at=datetime.now(UTC),
        shard_key=(3, 4),
        raw_payload=MappingProxyType({}),
    )
    poller_factory = StubCallPollerFactory(event)
    dependencies = BroadcastifyClientDependencies(
        authentication_backend=backend,
        archive_client=archive_client,
        http_client=StubHttpClient(),
        call_poller_factory=poller_factory,
    )
    client = BroadcastifyClient(dependencies=dependencies)

    result = await client.get_archived_calls(3, 4, 5)

    assert result is archive_result
    assert archive_client_stub.calls == 1

    await client.shutdown()


@pytest.mark.asyncio
async def test_create_live_producer_emits_events_once_started() -> None:
    backend = StubAuthenticationBackend()
    now = datetime.now(UTC)
    archive_result = ArchiveResult(
        calls=[],
        window=TimeWindow(start=now, end=now + timedelta(minutes=5)),
        fetched_at=now,
        cache_hit=False,
        raw=MappingProxyType({}),
    )
    archive_client_stub = StubArchiveClient(archive_result)
    archive_client = cast(ArchiveClient, archive_client_stub)
    call_payload = Call(
        call_id=EXPECTED_CALL_ID,
        system_id=1,
        talkgroup_id=2,
        received_at=datetime.now(UTC),
        frequency_hz=851.0125,
        metadata={},
    )
    call_event = CallEvent(
        call=call_payload,
        cursor=12.0,
        received_at=datetime.now(UTC),
        shard_key=(1, 2),
        raw_payload=MappingProxyType({}),
    )
    poller_factory = StubCallPollerFactory(call_event)
    dependencies = BroadcastifyClientDependencies(
        authentication_backend=backend,
        archive_client=archive_client,
        http_client=StubHttpClient(),
        call_poller_factory=poller_factory,
    )
    client = BroadcastifyClient(dependencies=dependencies)

    received_general: list[CallEvent] = []
    received_specific: list[CallEvent] = []
    general_event = asyncio.Event()
    specific_event = asyncio.Event()

    async def general_consumer(event: object) -> None:
        assert isinstance(event, CallEvent)
        received_general.append(event)
        general_event.set()

    async def specific_consumer(event: object) -> None:
        assert isinstance(event, CallEvent)
        received_specific.append(event)
        specific_event.set()

    await client.register_consumer("calls.live", general_consumer)
    await client.register_consumer("calls.live.1.2", specific_consumer)

    await client.create_live_producer(system_id=1, talkgroup_id=2)
    await client.start()
    try:
        await asyncio.wait_for(general_event.wait(), timeout=1.0)
        await asyncio.wait_for(specific_event.wait(), timeout=1.0)
        assert received_general[0].call.call_id == EXPECTED_CALL_ID
        assert received_specific[0].call.call_id == EXPECTED_CALL_ID
        assert poller_factory.pollers[0].invocations >= 1
    finally:
        await client.shutdown()
    assert archive_client_stub.calls == 0


@pytest.mark.asyncio
async def test_audio_pipeline_publishes_chunks() -> None:
    backend = StubAuthenticationBackend()
    now = datetime.now(UTC)
    archive_result = ArchiveResult(
        calls=[],
        window=TimeWindow(start=now, end=now + timedelta(minutes=5)),
        fetched_at=now,
        cache_hit=False,
        raw=MappingProxyType({}),
    )
    archive_client_stub = StubArchiveClient(archive_result)
    archive_client = cast(ArchiveClient, archive_client_stub)
    call_payload = Call(
        call_id=EXPECTED_CALL_ID,
        system_id=1,
        talkgroup_id=2,
        received_at=datetime.now(UTC),
        frequency_hz=851.0125,
        metadata={},
    )
    call_event = CallEvent(
        call=call_payload,
        cursor=12.0,
        received_at=datetime.now(UTC),
        shard_key=(1, 2),
        raw_payload=MappingProxyType({}),
    )
    poller_factory = StubCallPollerFactory(call_event)
    downloader = StubAudioDownloader()
    dependencies = BroadcastifyClientDependencies(
        authentication_backend=backend,
        archive_client=archive_client,
        http_client=StubHttpClient(),
        call_poller_factory=poller_factory,
        audio_consumer_factory=lambda: AudioConsumer(downloader, telemetry=NullTelemetrySink()),
    )
    client = BroadcastifyClient(dependencies=dependencies)

    audio_general_event = asyncio.Event()
    audio_specific_event = asyncio.Event()
    received_audio_general: list[AudioChunkEvent] = []
    received_audio_specific: list[AudioChunkEvent] = []

    async def audio_general_consumer(event: object) -> None:
        assert isinstance(event, AudioChunkEvent)
        received_audio_general.append(event)
        audio_general_event.set()

    async def audio_specific_consumer(event: object) -> None:
        assert isinstance(event, AudioChunkEvent)
        received_audio_specific.append(event)
        if event.finished:
            audio_specific_event.set()

    await client.register_consumer("calls.audio", audio_general_consumer)
    await client.register_consumer(f"calls.audio.{EXPECTED_CALL_ID}", audio_specific_consumer)

    await client.create_live_producer(system_id=1, talkgroup_id=2)
    await client.start()
    try:
        await asyncio.wait_for(audio_general_event.wait(), timeout=1.0)
        await asyncio.wait_for(audio_specific_event.wait(), timeout=1.0)
        assert received_audio_general[0].call_id == EXPECTED_CALL_ID
        assert received_audio_specific[-1].finished is True
        assert downloader.requests == [EXPECTED_CALL_ID]
    finally:
        await client.shutdown()
    assert archive_client_stub.calls == 0


class RecordingHttpClient(AsyncHttpClientProtocol):
    def __init__(self, payloads: list[dict[str, object]]) -> None:
        self._payloads = payloads
        self.calls: list[dict[str, object]] = []

    async def post_form(
        self,
        url: str,
        data: Mapping[str, str],
        *,
        headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        self.calls.append({"url": url, "data": dict(data)})
        if not self._payloads:
            raise AssertionError("Unexpected additional poll request")
        payload = self._payloads.pop(0)
        request = httpx.Request("POST", f"https://www.broadcastify.com{url}")
        return httpx.Response(200, json=payload, request=request)

    async def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, object] | None = None,
    ) -> httpx.Response:
        raise AssertionError("get should not be called")

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_http_call_poller_parses_events() -> None:
    expected_last_pos = 15.0
    expected_frequency = 851.0125
    expected_cursor = 12.5
    next_cursor_value = 18.0
    call_entry: dict[str, object] = {
        "id": "1-2",
        "systemId": 1,
        "sid": 1,
        "call_tg": 2,
        "metadata": {"foo": "bar"},
        "call_freq": expected_frequency,
        "call-ttl": 1758450000,
        "ts": 10,
        "pos": expected_cursor,
    }
    first_payload: dict[str, object] = {
        "sessionKey": "server-session",
        "serverTime": 20,
        "lastPos": expected_last_pos,
        "calls": [call_entry],
    }
    second_payload: dict[str, object] = {
        "serverTime": 25,
        "lastPos": next_cursor_value,
        "calls": [],
    }
    http_client = RecordingHttpClient([first_payload, second_payload])
    telemetry = NullTelemetrySink()
    poller = _HttpCallPoller(
        system_id=1,
        talkgroup_id=2,
        http_client=http_client,
        telemetry=telemetry,
    )

    events, cursor = await poller.fetch(cursor=None)

    assert cursor == expected_last_pos
    assert len(events) == 1
    event = events[0]
    assert event.call.call_id == "1-2"
    assert event.call.frequency_hz == expected_frequency
    assert event.call.metadata["foo"] == "bar"
    assert event.cursor == expected_cursor
    assert poller._session_key == "server-session"  # type: ignore[attr-defined]
    first_request = cast(dict[str, str], http_client.calls[0]["data"])
    assert first_request["doInit"] == "1"

    events_second, cursor_second = await poller.fetch(cursor=cursor)

    assert events_second == []
    assert cursor_second == next_cursor_value
    second_request = cast(dict[str, str], http_client.calls[1]["data"])
    assert second_request["doInit"] == "0"
