from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta
from typing import cast

import httpx
import pytest

from broadcastify_client import (
    ArchiveResult,
    BroadcastifyClient,
    Call,
    CallEvent,
    Credentials,
    SessionToken,
)
from broadcastify_client.archives import ArchiveClient
from broadcastify_client.client import BroadcastifyClientDependencies
from broadcastify_client.http import AsyncHttpClientProtocol
from broadcastify_client.live_producer import CallPoller
from broadcastify_client.telemetry import TelemetrySink

TEST_USERNAME = "alice"
TEST_CREDENTIAL_VALUE = "test-secret"
EXPECTED_SESSION_VALUE = "token-alice"
EXPECTED_CALL_ID = 9


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

    async def fetch(self, *, cursor: int | None) -> tuple[Iterable[CallEvent], int | None]:
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


@pytest.mark.asyncio
async def test_authenticate_with_credentials_caches_token() -> None:
    backend = StubAuthenticationBackend()
    archive_result = ArchiveResult(
        calls=[],
        fetched_at=datetime.now(UTC),
        window_start=datetime.now(UTC),
        window_end=datetime.now(UTC) + timedelta(minutes=5),
        cache_hit=False,
    )
    archive_client_stub = StubArchiveClient(archive_result)
    archive_client = cast(ArchiveClient, archive_client_stub)
    poller_factory = StubCallPollerFactory(
        CallEvent(
            call=Call(
                call_id=1,
                system_id=1,
                talkgroup_id=2,
                received_at=datetime.now(UTC),
                frequency_hz=None,
                metadata={},
            ),
            cursor=None,
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
    archive_result = ArchiveResult(
        calls=[],
        fetched_at=datetime.now(UTC),
        window_start=datetime.now(UTC),
        window_end=datetime.now(UTC) + timedelta(minutes=5),
        cache_hit=True,
    )
    archive_client_stub = StubArchiveClient(archive_result)
    archive_client = cast(ArchiveClient, archive_client_stub)
    event = CallEvent(
        call=Call(
            call_id=2,
            system_id=3,
            talkgroup_id=4,
            received_at=datetime.now(UTC),
            frequency_hz=None,
            metadata={},
        ),
        cursor=5,
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
    archive_result = ArchiveResult(
        calls=[],
        fetched_at=datetime.now(UTC),
        window_start=datetime.now(UTC),
        window_end=datetime.now(UTC) + timedelta(minutes=5),
        cache_hit=False,
    )
    archive_client_stub = StubArchiveClient(archive_result)
    archive_client = cast(ArchiveClient, archive_client_stub)
    call_event = CallEvent(
        call=Call(
            call_id=EXPECTED_CALL_ID,
            system_id=1,
            talkgroup_id=2,
            received_at=datetime.now(UTC),
            frequency_hz=851.0125,
            metadata={},
        ),
        cursor=12,
    )
    poller_factory = StubCallPollerFactory(call_event)
    dependencies = BroadcastifyClientDependencies(
        authentication_backend=backend,
        archive_client=archive_client,
        http_client=StubHttpClient(),
        call_poller_factory=poller_factory,
    )
    client = BroadcastifyClient(dependencies=dependencies)

    producer = await client.create_live_producer(system_id=1, talkgroup_id=2)
    await client.start()
    try:
        received = await asyncio.wait_for(producer.queue.get(), timeout=0.5)
        assert received.call.call_id == EXPECTED_CALL_ID
        assert poller_factory.pollers[0].invocations >= 1
    finally:
        await client.shutdown()
    assert archive_client_stub.calls == 0
