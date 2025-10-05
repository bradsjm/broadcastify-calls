"""Tests for the OpenAI transcription backend response parsing."""

from __future__ import annotations

import io
from dataclasses import dataclass
from types import SimpleNamespace
from typing import cast

import pytest

from broadcastify_client.config import TranscriptionConfig
from broadcastify_client.errors import TranscriptionError
from broadcastify_client.transcription_openai import OpenAIWhisperBackend


class DummyTranscriptions:
    """Stub transcription namespace returning a preconfigured response."""

    def __init__(self) -> None:
        """Initialise storage for the canned response object."""
        self.response: object | None = None
        self.last_file: tuple[str, object, str] | None = None
        self.last_model: str | None = None
        self.last_language: str | None = None
        self.last_prompt: str | None = None

    async def create(
        self,
        *,
        model: str,
        file: tuple[str, object, str],
        language: str | None,
        prompt: str | None = None,
    ) -> object:
        """Return the configured response captured via ``self.response``."""
        self.last_model = model
        self.last_file = file
        self.last_language = language
        self.last_prompt = prompt
        if self.response is None:  # pragma: no cover - defensive
            raise AssertionError("Test attempted to call create without configuring response")
        return self.response


@dataclass(slots=True)
class BackendHarness:
    """Aggregates the backend under test with its supporting stubs."""

    backend: OpenAIWhisperBackend
    transcriptions: DummyTranscriptions
    config: TranscriptionConfig


class DummyAudio:
    """Container exposing the dummy transcription API."""

    def __init__(self) -> None:
        """Initialise the transcription stub."""
        self.transcriptions = DummyTranscriptions()


class DummyAsyncOpenAI:
    """Minimal stand-in for the OpenAI Async client."""

    def __init__(self, **_: str) -> None:
        """Ignore client kwargs while exposing the audio namespace."""
        self.audio = DummyAudio()


@pytest.fixture(name="backend")
def backend_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> BackendHarness:
    """Provide a configured backend and direct access to the transcription stub."""
    dummy_module = SimpleNamespace(AsyncOpenAI=DummyAsyncOpenAI)

    def import_module(_: str) -> SimpleNamespace:
        return dummy_module

    monkeypatch.setattr(
        "broadcastify_client.transcription_openai.importlib.import_module",
        import_module,
    )

    config = TranscriptionConfig(provider="openai", enabled=True, api_key="token")
    backend = OpenAIWhisperBackend(config)
    transcriptions = cast(
        DummyTranscriptions,
        backend._client.audio.transcriptions,  # pyright: ignore[reportPrivateUsage]
    )
    return BackendHarness(backend=backend, transcriptions=transcriptions, config=config)


async def _invoke_transcription(harness: BackendHarness, response: object) -> str:
    """Call `_transcribe_bytes` with a canned provider response."""
    harness.transcriptions.response = response
    return await harness.backend._transcribe_bytes(b"audio")  # pyright: ignore[reportPrivateUsage]


@dataclass(slots=True)
class AttributeResponse:
    """Simple response object exposing a text attribute."""

    text: str


@pytest.mark.asyncio
async def test_transcribe_bytes_supports_attribute_response(backend: BackendHarness) -> None:
    """Ensure attribute-style responses return the embedded transcription text."""
    response = AttributeResponse(text="attribute value")
    text = await _invoke_transcription(backend, response)
    assert text == "attribute value"
    assert backend.transcriptions.last_file is not None
    filename, file_obj, mime = backend.transcriptions.last_file
    assert filename.endswith(".m4a")
    assert mime == "audio/mp4"
    assert isinstance(file_obj, io.BytesIO)
    assert backend.transcriptions.last_prompt == backend.config.initial_prompt.strip()


@pytest.mark.asyncio
async def test_transcribe_bytes_raises_when_text_missing(backend: BackendHarness) -> None:
    """Raise an error when provider response does not expose text attribute."""
    with pytest.raises(TranscriptionError, match="missing text"):
        await _invoke_transcription(backend, object())


@pytest.mark.asyncio
async def test_transcribe_bytes_raises_when_text_empty(backend: BackendHarness) -> None:
    """Raise an error when provider text attribute is empty."""
    response = AttributeResponse(text="   ")
    with pytest.raises(TranscriptionError, match="empty text"):
        await _invoke_transcription(backend, response)


@pytest.mark.asyncio
async def test_transcribe_bytes_skips_empty_prompt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure blank prompts are not forwarded to the provider."""
    dummy_module = SimpleNamespace(AsyncOpenAI=DummyAsyncOpenAI)

    def import_module(_: str) -> SimpleNamespace:
        return dummy_module

    monkeypatch.setattr(
        "broadcastify_client.transcription_openai.importlib.import_module",
        import_module,
    )

    config = TranscriptionConfig(
        provider="openai",
        enabled=True,
        api_key="token",
        initial_prompt="  ",
    )
    backend = OpenAIWhisperBackend(config)
    transcriptions = cast(
        DummyTranscriptions,
        backend._client.audio.transcriptions,  # pyright: ignore[reportPrivateUsage]
    )
    harness = BackendHarness(backend=backend, transcriptions=transcriptions, config=config)

    response = AttributeResponse(text="value")
    text = await _invoke_transcription(harness, response)
    assert text == "value"
    assert harness.transcriptions.last_prompt is None
