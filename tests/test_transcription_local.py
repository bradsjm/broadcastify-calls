"""Tests for the local faster-whisper transcription backend prompt behaviour."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest

from broadcastify_client.config import TranscriptionConfig
from broadcastify_client.transcription_local import LocalWhisperBackend


class DummySegment:
    """Simple transcription segment exposing text."""

    def __init__(self, text: str) -> None:
        """Capture the provided segment text."""
        self.text = text


@dataclass(slots=True)
class DummyModel:
    """Capture transcribe calls and return a canned transcript."""

    last_kwargs: dict[str, object] | None = None
    text: str = "Segment text"

    def transcribe(self, path: str, **kwargs: object) -> tuple[list[Any], SimpleNamespace]:
        """Record keyword arguments and emit a single segment."""
        _ = path
        self.last_kwargs = dict(kwargs)
        return [DummySegment(self.text)], SimpleNamespace()


def _install_dummy_module(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install a fake faster-whisper module returning DummyModel instances."""

    class DummyWhisperFactory:
        def __call__(self, *args: object, **kwargs: object) -> DummyModel:
            _ = args, kwargs
            return DummyModel()

    dummy_module = SimpleNamespace(WhisperModel=DummyWhisperFactory())

    def import_module(_: str) -> SimpleNamespace:
        return dummy_module

    monkeypatch.setattr(
        "broadcastify_client.transcription_local.importlib.import_module",
        import_module,
    )


def _build_backend(
    monkeypatch: pytest.MonkeyPatch, **config_overrides: Any
) -> tuple[InspectableLocalWhisperBackend, TranscriptionConfig]:
    """Create a LocalWhisperBackend with patched dependencies."""
    _install_dummy_module(monkeypatch)
    config = TranscriptionConfig(provider="local", enabled=True, **config_overrides)
    backend = InspectableLocalWhisperBackend(config)
    return backend, config


def test_transcribe_sync_forwards_initial_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure the synchronous transcription path forwards the prompt."""
    backend, config = _build_backend(monkeypatch, initial_prompt="   Custom prompt  ")
    model = DummyModel()

    result = backend.transcribe_sync_public(model, b"audio", prompt="Custom prompt")

    assert result == "Segment text"
    assert model.last_kwargs is not None
    assert model.last_kwargs.get("initial_prompt") == "Custom prompt"
    assert model.last_kwargs.get("language") == config.language


@pytest.mark.asyncio
async def test_transcribe_bytes_skips_blank_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify blank prompts are not forwarded to the local transcriber."""
    backend, _config = _build_backend(monkeypatch, initial_prompt="   ")

    dummy_model = DummyModel()
    backend.set_test_model(dummy_model)

    async def fake_to_thread(
        func: Callable[..., str],
        *args: object,
        **kwargs: object,
    ) -> str:
        return func(*args, **kwargs)

    monkeypatch.setattr(
        "broadcastify_client.transcription_local.asyncio.to_thread", fake_to_thread
    )

    text = await backend.transcribe_bytes_public(b"audio")

    assert text == "Segment text"
    assert dummy_model.last_kwargs is not None
    assert "initial_prompt" not in dummy_model.last_kwargs
class InspectableLocalWhisperBackend(LocalWhisperBackend):
    """Test-friendly backend exposing hooks around private methods."""

    def __init__(self, config: TranscriptionConfig) -> None:
        """Initialise the backend with optional test overrides."""
        super().__init__(config)
        self._test_model: DummyModel | None = None

    def set_test_model(self, model: DummyModel) -> None:
        """Inject a dummy model returned by `_ensure_model`."""
        self._test_model = model

    async def _ensure_model(self) -> object:  # type: ignore[override]
        if self._test_model is not None:
            return self._test_model
        return await super()._ensure_model()

    async def transcribe_bytes_public(self, payload: bytes) -> str:
        """Expose `_transcribe_bytes` for assertions."""
        return await super()._transcribe_bytes(payload)

    def transcribe_sync_public(
        self,
        model: object,
        payload: bytes,
        prompt: str | None = None,
    ) -> str:
        """Expose `_transcribe_sync` for assertions."""
        return super()._transcribe_sync(model, payload, prompt)
