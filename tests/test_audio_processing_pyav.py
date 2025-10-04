"""Tests for the PyAV audio processing integration."""

from __future__ import annotations

import asyncio
import importlib
import logging
from collections.abc import Callable
from types import MethodType, ModuleType, SimpleNamespace
from typing import Any, ClassVar

import numpy as np
import pytest

from broadcastify_client.audio_processing import AudioProcessingError, NullAudioProcessor
from broadcastify_client.audio_processing_pyav import PyAvSilenceTrimmer
from broadcastify_client.client import BroadcastifyClient, BroadcastifyClientDependencies
from broadcastify_client.config import AudioProcessingConfig, AudioProcessingStage
from broadcastify_client.models import AudioPayloadEvent


def test_pyav_trimmer_requires_pyav(monkeypatch: pytest.MonkeyPatch) -> None:
    """Instantiation fails with a helpful error when PyAV is unavailable."""
    original_import = importlib.import_module

    def _import(name: str, package: str | None = None) -> Any:
        if name == "av":
            raise ImportError("PyAV missing")
        return original_import(name, package)

    monkeypatch.setattr(importlib, "import_module", _import)
    cfg = AudioProcessingConfig(
        stages=frozenset({AudioProcessingStage.TRIM})
    )
    with pytest.raises(AudioProcessingError) as excinfo:
        PyAvSilenceTrimmer(cfg)
    assert "PyAV" in str(excinfo.value)


def test_client_falls_back_when_pyav_missing(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Client falls back to the null processor and emits a warning when PyAV is missing."""
    monkeypatch.setattr("broadcastify_client.client.PyAvSilenceTrimmer", None)

    cfg = AudioProcessingConfig(
        stages=frozenset({AudioProcessingStage.TRIM})
    )
    caplog.set_level(logging.WARNING, logger="broadcastify_client.client")
    client = BroadcastifyClient(
        dependencies=BroadcastifyClientDependencies(audio_processing_config=cfg)
    )
    processor = client._create_audio_processor()  # pyright: ignore[reportPrivateUsage]
    assert isinstance(processor, NullAudioProcessor)
    assert any("PyAV" in record.message for record in caplog.records)


def test_client_logs_activation_when_pyav_available(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A stub trimmer triggers the informational activation log."""

    class FakeTrimmer(NullAudioProcessor):
        def __init__(
            self,
            config: AudioProcessingConfig,
            *,
            logger: logging.Logger | None = None,
        ) -> None:
            self.config = config
            self.logger = logger

    monkeypatch.setattr(
        "broadcastify_client.client.PyAvSilenceTrimmer",
        FakeTrimmer,
        raising=False,
    )

    cfg = AudioProcessingConfig(
        stages=frozenset({AudioProcessingStage.TRIM})
    )
    caplog.set_level(logging.INFO, logger="broadcastify_client.client")
    client = BroadcastifyClient(
        dependencies=BroadcastifyClientDependencies(audio_processing_config=cfg)
    )
    processor = client._create_audio_processor()  # pyright: ignore[reportPrivateUsage]
    assert isinstance(processor, FakeTrimmer)
    assert any(
        "PyAV audio processing enabled" in record.message for record in caplog.records
    )


@pytest.mark.asyncio
async def test_process_propagates_context_for_unexpected_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unexpected exceptions are wrapped with contextual information for diagnostics."""

    async def _to_thread(
        func: Callable[..., object],
        *args: object,
        **kwargs: object,
    ) -> object:  # pragma: no cover - test helper
        return func(*args, **kwargs)

    def _boom(self: PyAvSilenceTrimmer, event: AudioPayloadEvent) -> AudioPayloadEvent:
        raise RuntimeError("decode explosion")

    monkeypatch.setattr(asyncio, "to_thread", _to_thread)
    monkeypatch.setattr(PyAvSilenceTrimmer, "_process_sync", _boom)

    trimmer = object.__new__(PyAvSilenceTrimmer)
    trimmer._config = AudioProcessingConfig(  # type: ignore[attr-defined]
        stages=frozenset({AudioProcessingStage.TRIM})
    )
    trimmer._logger = logging.getLogger("test.trimmer")  # type: ignore[attr-defined]

    event = AudioPayloadEvent(
        call_id="test",
        sequence=0,
        start_offset=0.0,
        end_offset=1.0,
        payload=b"data",
        content_type="audio/mp4",
        finished=True,
    )

    with pytest.raises(AudioProcessingError) as excinfo:
        await PyAvSilenceTrimmer.process(trimmer, event)

    message = str(excinfo.value)
    assert "RuntimeError" in message
    assert "decode explosion" in message


def test_frame_to_mono_handles_format_typeerror() -> None:
    """Frame conversion falls back when PyAV rejects the format keyword argument."""

    class FakeFrame:
        def __init__(self) -> None:
            self.invocations: list[dict[str, Any]] = []

        def to_ndarray(self, **kwargs: Any) -> Any:
            self.invocations.append(kwargs)
            if "format" in kwargs:
                raise TypeError("unexpected keyword argument 'format'")
            return np.array([[1.0, -1.0], [0.0, 0.5]], dtype=np.float32)

    trimmer = object.__new__(PyAvSilenceTrimmer)
    trimmer._np = np  # type: ignore[attr-defined]
    trimmer._logger = logging.getLogger("test.trimmer")  # type: ignore[attr-defined]

    frame = FakeFrame()
    result = PyAvSilenceTrimmer._frame_to_mono(trimmer, frame)  # pyright: ignore[reportPrivateUsage]

    assert np.allclose(result, np.array([0.5, -0.25], dtype=np.float32))
    assert frame.invocations[0] == {"format": "fltp"}
    assert frame.invocations[1] == {}


def test_trimmer_uses_av_error_module(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fallback to av.error.AVError allows catching container errors consistently."""
    original_import = importlib.import_module

    class FakeAVError(Exception):
        pass

    fake_av_error_module = ModuleType("av.error")
    fake_av_error_module.AVError = FakeAVError  # type: ignore[attr-defined]

    fake_av = ModuleType("av")

    def _open(*_args: object, **_kwargs: object) -> None:
        raise FakeAVError("container failure")

    fake_av.open = _open  # type: ignore[attr-defined]

    def _import(name: str, package: str | None = None) -> Any:
        if name == "av":
            return fake_av
        if name == "av.error":
            return fake_av_error_module
        return original_import(name, package)

    monkeypatch.setattr(importlib, "import_module", _import)

    cfg = AudioProcessingConfig(
        stages=frozenset({AudioProcessingStage.TRIM})
    )
    trimmer = PyAvSilenceTrimmer(cfg)

    event = AudioPayloadEvent(
        call_id="test",
        sequence=0,
        start_offset=0.0,
        end_offset=0.5,
        payload=b"payload",
        content_type="audio/mp4",
        finished=True,
    )

    with pytest.raises(AudioProcessingError) as excinfo:
        PyAvSilenceTrimmer._process_sync(trimmer, event)  # pyright: ignore[reportPrivateUsage]

    message = str(excinfo.value)
    assert "Failed to open audio container" in message
    assert "FakeAVError" in message


def test_encode_failures_include_cause(monkeypatch: pytest.MonkeyPatch) -> None:
    """Encoding failures include the underlying exception detail."""
    original_import = importlib.import_module

    class FakeAVError(Exception):
        pass

    class FakeAudioFrameObject:
        def __init__(self) -> None:
            self.sample_rate: int | None = None
            self.time_base: Any = None
            self.pts: int | None = None

    def _from_ndarray(data: Any, layout: str | None = None) -> FakeAudioFrameObject:
        if getattr(data, "dtype", None) != np.int16:
            raise ValueError("Expected numpy array with dtype `int16`")
        return FakeAudioFrameObject()

    class FakeStream:
        def __init__(self) -> None:
            self.codec_context = type(
                "Codec",
                (),
                {"frame_size": 2, "sample_rate": 48_000, "name": "aac"},
            )()
            self.layout = "mono"
            self.time_base = None
            self.bit_rate = 0

        def encode(self, _frame: object) -> list[object]:
            raise FakeAVError("encoder boom")

    class FakeContainer:
        def __enter__(self) -> FakeContainer:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def add_stream(self, *_args: object, **_kwargs: object) -> FakeStream:
            return FakeStream()

        def mux(self, _packet: object) -> None:
            return None

    def _open(*_args: object, **_kwargs: object) -> FakeContainer:
        return FakeContainer()

    def _import(name: str, package: str | None = None) -> Any:
        if name == "av":
            fake_av = ModuleType("av")
            fake_av.open = _open  # type: ignore[attr-defined]
            fake_av.AudioFrame = type(  # type: ignore[attr-defined]
                "AudioFrame",
                (),
                {"from_ndarray": staticmethod(_from_ndarray)},
            )
            fake_av.AVError = FakeAVError  # type: ignore[attr-defined]
            return fake_av
        if name == "av.error":
            fake_error_module = ModuleType("av.error")
            fake_error_module.AVError = FakeAVError  # type: ignore[attr-defined]
            return fake_error_module
        return original_import(name, package)

    monkeypatch.setattr(importlib, "import_module", _import)

    cfg = AudioProcessingConfig(
        stages=frozenset({AudioProcessingStage.TRIM})
    )
    trimmer = PyAvSilenceTrimmer(cfg)

    trimmed = trimmer._np.array([0.1, 0.2], dtype=trimmer._np.float32)  # type: ignore[attr-defined]

    with pytest.raises(AudioProcessingError) as excinfo:
        trimmer._encode_aac(trimmed, sample_rate=48_000, bit_rate=0)  # pyright: ignore[reportPrivateUsage]

    message = str(excinfo.value)
    assert "Failed to encode trimmed audio" in message
    assert "FakeAVError" in message


def test_band_pass_disabled_when_filter_module_missing(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Missing av.filter module disables the band-pass feature with a warning."""
    original_import = importlib.import_module

    class FakeAVError(Exception):
        pass

    fake_av = ModuleType("av")
    fake_av.AVError = FakeAVError  # type: ignore[attr-defined]

    def _import(name: str, package: str | None = None) -> Any:
        if name == "av":
            return fake_av
        if name == "av.error":
            mod = ModuleType("av.error")
            mod.AVError = FakeAVError  # type: ignore[attr-defined]
            return mod
        if name == "av.filter":
            raise ImportError("filter module not available")
        return original_import(name, package)

    caplog.set_level(logging.WARNING)
    monkeypatch.setattr(importlib, "import_module", _import)

    cfg = AudioProcessingConfig(
        stages=frozenset({AudioProcessingStage.TRIM, AudioProcessingStage.BAND_PASS})
    )
    trimmer = PyAvSilenceTrimmer(cfg, logger=logging.getLogger("test.trimmer"))

    assert trimmer._band_pass_enabled is False  # type: ignore[attr-defined]
    assert any("Band-pass filter disabled" in record.message for record in caplog.records)


def test_apply_band_pass_uses_filter_graph() -> None:
    """Band-pass application drives the configured graph and returns filtered audio."""

    class FakeAVError(Exception):
        pass

    class FakeBlockingError(Exception):
        pass

    class FakeFrame:
        def __init__(self, data: np.ndarray) -> None:
            self.data = data
            self.pts: int | None = None

    class FakeGraph:
        instances: ClassVar[list[FakeGraph]] = []

        def __init__(self) -> None:
            self.__class__.instances.append(self)
            self.abuffer_args: dict[str, Any] | None = None
            self.added_filters: list[tuple[str, str | None]] = []
            self.queue: list[FakeFrame] = []

        def add(self, name: str, args: str | None = None) -> tuple[str, str | None]:
            self.added_filters.append((name, args))
            return (name, args)

        def link_nodes(self, *_nodes: object) -> FakeGraph:
            return self

        def configure(self) -> FakeGraph:
            return self

        def push(self, frame: FakeFrame | None) -> None:
            if frame is None:
                return
            scaled = FakeFrame(frame.data * 0.5)
            self.queue.append(scaled)

        def pull(self) -> FakeFrame:
            if not self.queue:
                raise FakeBlockingError()
            return self.queue.pop(0)

    fake_filter_module = SimpleNamespace(Graph=FakeGraph)

    trimmer = object.__new__(PyAvSilenceTrimmer)
    trimmer._config = AudioProcessingConfig(  # type: ignore[attr-defined]
        stages=frozenset({AudioProcessingStage.TRIM, AudioProcessingStage.BAND_PASS})
    )
    trimmer._logger = logging.getLogger("test.trimmer")  # type: ignore[attr-defined]
    trimmer._band_pass_enabled = True  # type: ignore[attr-defined]
    trimmer._filter_module = fake_filter_module  # type: ignore[attr-defined]
    trimmer._av = SimpleNamespace()  # type: ignore[attr-defined]
    trimmer._np = np  # type: ignore[attr-defined]
    trimmer._av_error_types = (FakeAVError,)  # type: ignore[attr-defined]
    trimmer._blocking_errors = (FakeBlockingError,)  # type: ignore[attr-defined]
    trimmer._eof_errors = ()  # type: ignore[attr-defined]
    def _fake_frame_to_mono(_self: PyAvSilenceTrimmer, frame: FakeFrame) -> np.ndarray:
        return frame.data

    def _fake_create_audio_frame(
        _self: PyAvSilenceTrimmer, chunk: Any, _sample_rate: int
    ) -> FakeFrame:
        return FakeFrame(np.asarray(chunk, dtype=np.float32))

    trimmer._frame_to_mono = MethodType(_fake_frame_to_mono, trimmer)  # type: ignore[attr-defined]
    trimmer._create_audio_frame = MethodType(  # type: ignore[attr-defined]
        _fake_create_audio_frame,
        trimmer,
    )

    audio = np.array([1.0, -0.5, 0.25], dtype=np.float32)
    filtered = PyAvSilenceTrimmer._apply_band_pass(  # pyright: ignore[reportPrivateUsage]
        trimmer,
        audio,
        sample_rate=48_000,
    )

    assert np.allclose(filtered, audio * 0.5)
    graph = FakeGraph.instances.pop()
    assert (
        "abuffer",
        "sample_rate=48000:sample_fmt=fltp:channel_layout=mono:time_base=1/48000",
    ) in graph.added_filters
    assert ("highpass", "f=250.0:poles=2") in graph.added_filters
    assert ("lowpass", "f=3800.0:poles=2") in graph.added_filters
