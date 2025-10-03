"""PyAV-backed audio processing utilities."""

from __future__ import annotations

import asyncio
import importlib
import io
import logging
from fractions import Fraction
from typing import Any

from .audio_processing import AudioProcessingError, AudioProcessor
from .config import AudioProcessingConfig
from .models import AudioPayloadEvent


class PyAvSilenceTrimmer(AudioProcessor):
    """Trim leading and trailing silence from AAC payloads using PyAV."""

    _SUPPORTED_CONTENT_TYPES: tuple[str, ...] = ("audio/mp4", "audio/m4a", "audio/aac")

    def __init__(
        self,
        config: AudioProcessingConfig,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        """Initialise the trimmer, validating runtime dependencies."""
        self._config = config
        self._logger = logger or logging.getLogger(__name__)
        try:
            av = importlib.import_module("av")
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise AudioProcessingError(
                "PyAV is not installed; install the 'audio-processing' extra to enable trimming"
            ) from exc
        try:
            np = importlib.import_module("numpy")
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise AudioProcessingError(
                "NumPy is required by PyAV; install the 'audio-processing' extra to enable trimming"
            ) from exc

        self._av = av
        self._np = np
        self._av_error_types = self._resolve_error_types(av)

    async def process(self, event: AudioPayloadEvent) -> AudioPayloadEvent:
        """Trim silence from *event* while executing CPU work off the event loop."""
        if not event.payload:
            return event
        if event.content_type not in self._SUPPORTED_CONTENT_TYPES:
            raise AudioProcessingError(
                f"Unsupported content type '{event.content_type}' for PyAV processing"
            )
        try:
            return await asyncio.to_thread(self._process_sync, event)
        except AudioProcessingError:
            raise
        except Exception as exc:  # pragma: no cover - defensive path
            message = f"PyAV silence trimming failed ({exc.__class__.__name__}: {exc})"
            raise AudioProcessingError(message) from exc

    # ---------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------
    def _process_sync(self, event: AudioPayloadEvent) -> AudioPayloadEvent:
        payload = event.payload
        input_buffer = io.BytesIO(payload)

        try:
            container = self._av.open(input_buffer, mode="r")
        except self._av_error_types as exc:
            message = f"Failed to open audio container ({exc.__class__.__name__}: {exc})"
            raise AudioProcessingError(message) from exc

        with container:
            audio_stream = self._select_audio_stream(container)
            sample_rate = int(audio_stream.rate or audio_stream.codec_context.sample_rate or 0)
            if sample_rate <= 0:
                raise AudioProcessingError("Audio stream did not specify a valid sample rate")
            codec_name = audio_stream.codec_context.name
            if codec_name not in {"aac", "aac_latm"}:
                raise AudioProcessingError(
                    f"Unsupported audio codec '{codec_name}'; only AAC input is supported"
                )
            bit_rate = int(audio_stream.bit_rate or 0)
            audio = self._decode_to_mono(container, audio_stream)

        if audio.size == 0:
            return event

        trim_start, trim_end = self._determine_trim_bounds(audio, sample_rate)
        if trim_start == 0 and trim_end == audio.size:
            # Nothing to trim.
            return event
        if trim_start >= trim_end:
            self._logger.debug("All samples evaluated as silence; keeping original payload")
            return event

        trimmed = audio[trim_start:trim_end]
        trimmed_seconds = trimmed.size / sample_rate
        leading_seconds = trim_start / sample_rate
        trailing_seconds = (audio.size - trim_end) / sample_rate

        output_payload = self._encode_aac(trimmed, sample_rate, bit_rate)
        new_event = AudioPayloadEvent(
            call_id=event.call_id,
            sequence=event.sequence,
            start_offset=event.start_offset + leading_seconds,
            end_offset=max(
                event.start_offset + leading_seconds,
                event.end_offset - trailing_seconds,
            ),
            payload=output_payload,
            content_type=event.content_type,
            finished=event.finished,
        )
        if leading_seconds or trailing_seconds:
            self._logger.info(
                "Trimmed call %s: removed %.3f s leading, %.3f s trailing, new duration %.3f s",
                event.call_id,
                leading_seconds,
                trailing_seconds,
                trimmed_seconds,
            )
        return new_event

    def _select_audio_stream(self, container: Any) -> Any:
        streams = [stream for stream in container.streams if stream.type == "audio"]
        if not streams:
            raise AudioProcessingError("No audio stream found in container")
        return streams[0]

    def _decode_to_mono(self, container: Any, stream: Any) -> Any:
        np = self._np
        samples: list[Any] = []
        for frame in container.decode(stream):
            mono = self._frame_to_mono(frame)
            samples.append(mono)
        if not samples:
            return np.array([], dtype=np.float32)
        return np.concatenate(samples).astype(np.float32, copy=False)

    def _frame_to_mono(self, frame: Any) -> Any:
        np = self._np
        try:
            array = frame.to_ndarray(format="fltp")
        except TypeError:
            array = frame.to_ndarray()
        array = np.asarray(array, dtype=np.float32)
        if array.ndim == 1:
            return array
        return array.mean(axis=0, dtype=np.float32)

    def _determine_trim_bounds(self, audio: Any, sample_rate: int) -> tuple[int, int]:
        np = self._np
        cfg = self._config
        total_samples = audio.size
        window_samples = max(1, int(sample_rate * cfg.analysis_window_ms / 1000))
        min_silence_samples = int(sample_rate * cfg.min_silence_duration_ms / 1000)
        threshold_linear = 10 ** (cfg.silence_threshold_db / 20)

        if total_samples <= min_silence_samples or window_samples <= 0:
            return 0, total_samples

        window_sizes: list[int] = []
        rms_values: list[float] = []
        index = 0
        while index < total_samples:
            span = min(window_samples, total_samples - index)
            segment = audio[index : index + span]
            rms = float(np.sqrt(np.mean(segment * segment))) if span > 0 else 0.0
            window_sizes.append(span)
            rms_values.append(rms)
            index += span

        leading_samples = self._count_leading_silence(window_sizes, rms_values, threshold_linear)
        trailing_samples = self._count_trailing_silence(window_sizes, rms_values, threshold_linear)

        if leading_samples < min_silence_samples:
            leading_samples = 0
        if trailing_samples < min_silence_samples:
            trailing_samples = 0
        if leading_samples + trailing_samples >= total_samples:
            return 0, total_samples
        return leading_samples, total_samples - trailing_samples

    def _count_leading_silence(
        self,
        window_sizes: list[int],
        rms_values: list[float],
        threshold: float,
    ) -> int:
        samples = 0
        for size, rms in zip(window_sizes, rms_values, strict=False):
            if rms >= threshold:
                break
            samples += size
        return samples

    def _count_trailing_silence(
        self,
        window_sizes: list[int],
        rms_values: list[float],
        threshold: float,
    ) -> int:
        samples = 0
        for size, rms in zip(reversed(window_sizes), reversed(rms_values), strict=False):
            if rms >= threshold:
                break
            samples += size
        return samples

    def _encode_aac(self, audio: Any, sample_rate: int, bit_rate: int) -> bytes:
        np = self._np
        output_buffer = io.BytesIO()
        trimmed = np.clip(audio, -1.0, 1.0).astype(np.float32, copy=False)
        try:
            with self._av.open(output_buffer, mode="w", format="mp4") as container:
                stream = container.add_stream("aac", rate=sample_rate)
                stream.layout = "mono"
                stream.time_base = Fraction(1, sample_rate)
                if bit_rate:
                    stream.bit_rate = bit_rate
                frame_size = stream.codec_context.frame_size or 1024
                position = 0
                while position < trimmed.size:
                    chunk = trimmed[position : position + frame_size]
                    frame = self._create_audio_frame(chunk, sample_rate)
                    frame.pts = position
                    for packet in stream.encode(frame):
                        container.mux(packet)
                    position += chunk.size
                # Flush encoder
                for packet in stream.encode(None):
                    container.mux(packet)
        except self._av_error_types as exc:
            message = f"Failed to encode trimmed audio ({exc.__class__.__name__}: {exc})"
            raise AudioProcessingError(message) from exc
        return output_buffer.getvalue()

    def _create_audio_frame(self, chunk: Any, sample_rate: int) -> Any:
        np = self._np
        frame_data = chunk.reshape(1, -1)
        try:
            frame = self._av.AudioFrame.from_ndarray(frame_data, layout="mono")
        except ValueError as exc:
            message = str(exc).lower()
            if "int16" in message or "s16" in message:
                scaled = np.clip(frame_data, -1.0, 1.0)
                int_frame = np.round(scaled * 32767.0).astype(np.int16, copy=False)
                frame = self._av.AudioFrame.from_ndarray(int_frame, layout="mono")
            else:
                raise AudioProcessingError(
                    f"Failed to create audio frame ({exc.__class__.__name__}: {exc})"
                ) from exc
        frame.sample_rate = sample_rate
        frame.time_base = Fraction(1, sample_rate)
        return frame

    def _resolve_error_types(self, av: Any) -> tuple[type[BaseException], ...]:
        error_types: list[type[BaseException]] = []
        maybe_av_error = getattr(av, "AVError", None)
        if isinstance(maybe_av_error, type) and issubclass(maybe_av_error, BaseException):
            error_types.append(maybe_av_error)
        try:
            av_error_module = importlib.import_module("av.error")
        except ImportError:  # pragma: no cover - optional module
            av_error_module = None
        if av_error_module is not None:
            module_error = getattr(av_error_module, "AVError", None)
            if isinstance(module_error, type) and issubclass(module_error, BaseException):
                error_types.append(module_error)
        if not error_types:
            error_types.append(Exception)
        return tuple(dict.fromkeys(error_types))


__all__ = ["PyAvSilenceTrimmer"]
