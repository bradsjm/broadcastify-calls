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

_RMS_EPSILON = 1e-12
_NORMALIZATION_NOOP_TOLERANCE = 1e-3


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
        self._trim_enabled = config.trim_enabled
        self._band_pass_enabled = config.band_pass_enabled
        self._filter_module = None
        self._blocking_errors = self._resolve_named_errors(av, "BlockingIOError")
        self._eof_errors = self._resolve_named_errors(av, "EOFError")
        if self._band_pass_enabled:
            try:
                self._filter_module = importlib.import_module("av.filter")
            except ImportError as exc:  # pragma: no cover - optional module
                self._logger.warning(
                    "Band-pass filter disabled: PyAV filter module unavailable (%s)",
                    exc,
                )
                self._band_pass_enabled = False
            else:
                if not hasattr(self._filter_module, "Graph"):
                    self._logger.warning(
                        "Band-pass filter disabled: av.filter.Graph missing from PyAV",
                    )
                    self._band_pass_enabled = False

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

        (
            trimmed,
            leading_seconds,
            trailing_seconds,
            trim_applied,
            skip_processing,
        ) = self._prepare_trimmed_audio(audio, sample_rate)
        if skip_processing or trimmed.size == 0:
            return event

        trimmed = self._maybe_apply_band_pass(trimmed, sample_rate, event)

        trimmed_seconds = trimmed.size / sample_rate
        start_offset = (
            event.start_offset + leading_seconds if trim_applied else event.start_offset
        )
        end_offset = (
            max(start_offset, event.end_offset - trailing_seconds)
            if trim_applied
            else event.end_offset
        )

        output_payload = self._encode_aac(trimmed, sample_rate, bit_rate)
        new_event = AudioPayloadEvent(
            call_id=event.call_id,
            sequence=event.sequence,
            start_offset=start_offset,
            end_offset=end_offset,
            payload=output_payload,
            content_type=event.content_type,
            finished=event.finished,
        )
        if trim_applied and (leading_seconds or trailing_seconds):
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

    def _prepare_trimmed_audio(
        self, audio: Any, sample_rate: int
    ) -> tuple[Any, float, float, bool, bool]:
        trimmed = audio
        leading_seconds = 0.0
        trailing_seconds = 0.0
        trim_applied = False
        skip_processing = False

        if not self._trim_enabled:
            skip_processing = not self._band_pass_enabled
            return trimmed, leading_seconds, trailing_seconds, trim_applied, skip_processing

        trim_start, trim_end = self._determine_trim_bounds(audio, sample_rate)
        if trim_start == 0 and trim_end == audio.size:
            skip_processing = not self._band_pass_enabled
        elif trim_start >= trim_end:
            self._logger.debug(
                "All samples evaluated as silence; keeping original payload"
            )
            skip_processing = True
        else:
            candidate = audio[trim_start:trim_end]
            if candidate.size == 0:
                if self._band_pass_enabled:
                    trimmed = audio
                else:
                    self._logger.debug(
                        "All samples evaluated as silence; keeping original payload"
                    )
                    skip_processing = True
            else:
                trimmed = candidate
                trim_applied = True
                leading_seconds = trim_start / sample_rate
                trailing_seconds = (audio.size - trim_end) / sample_rate

        return trimmed, leading_seconds, trailing_seconds, trim_applied, skip_processing

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
        frame_data = chunk.reshape(1, -1)
        frame: Any
        try:
            frame = self._av.AudioFrame.from_ndarray(
                frame_data,
                layout="mono",
                format="fltp",
            )
        except TypeError:
            frame = self._create_audio_frame_without_format(frame_data)
        except ValueError as exc:
            frame = self._create_audio_frame_from_int16(frame_data, exc)
        return self._finalize_frame(frame, sample_rate)

    def _create_audio_frame_without_format(self, frame_data: Any) -> Any:
        try:
            return self._av.AudioFrame.from_ndarray(frame_data, layout="mono")
        except ValueError as exc:
            return self._create_audio_frame_from_int16(frame_data, exc)

    def _create_audio_frame_from_int16(self, frame_data: Any, error: BaseException) -> Any:
        np = self._np
        message = str(error).lower()
        if "int16" not in message and "s16" not in message:
            raise AudioProcessingError(
                f"Failed to create audio frame ({error.__class__.__name__}: {error})"
            ) from error
        scaled = np.clip(frame_data, -1.0, 1.0)
        int_frame = np.round(scaled * 32767.0).astype(np.int16, copy=False)
        return self._av.AudioFrame.from_ndarray(int_frame, layout="mono")

    def _finalize_frame(self, frame: Any, sample_rate: int) -> Any:
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

    def _resolve_named_errors(self, av: Any, *names: str) -> tuple[type[BaseException], ...]:
        """Return exception types referenced by *names* across av modules."""
        modules: list[Any] = [av]
        try:
            modules.append(importlib.import_module("av.error"))
        except ImportError:  # pragma: no cover - optional module
            pass
        resolved: list[type[BaseException]] = []
        for module in modules:
            for name in names:
                candidate = getattr(module, name, None)
                if isinstance(candidate, type) and issubclass(candidate, BaseException):
                    resolved.append(candidate)
        return tuple(dict.fromkeys(resolved))

    def _maybe_apply_band_pass(
        self, audio: Any, sample_rate: int, event: AudioPayloadEvent
    ) -> Any:
        """Apply band-pass filtering when enabled, logging attenuation metrics."""
        if not self._band_pass_enabled or audio.size == 0:
            return audio

        np = self._np
        rms_before = float(np.sqrt(np.mean(audio * audio))) if audio.size else 0.0
        try:
            filtered = self._apply_band_pass(audio, sample_rate)
        except AudioProcessingError as exc:
            self._logger.warning(
                "Band-pass filter fallback for call %s: %s",
                event.call_id,
                exc,
            )
            return audio

        if filtered.size == 0:
            self._logger.warning(
                "Band-pass filter produced empty output for call %s; using trimmed audio",
                event.call_id,
            )
            return audio

        rms_filtered = float(np.sqrt(np.mean(filtered * filtered))) if filtered.size else 0.0

        normalization_gain_db = 0.0
        if self._config.normalization_enabled and filtered.size:
            filtered, normalization_gain_db = self._apply_normalization(filtered, np)

        rms_after = float(np.sqrt(np.mean(filtered * filtered))) if filtered.size else 0.0
        attenuation_db = 0.0
        if rms_before > 0.0 and rms_after > 0.0:
            attenuation_db = 20.0 * np.log10(rms_after / rms_before)

        self._logger.info(
            (
                "Applied band-pass filter to call %s (%.0f-%.0f Hz, rms_before=%.6f, "
                "rms_filtered=%.6f, normalization_gain_db=%.2f, rms_after=%.6f, delta_db=%.2f)"
            ),
            event.call_id,
            self._config.low_cut_hz,
            self._config.high_cut_hz,
            rms_before,
            rms_filtered,
            normalization_gain_db,
            rms_after,
            attenuation_db,
        )
        return filtered

    def _apply_band_pass(self, audio: Any, sample_rate: int) -> Any:
        """Return *audio* filtered through a PyAV band-pass graph."""
        if not self._band_pass_enabled:
            return audio
        if self._filter_module is None:
            raise AudioProcessingError("PyAV filter module unavailable")

        graph = self._configure_band_pass_graph(sample_rate)
        frame_size = 1024
        np = self._np

        filtered_parts: list[Any] = []
        position = 0
        while position < audio.size:
            chunk = audio[position : position + frame_size]
            frame = self._create_audio_frame(chunk, sample_rate)
            frame.pts = position
            try:
                graph.push(frame)
            except self._av_error_types as exc:
                message = f"Failed to push audio frame into band-pass graph ({exc})"
                raise AudioProcessingError(message) from exc
            filtered_parts.extend(self._drain_band_pass_graph(graph))
            position += chunk.size

        try:
            graph.push(None)
        except self._av_error_types as exc:
            if not self._is_blocking_exception(exc) and not self._is_eof_exception(exc):
                message = f"Failed to flush band-pass graph ({exc})"
                raise AudioProcessingError(message) from exc

        filtered_parts.extend(self._drain_band_pass_graph(graph))

        if not filtered_parts:
            return np.array([], dtype=np.float32)
        return np.concatenate(filtered_parts).astype(np.float32, copy=False)

    def _apply_normalization(self, audio: Any, np_module: Any) -> tuple[Any, float]:
        """Return audio scaled toward the configured RMS target and gain amount."""
        rms = float(np_module.sqrt(np_module.mean(audio * audio))) if audio.size else 0.0
        if rms <= _RMS_EPSILON:
            return audio, 0.0

        target_dbfs = self._config.normalization_target_dbfs
        target_rms = 10.0 ** (target_dbfs / 20.0)
        if target_rms <= 0.0:
            return audio, 0.0

        scale = target_rms / rms
        if scale <= 0.0:
            return audio, 0.0

        max_gain_linear = 10.0 ** (self._config.normalization_max_gain_db / 20.0)
        scale = min(scale, max_gain_linear)

        peak = float(np_module.max(np_module.abs(audio))) if audio.size else 0.0
        if peak > 0.0:
            clip_scale = 0.999 / peak
            scale = min(scale, clip_scale)

        if abs(scale - 1.0) <= _NORMALIZATION_NOOP_TOLERANCE:
            return audio, 0.0

        normalized = (audio * scale).astype(np_module.float32, copy=False)
        gain_db = 20.0 * np_module.log10(scale) if scale > 0.0 else 0.0
        return normalized, gain_db

    def _configure_band_pass_graph(self, sample_rate: int) -> Any:
        if self._filter_module is None:
            raise AudioProcessingError("PyAV filter module unavailable")
        graph = self._filter_module.Graph()
        low_cut = self._config.low_cut_hz
        high_cut = self._config.high_cut_hz
        try:
            source = graph.add(
                "abuffer",
                args=(
                    f"sample_rate={sample_rate}:sample_fmt=fltp:"
                    f"channel_layout=mono:time_base=1/{sample_rate}"
                ),
            )
            highpass_primary = graph.add("highpass", args=f"f={low_cut}:poles=2")
            highpass_secondary = graph.add("highpass", args=f"f={low_cut}:poles=2")
            lowpass_primary = graph.add("lowpass", args=f"f={high_cut}:poles=2")
            lowpass_secondary = graph.add("lowpass", args=f"f={high_cut}:poles=2")

            nodes: list[Any] = [
                source,
                highpass_primary,
                highpass_secondary,
            ]

            if self._config.notch_enabled:
                notch_args = (
                    f"f={self._config.notch_center_hz}:t=q:w={self._config.notch_q}:"
                    f"g={self._config.notch_gain_db}"
                )
                notch = graph.add("equalizer", args=notch_args)
                nodes.append(notch)

            nodes.extend([lowpass_primary, lowpass_secondary, graph.add("abuffersink")])

            graph.link_nodes(*nodes).configure()
        except self._av_error_types as exc:
            message = f"Failed to configure band-pass graph ({exc.__class__.__name__}: {exc})"
            raise AudioProcessingError(message) from exc
        return graph

    def _drain_band_pass_graph(self, graph: Any) -> list[Any]:
        drained: list[Any] = []
        while True:
            try:
                filtered_frame = graph.pull()
            except self._av_error_types as exc:
                if self._is_blocking_exception(exc) or self._is_eof_exception(exc):
                    break
                message = f"Failed to pull filtered audio ({exc.__class__.__name__}: {exc})"
                raise AudioProcessingError(message) from exc
            except Exception as exc:  # pragma: no cover - defensive
                if self._is_blocking_exception(exc) or self._is_eof_exception(exc):
                    break
                raise AudioProcessingError(
                    f"Unexpected error pulling from band-pass graph ({exc})"
                ) from exc
            if filtered_frame is None:
                break
            drained.append(self._frame_to_mono(filtered_frame))
        return drained

    def _is_blocking_exception(self, exc: BaseException) -> bool:
        return bool(self._blocking_errors) and isinstance(exc, self._blocking_errors)

    def _is_eof_exception(self, exc: BaseException) -> bool:
        return bool(self._eof_errors) and isinstance(exc, self._eof_errors)


__all__ = ["PyAvSilenceTrimmer"]
