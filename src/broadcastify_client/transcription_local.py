"""Local Whisper transcription backend using the faster-whisper package.

This backend retains the generic 'whisper' naming for compatibility.
"""

from __future__ import annotations

import asyncio
import importlib
import io
import logging
import tempfile
from collections.abc import AsyncIterator
from typing import Any, cast

from .audio_segmentation import AudioSegment, AudioSegmentationError, AudioSegmenter
from .config import TranscriptionConfig
from .errors import TranscriptionError
from .models import AudioPayloadEvent, TranscriptionResult

logger = logging.getLogger(__name__)


_MODEL_ALIASES: dict[str, str] = {"whisper-1": "large-v3"}


class LocalWhisperBackend:
    """Transcription backend powered by a locally hosted faster-whisper model."""

    def __init__(self, config: TranscriptionConfig) -> None:
        """Initialise bookkeeping and validate local dependencies."""
        self._config = config
        try:
            module = importlib.import_module("faster_whisper")
        except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency missing
            raise TranscriptionError(
                "faster-whisper package not installed. "
                "Install with the 'transcription-local' extra."
            ) from exc
        self._whisper_model_cls = getattr(module, "WhisperModel", None)
        if self._whisper_model_cls is None:  # pragma: no cover - unexpected API surface
            raise TranscriptionError("faster_whisper.WhisperModel not found in installed package")
        self._model_name = _MODEL_ALIASES.get(config.model, config.model)
        self._model: object | None = None
        self._load_lock = asyncio.Lock()
        self._model_lock = asyncio.Lock()

    # streaming partials removed

    async def finalize(
        self, audio_stream: AsyncIterator[AudioPayloadEvent]
    ) -> AsyncIterator[TranscriptionResult]:
        """Yield transcription results for the audio stream, with optional segmentation."""
        if not self._config.whisper_segmentation_enabled:
            # Use existing single-result logic when segmentation disabled
            async for result in self._finalize_single(audio_stream):
                yield result
            return

        # Segmented transcription logic
        async for event in audio_stream:
            try:
                segments = self._segment_audio_by_silence(event)
                total_segments = len(segments)

                # Log segmentation info
                if total_segments > 1:
                    logger.info(
                        "Audio segmented into %d parts for call %s (duration: %.2fs)",
                        total_segments,
                        event.call_id,
                        event.end_offset - event.start_offset,
                    )
                elif total_segments == 1:
                    logger.debug(
                        "Audio kept as single segment for call %s (duration: %.2fs)",
                        event.call_id,
                        event.end_offset - event.start_offset,
                    )

                if total_segments == 0:
                    # No segments created, yield empty result
                    yield TranscriptionResult(
                        call_id=event.call_id,
                        text="",
                        language=self._config.language or "",
                        average_logprob=None,
                        segments=(),
                        segment_id=0,
                        total_segments=0,
                        segment_start_time=event.start_offset,
                    )
                    continue

                # Process each segment individually
                for segment in segments:
                    try:
                        text = await self._transcribe_bytes(segment.payload)
                        yield TranscriptionResult(
                            call_id=event.call_id,
                            segment_id=segment.segment_id,
                            total_segments=total_segments,
                            text=text,
                            language=self._config.language or "",
                            average_logprob=None,
                            segments=(),
                            segment_start_time=segment.start_offset,
                        )
                    except TranscriptionError as exc:
                        logger.warning(
                            "Segment %d transcription failed for call %s: %s",
                            segment.segment_id,
                            event.call_id,
                            exc,
                        )
                        # Continue with other segments by yielding empty result
                        yield TranscriptionResult(
                            call_id=event.call_id,
                            segment_id=segment.segment_id,
                            total_segments=total_segments,
                            text="",
                            language=self._config.language or "",
                            average_logprob=None,
                            segments=(),
                            segment_start_time=segment.start_offset,
                        )
            except AudioSegmentationError as exc:
                logger.warning("Audio segmentation failed for call %s: %s", event.call_id, exc)
                # Fall back to single transcription
                async for result in self._finalize_single(_single_event_stream(event)):
                    yield result

    async def _finalize_single(
        self, audio_stream: AsyncIterator[AudioPayloadEvent]
    ) -> AsyncIterator[TranscriptionResult]:
        """Return a single transcription result for the entire audio stream (legacy behavior)."""
        buffer = io.BytesIO()
        call_id: str | None = None
        async for event in audio_stream:
            call_id = event.call_id
            buffer.write(event.payload)
        if call_id is None:
            yield TranscriptionResult(
                call_id="",
                text="",
                language=self._config.language or "",
                average_logprob=None,
                segments=(),
            )
            return

        payload = buffer.getvalue()
        min_bytes = int(self._config.min_batch_bytes)
        if len(payload) < min_bytes:
            logger.debug(
                "Skipping final local transcription: bytes=%d below threshold=%d",
                len(payload),
                min_bytes,
            )
            text = ""
        else:
            try:
                text = await self._transcribe_bytes(payload)
            except TranscriptionError as exc:
                logger.warning("Local transcription failed (non-fatal): %s", exc)
                text = ""

        yield TranscriptionResult(
            call_id=call_id,
            text=text,
            language=self._config.language or "",
            average_logprob=None,
            segments=(),
        )

    def _segment_audio_by_silence(self, event: AudioPayloadEvent) -> list[AudioSegment]:
        """Segment audio by silence using the AudioSegmenter."""
        try:
            segmenter = AudioSegmenter(self._config)
            return list(segmenter.segment_event(event))
        except AudioSegmentationError as exc:
            logger.error("Failed to segment audio for call %s: %s", event.call_id, exc)
            raise

    async def _ensure_model(self) -> object:
        if self._model is not None:
            return self._model
        async with self._load_lock:
            if self._model is None:
                try:
                    model_cls = cast(Any, self._whisper_model_cls)
                    self._model = await asyncio.to_thread(
                        model_cls,
                        self._model_name,
                        device=self._config.device,
                        compute_type=self._config.compute_type,
                    )
                except Exception as exc:  # pragma: no cover - propagate load errors
                    raise TranscriptionError(
                        f"Unable to load Whisper model '{self._model_name}': {exc}"
                    ) from exc
        return self._model

    async def _transcribe_bytes(self, payload: bytes) -> str:
        if not payload:
            return ""
        model = await self._ensure_model()
        async with self._model_lock:
            try:
                result = await asyncio.to_thread(self._transcribe_sync, model, payload)
            except Exception as exc:  # pragma: no cover - defensive
                raise TranscriptionError(f"Local Whisper transcription failed: {exc}") from exc
        return result

    def _transcribe_sync(self, model: object, payload: bytes) -> str:
        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=True) as tmp:
            tmp.write(payload)
            tmp.flush()
            try:
                segments, _info = model.transcribe(  # type: ignore[attr-defined]
                    tmp.name,
                    language=self._config.language,
                    condition_on_previous_text=False,
                )
            except AttributeError as exc:  # pragma: no cover - unexpected API surface
                raise TranscriptionError(
                    "Local Whisper model does not expose transcribe()"
                ) from exc
        parts: list[str] = []
        segments = cast(list[Any], segments)
        for seg in segments:
            seg_text = getattr(seg, "text", None)
            if isinstance(seg_text, str):
                cleaned = seg_text.strip()
                if cleaned:
                    parts.append(cleaned)
        text = " ".join(parts).strip()
        return text


def _single_event_stream(event: AudioPayloadEvent) -> AsyncIterator[AudioPayloadEvent]:
    """Create a single-event stream for fallback scenarios."""

    async def generator() -> AsyncIterator[AudioPayloadEvent]:
        yield event

    return generator()
