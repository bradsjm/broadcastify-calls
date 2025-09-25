"""Local Whisper transcription backend using the open-source whisper package."""

from __future__ import annotations

import asyncio
import importlib
import io
import logging
import tempfile
from collections.abc import AsyncIterator, Callable, Mapping
from typing import cast

from .config import TranscriptionConfig
from .errors import TranscriptionError
from .models import AudioChunkEvent, TranscriptionPartial, TranscriptionResult

logger = logging.getLogger(__name__)


_MODEL_ALIASES: dict[str, str] = {"whisper-1": "large-v3"}


class LocalWhisperBackend:
    """Transcription backend powered by a locally hosted Whisper model."""

    def __init__(self, config: TranscriptionConfig) -> None:
        """Initialise bookkeeping and validate local dependencies."""
        self._config = config
        try:
            self._whisper = importlib.import_module("whisper")
        except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency missing
            raise TranscriptionError(
                "whisper package not installed. Install with the 'transcription-local' extra."
            ) from exc

        self._model_name = _MODEL_ALIASES.get(config.model, config.model)
        self._model: object | None = None
        self._load_lock = asyncio.Lock()
        self._model_lock = asyncio.Lock()

    async def stream_transcription(
        self, audio_stream: AsyncIterator[AudioChunkEvent]
    ) -> AsyncIterator[TranscriptionPartial]:
        """Yield partial transcriptions for chunks received from *audio_stream*."""
        buffer = io.BytesIO()
        has_buffer = False
        start_time = 0.0
        end_time = 0.0
        chunk_index = 0

        async for chunk in audio_stream:
            if not has_buffer:
                start_time = chunk.start_offset
                has_buffer = True
            end_time = chunk.end_offset
            buffer.write(chunk.payload)
            duration = end_time - start_time
            should_flush = duration >= float(self._config.chunk_seconds) or chunk.finished
            if not should_flush:
                continue

            payload = buffer.getvalue()
            buffer = io.BytesIO()
            has_buffer = False
            duration = end_time - start_time
            if duration < float(self._config.min_batch_seconds) or len(payload) < int(
                self._config.min_batch_bytes
            ):
                logger.debug(
                    "Skipping local transcription batch: duration=%.3fs bytes=%d below thresholds",
                    duration,
                    len(payload),
                )
                continue
            try:
                text = await self._transcribe_bytes(payload)
            except TranscriptionError as exc:
                logger.warning("Local transcription batch failed (non-fatal): %s", exc)
                text = ""
            if text:
                yield TranscriptionPartial(
                    call_id=chunk.call_id,
                    chunk_index=chunk_index,
                    start_time=start_time,
                    end_time=end_time,
                    text=text,
                    confidence=None,
                )
                chunk_index += 1

    async def finalize(self, audio_stream: AsyncIterator[AudioChunkEvent]) -> TranscriptionResult:
        """Return a final transcription result for the entirety of *audio_stream*."""
        buffer = io.BytesIO()
        call_id: str | None = None
        async for chunk in audio_stream:
            call_id = chunk.call_id
            buffer.write(chunk.payload)
        if call_id is None:
            return TranscriptionResult(
                call_id="",
                text="",
                language=self._config.language or "",
                average_logprob=None,
                segments=(),
            )

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

        return TranscriptionResult(
            call_id=call_id,
            text=text,
            language=self._config.language or "",
            average_logprob=None,
            segments=(),
        )

    async def _ensure_model(self) -> object:
        if self._model is not None:
            return self._model
        async with self._load_lock:
            if self._model is None:
                try:
                    self._model = await asyncio.to_thread(
                        self._whisper.load_model, self._model_name
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
                transcribe = model.transcribe  # type: ignore[attr-defined]
            except AttributeError as exc:  # pragma: no cover - unexpected API surface
                raise TranscriptionError(
                    "Local Whisper model does not expose transcribe()"
                ) from exc
            typed_transcribe = cast(
                Callable[..., Mapping[str, object]],
                transcribe,
            )
            result = typed_transcribe(
                tmp.name,
                language=self._config.language,
                verbose=False,
                condition_on_previous_text=False,
            )
        text = ""
        raw_text = result.get("text")
        if isinstance(raw_text, str):
            text = raw_text.strip()
        return text
