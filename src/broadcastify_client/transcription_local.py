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

    async def finalize(self, audio_stream: AsyncIterator[AudioPayloadEvent]) -> TranscriptionResult:
        """Return a final transcription result for the entirety of *audio_stream*."""
        buffer = io.BytesIO()
        call_id: str | None = None
        async for event in audio_stream:
            call_id = event.call_id
            buffer.write(event.payload)
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
