"""Transcription pipeline consuming audio chunks."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Protocol

from .errors import TranscriptionError
from .models import AudioChunkEvent, TranscriptionResult
from .telemetry import NullTelemetrySink, TelemetrySink

logger = logging.getLogger(__name__)


class TranscriptionBackend(Protocol):
    """Protocol implemented by transcription service integrations (final-only)."""

    async def finalize(
        self, audio_stream: AsyncIterator[AudioChunkEvent]
    ) -> TranscriptionResult:  # pragma: no cover - protocol
        """Return the final transcription result for the provided audio stream."""
        ...


class TranscriptionPipeline:
    """Coordinates transcription using a backend provider (final-only)."""

    def __init__(
        self,
        backend: TranscriptionBackend,
        *,
        telemetry: TelemetrySink | None = None,
    ) -> None:
        """Initialise the pipeline with a transcription *backend*."""
        self._backend = backend
        self._telemetry = telemetry or NullTelemetrySink()

    async def transcribe_final(
        self, audio_stream: AsyncIterator[AudioChunkEvent]
    ) -> TranscriptionResult:
        """Return the final transcription result for *audio_stream*."""
        try:
            logger.debug("Starting final transcription")
            return await self._backend.finalize(audio_stream)
        except TranscriptionError as exc:
            logger.error("Final transcription failed: %s", exc)
            raise
        except Exception as exc:  # pragma: no cover - defensive path
            logger.error("Final transcription failed: %s", exc)
            raise TranscriptionError(str(exc)) from exc
