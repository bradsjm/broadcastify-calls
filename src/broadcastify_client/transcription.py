"""Transcription pipeline consuming audio chunks."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Protocol

from .errors import TranscriptionError
from .models import AudioChunkEvent, TranscriptionPartial, TranscriptionResult
from .telemetry import NullTelemetrySink, TelemetrySink

logger = logging.getLogger(__name__)


class TranscriptionBackend(Protocol):
    """Protocol implemented by transcription service integrations."""

    def stream_transcription(
        self, audio_stream: AsyncIterator[AudioChunkEvent]
    ) -> AsyncIterator[TranscriptionPartial]:  # pragma: no cover - protocol
        """Yield partial transcriptions for the provided audio stream."""
        ...

    async def finalize(
        self, audio_stream: AsyncIterator[AudioChunkEvent]
    ) -> TranscriptionResult:  # pragma: no cover - protocol
        """Return the final transcription result for the provided audio stream."""
        ...


class TranscriptionPipeline:
    """Coordinates transcription of audio chunks using a backend provider."""

    def __init__(
        self,
        backend: TranscriptionBackend,
        *,
        telemetry: TelemetrySink | None = None,
    ) -> None:
        """Initialise the pipeline with a transcription *backend*."""
        self._backend = backend
        self._telemetry = telemetry or NullTelemetrySink()

    async def transcribe_stream(
        self, audio_stream: AsyncIterator[AudioChunkEvent]
    ) -> AsyncIterator[TranscriptionPartial]:
        """Yield transcription partials for *audio_stream*."""
        try:
            logger.debug("Starting streaming transcription")
            partial_stream = self._backend.stream_transcription(audio_stream)
            async for partial in partial_stream:
                yield partial
        except TranscriptionError:
            logger.warning("Streaming transcription backend raised TranscriptionError")
            raise
        except Exception as exc:  # pragma: no cover - defensive path
            logger.exception("Unexpected streaming transcription failure")
            raise TranscriptionError(str(exc)) from exc

    async def transcribe_final(
        self, audio_stream: AsyncIterator[AudioChunkEvent]
    ) -> TranscriptionResult:
        """Return the final transcription result for *audio_stream*."""
        try:
            logger.debug("Starting final transcription")
            return await self._backend.finalize(audio_stream)
        except TranscriptionError:
            logger.warning("Final transcription backend raised TranscriptionError")
            raise
        except Exception as exc:  # pragma: no cover - defensive path
            logger.exception("Unexpected final transcription failure")
            raise TranscriptionError(str(exc)) from exc
