"""Transcription pipeline consuming call audio events."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Protocol

from .errors import TranscriptionError
from .models import AudioPayloadEvent, TranscriptionResult
from .telemetry import NullTelemetrySink, TelemetrySink

logger = logging.getLogger(__name__)


class TranscriptionBackend(Protocol):
    """Protocol implemented by transcription service integrations (final-only)."""

    def finalize(
        self, audio_stream: AsyncIterator[AudioPayloadEvent]
    ) -> AsyncIterator[TranscriptionResult]:  # pragma: no cover - protocol
        """Yield transcription results for the provided audio stream."""
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
        self, audio_stream: AsyncIterator[AudioPayloadEvent]
    ) -> AsyncIterator[TranscriptionResult]:
        """Yield transcription results for *audio_stream*.

        The iterator typically yields a single :class:`AudioPayloadEvent` containing
        the entire call audio payload, but may yield multiple results when segmentation
        is enabled.
        """
        try:
            logger.debug("Starting final transcription")
            async for result in self._backend.finalize(audio_stream):
                yield result
        except TranscriptionError as exc:
            logger.error("Final transcription failed: %s", exc)
            raise
        except Exception as exc:  # pragma: no cover - defensive path
            logger.error("Final transcription failed: %s", exc)
            raise TranscriptionError(str(exc)) from exc
