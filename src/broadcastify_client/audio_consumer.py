"""Audio consumer that downloads call audio for downstream processing."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Protocol

from .audio_processing import AudioProcessingError, AudioProcessor, NullAudioProcessor
from .errors import AudioDownloadError
from .models import AudioPayloadEvent, LiveCallEnvelope
from .telemetry import AudioErrorEvent, NullTelemetrySink, TelemetrySink

logger = logging.getLogger(__name__)


class AudioDownloader(Protocol):
    """Protocol describing the interface for downloading call audio."""

    async def fetch_audio(
        self, call: LiveCallEnvelope
    ) -> AsyncIterator[AudioPayloadEvent]:  # pragma: no cover - protocol
        """Yield audio payload events for *call*."""
        ...


class AudioConsumer:
    """Consumes call events and emits audio payload events."""

    def __init__(
        self,
        downloader: AudioDownloader,
        *,
        telemetry: TelemetrySink | None = None,
        processor: AudioProcessor | None = None,
    ) -> None:
        """Create an AudioConsumer backed by *downloader*."""
        self._downloader = downloader
        self._telemetry = telemetry or NullTelemetrySink()
        self._processor = processor or NullAudioProcessor()

    async def consume(
        self, call_event: LiveCallEnvelope, queue: asyncio.Queue[AudioPayloadEvent]
    ) -> None:
        """Download audio for *call_event* and enqueue the resulting event."""
        logger.debug(
            "Starting audio download for call %s (system %s, talkgroup %s)",
            call_event.call.call_id,
            call_event.call.system_id,
            call_event.call.talkgroup_id,
        )
        try:
            audio_stream = await self._downloader.fetch_audio(call_event)
            async for event in audio_stream:
                processed_event = event
                try:
                    processed_event = await self._processor.process(event)
                except AudioProcessingError as exc:
                    logger.warning(
                        "Audio processing failed for call %s (system %s, talkgroup %s): %s",
                        call_event.call.call_id,
                        call_event.call.system_id,
                        call_event.call.talkgroup_id,
                        exc,
                    )
                    self._telemetry.record_event(
                        AudioErrorEvent(
                            call_id=call_event.call.call_id,
                            system_id=call_event.call.system_id,
                            talkgroup_id=call_event.call.talkgroup_id,
                            error_type=exc.__class__.__name__,
                            message=str(exc),
                        )
                    )
                except Exception as exc:  # pragma: no cover - defensive path
                    logger.exception(
                        "Unexpected audio processing failure for call %s (system %s, talkgroup %s)",
                        call_event.call.call_id,
                        call_event.call.system_id,
                        call_event.call.talkgroup_id,
                    )
                    self._telemetry.record_event(
                        AudioErrorEvent(
                            call_id=call_event.call.call_id,
                            system_id=call_event.call.system_id,
                            talkgroup_id=call_event.call.talkgroup_id,
                            error_type=exc.__class__.__name__,
                            message=str(exc),
                        )
                    )
                    processed_event = event

                await queue.put(processed_event)
                if processed_event.finished:
                    break
        except AudioDownloadError as exc:
            logger.warning(
                "Download error for call %s (system %s, talkgroup %s): %s",
                call_event.call.call_id,
                call_event.call.system_id,
                call_event.call.talkgroup_id,
                exc,
            )
            raise
        except Exception as exc:  # pragma: no cover - defensive path
            logger.exception(
                "Unexpected error while downloading audio for call %s (system %s, talkgroup %s)",
                call_event.call.call_id,
                call_event.call.system_id,
                call_event.call.talkgroup_id,
            )
            raise AudioDownloadError(str(exc)) from exc
