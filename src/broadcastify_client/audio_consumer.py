"""Audio consumer that downloads call audio for downstream processing."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Protocol

from .errors import AudioDownloadError
from .models import AudioChunkEvent, LiveCallEnvelope
from .telemetry import NullTelemetrySink, TelemetrySink

logger = logging.getLogger(__name__)


class AudioDownloader(Protocol):
    """Protocol describing the interface for downloading audio chunks."""

    async def fetch_audio(
        self, call: LiveCallEnvelope
    ) -> AsyncIterator[AudioChunkEvent]:  # pragma: no cover - protocol
        """Yield audio chunks for *call*."""
        ...


class AudioConsumer:
    """Consumes call events and emits audio chunk events."""

    def __init__(
        self,
        downloader: AudioDownloader,
        *,
        telemetry: TelemetrySink | None = None,
    ) -> None:
        """Create an AudioConsumer backed by *downloader*."""
        self._downloader = downloader
        self._telemetry = telemetry or NullTelemetrySink()

    async def consume(
        self, call_event: LiveCallEnvelope, queue: asyncio.Queue[AudioChunkEvent]
    ) -> None:
        """Download audio for *call_event* and enqueue resulting chunks."""
        logger.debug(
            "Starting audio download for call %s (system %s, talkgroup %s)",
            call_event.call.call_id,
            call_event.call.system_id,
            call_event.call.talkgroup_id,
        )
        try:
            audio_stream = await self._downloader.fetch_audio(call_event)
            async for chunk in audio_stream:
                await queue.put(chunk)
                if chunk.finished:
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
