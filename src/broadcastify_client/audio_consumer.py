"""Audio consumer that downloads call audio for downstream processing."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Protocol

from .errors import AudioDownloadError
from .models import AudioChunkEvent, CallEvent
from .telemetry import NullTelemetrySink, TelemetrySink


class AudioDownloader(Protocol):
    """Protocol describing the interface for downloading audio chunks."""

    async def fetch_audio(
        self, call: CallEvent
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

    async def consume(self, call_event: CallEvent, queue: asyncio.Queue[AudioChunkEvent]) -> None:
        """Download audio for *call_event* and enqueue resulting chunks."""

        try:
            audio_stream = await self._downloader.fetch_audio(call_event)
            async for chunk in audio_stream:
                await queue.put(chunk)
                if chunk.finished:
                    break
        except AudioDownloadError:
            raise
        except Exception as exc:  # pragma: no cover - defensive path
            raise AudioDownloadError(str(exc)) from exc
