"""Audio processing interfaces for Broadcastify audio pipelines."""

from __future__ import annotations

from typing import Protocol

from .errors import BroadcastifyError
from .models import AudioPayloadEvent


class AudioProcessingError(BroadcastifyError):
    """Raised when audio processing fails."""


class AudioProcessor(Protocol):
    """Protocol describing asynchronous audio post-processing."""

    async def process(
        self, event: AudioPayloadEvent
    ) -> AudioPayloadEvent:  # pragma: no cover - protocol
        """Return the processed `AudioPayloadEvent` for downstream consumption."""
        ...


class NullAudioProcessor:
    """Audio processor that returns payloads unchanged."""

    async def process(self, event: AudioPayloadEvent) -> AudioPayloadEvent:
        """Return the input event without modification."""
        return event


__all__ = [
    "AudioProcessingError",
    "AudioProcessor",
    "NullAudioProcessor",
]
