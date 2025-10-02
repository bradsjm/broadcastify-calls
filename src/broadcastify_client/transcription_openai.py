"""OpenAI-compatible Whisper transcription backend.

This backend targets the OpenAI transcription API surface and compatible providers
exposing the same interface. It implements the TranscriptionBackend protocol and
converts ``AudioPayloadEvent`` streams into Whisper requests using multipart form data.
"""

from __future__ import annotations

import importlib
import io
import logging
from collections.abc import AsyncIterator
from typing import Protocol, cast, runtime_checkable

from .config import TranscriptionConfig
from .errors import TranscriptionError
from .models import AudioPayloadEvent, TranscriptionResult

logger = logging.getLogger(__name__)


@runtime_checkable
class _TranscriptionsAPI(Protocol):
    async def create(
        self,
        *,
        model: str,
        file: tuple[str, io.BytesIO, str],
        language: str | None,
    ) -> object:  # pragma: no cover - protocol
        ...


@runtime_checkable
class _AudioAPI(Protocol):
    @property
    def transcriptions(self) -> _TranscriptionsAPI:  # pragma: no cover - protocol
        ...


@runtime_checkable
class OpenAIClientLike(Protocol):
    """Minimal protocol for the OpenAI async client used by this backend."""

    @property
    def audio(self) -> _AudioAPI:  # pragma: no cover - protocol
        """Return the audio API namespace exposing transcriptions.create()."""
        ...


class OpenAIWhisperBackend:
    """Transcription backend using an OpenAI-compatible Whisper endpoint."""

    def __init__(self, config: TranscriptionConfig) -> None:
        """Initialise backend from a validated TranscriptionConfig."""
        if not config.api_key:
            raise TranscriptionError("Transcription API key not configured")
        self._config = config
        client_kwargs: dict[str, str] = {"api_key": config.api_key}
        if config.endpoint is not None:
            client_kwargs["base_url"] = str(config.endpoint)
        # Dynamically import the OpenAI client to avoid hard import dependency.
        try:
            module = importlib.import_module("openai")
        except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency missing
            raise TranscriptionError(
                "openai package not installed. Install with the 'transcription' extra."
            ) from exc

        AsyncOpenAIClass = getattr(module, "AsyncOpenAI", None)
        if AsyncOpenAIClass is None:  # pragma: no cover - unexpected API surface
            raise TranscriptionError("openai.AsyncOpenAI not found in installed package")

        # Instantiate the provider client and narrow the type to our protocol.
        self._client = cast(OpenAIClientLike, AsyncOpenAIClass(**client_kwargs))
        if not isinstance(self._client, OpenAIClientLike):  # type: ignore[arg-type]
            # Best-effort guard; in practice AsyncOpenAI conforms.
            raise TranscriptionError("OpenAI client does not expose expected API surface")

    # streaming partials removed

    async def finalize(self, audio_stream: AsyncIterator[AudioPayloadEvent]) -> TranscriptionResult:
        """Return a single transcription result for the entire audio stream."""
        buffer = io.BytesIO()
        call_id: str | None = None
        last_mime: str | None = None
        async for event in audio_stream:
            call_id = event.call_id
            buffer.write(event.payload)
            last_mime = event.content_type or last_mime
        if call_id is None:
            # With no audio events, preserve API semantics by returning an empty result.
            return TranscriptionResult(
                call_id="",
                text="",
                language=self._config.language or "",
                average_logprob=None,
                segments=(),
            )

        data = buffer.getvalue()
        min_seconds = float(self._config.min_batch_seconds)
        min_bytes = int(self._config.min_batch_bytes)
        if min_seconds > 0.0 and len(data) < min_bytes:
            logger.debug(
                "Skipping final transcription: bytes=%d below threshold=%d",
                len(data),
                min_bytes,
            )
            text = ""
        else:
            text = await self._transcribe_bytes(data, content_type=last_mime)
        # For now, no segment-level partials are returned from finalize.
        return TranscriptionResult(
            call_id=call_id,
            text=text,
            language=self._config.language or "",
            average_logprob=None,
            segments=(),
        )

    async def _transcribe_bytes(self, data: bytes, *, content_type: str | None = None) -> str:
        """Call the provider transcription endpoint with the given bytes and return text.

        Uploads the aggregated audio as an AAC ``.m4a`` file using the ``audio/mp4``
        content type expected by the provider.
        """
        _ = content_type  # Retained for signature compatibility; currently unused.
        filename = "audio.m4a"

        # Use the OpenAI audio transcriptions endpoint via the files API.
        response = await self._client.audio.transcriptions.create(  # type: ignore[reportUnknownMemberType]
            model=self._config.model,
            file=(filename, io.BytesIO(data), "audio/mp4"),
            language=self._config.language,
        )

        text = getattr(response, "text", None)
        if not isinstance(text, str):
            raise TranscriptionError("Transcription provider response missing text")
        cleaned = text.strip()
        if not cleaned:
            raise TranscriptionError("Transcription provider returned empty text")
        return cleaned
