"""OpenAI-compatible Whisper transcription backend.

This backend targets the OpenAI transcription API surface and compatible providers
exposing the same interface. It implements the TranscriptionBackend protocol and
converts ``AudioChunkEvent`` streams into Whisper requests using multipart form data.
"""

from __future__ import annotations

import asyncio
import importlib
import io
import logging
from collections.abc import AsyncIterator
from typing import Protocol, cast, runtime_checkable

from .config import TranscriptionConfig
from .errors import TranscriptionError
from .models import AudioChunkEvent, TranscriptionPartial, TranscriptionResult

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

    async def stream_transcription(
        self, audio_stream: AsyncIterator[AudioChunkEvent]
    ) -> AsyncIterator[TranscriptionPartial]:
        """Yield partial transcriptions for the audio stream."""
        buffer = io.BytesIO()
        has_buffer = False
        start_time: float = 0.0
        end_time: float = 0.0
        chunk_index = 0
        semaphore = asyncio.Semaphore(self._config.max_concurrency)
        last_mime: str | None = None

        async def flush_batch(data: bytes, mime: str | None) -> str:
            async with semaphore:
                return await self._transcribe_bytes(data, content_type=mime)

        async for chunk in audio_stream:
            if not has_buffer:
                start_time = chunk.start_offset
                has_buffer = True
            end_time = chunk.end_offset
            buffer.write(chunk.payload)
            last_mime = chunk.content_type or last_mime
            duration = end_time - start_time
            should_flush = duration >= float(self._config.chunk_seconds) or chunk.finished
            if should_flush:
                data = buffer.getvalue()
                buffer = io.BytesIO()  # reset
                has_buffer = False
                duration = end_time - start_time
                if duration < float(self._config.min_batch_seconds) or len(data) < int(
                    self._config.min_batch_bytes
                ):
                    logger.debug(
                        "Skipping transcription batch: duration=%.3fs bytes=%d below thresholds",
                        duration,
                        len(data),
                    )
                else:
                    try:
                        text = await flush_batch(data, last_mime)
                    except TranscriptionError as exc:
                        logger.warning("Transcription batch failed (non-fatal): %s", exc)
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
        """Return a single transcription result for the entire audio stream."""
        buffer = io.BytesIO()
        call_id: str | None = None
        last_mime: str | None = None
        async for chunk in audio_stream:
            call_id = chunk.call_id
            buffer.write(chunk.payload)
            last_mime = chunk.content_type or last_mime
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
            try:
                text = await self._transcribe_bytes(data, content_type=last_mime)
            except TranscriptionError as exc:
                logger.warning("Final transcription failed (non-fatal): %s", exc)
                text = ""
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

        Respects ``content_type`` when provided to set the filename and MIME type for
        the multipart upload. Falls back to ``audio/mpeg`` if not specified.
        """
        mime = (content_type or "").lower() or "audio/mpeg"

        def _guess_extension(ct: str) -> str:
            if ct in {"audio/wav", "audio/x-wav"}:
                return ".wav"
            if ct in {"audio/mpeg", "audio/mp3"}:
                return ".mp3"
            if ct in {"audio/mp4", "audio/aac", "audio/m4a"}:
                return ".m4a"
            if ct == "audio/flac":
                return ".flac"
            return ".bin"

        ext = _guess_extension(mime)
        filename = f"audio{ext}"

        # Use the OpenAI audio transcriptions endpoint via the files API.
        response = await self._client.audio.transcriptions.create(  # type: ignore[reportUnknownMemberType]
            model=self._config.model,
            file=(filename, io.BytesIO(data), mime),
            language=self._config.language,
        )

        response_obj: object = response
        text: str | None = getattr(response_obj, "text", None)
        if not text:
            logger.warning("Empty transcription response from provider; treating as empty text")
            return ""
        return text
