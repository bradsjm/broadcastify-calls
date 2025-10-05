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
from typing import Any, Protocol, cast, runtime_checkable

from .audio_segmentation import AudioSegment, AudioSegmentationError, AudioSegmenter
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

        # Instantiate the provider client and validate the minimal API surface.
        client = AsyncOpenAIClass(**client_kwargs)
        if not _supports_transcriptions_api(client):
            raise TranscriptionError("OpenAI client does not expose expected API surface")
        self._client = cast(OpenAIClientLike, client)

        # streaming partials removed

    async def finalize(
        self, audio_stream: AsyncIterator[AudioPayloadEvent]
    ) -> AsyncIterator[TranscriptionResult]:
        """Yield transcription results for the audio stream, with optional segmentation."""
        if not self._config.whisper_segmentation_enabled:
            # Use existing single-result logic when segmentation disabled
            async for result in self._finalize_single(audio_stream):
                yield result
            return

        # Segmented transcription logic
        async for event in audio_stream:
            try:
                segments = self._segment_audio_by_silence(event)
                total_segments = len(segments)

                # Log segmentation info
                if total_segments > 1:
                    logger.info(
                        "Audio segmented into %d parts for call %s (duration: %.2fs)",
                        total_segments,
                        event.call_id,
                        event.end_offset - event.start_offset,
                    )
                elif total_segments == 1:
                    logger.debug(
                        "Audio kept as single segment for call %s (duration: %.2fs)",
                        event.call_id,
                        event.end_offset - event.start_offset,
                    )

                if total_segments == 0:
                    # No segments created, yield empty result
                    yield TranscriptionResult(
                        call_id=event.call_id,
                        text="",
                        language=self._config.language or "",
                        average_logprob=None,
                        segments=(),
                        segment_id=0,
                        total_segments=0,
                        segment_start_time=event.start_offset,
                    )
                    continue

                # Process each segment individually
                for segment in segments:
                    try:
                        text = await self._transcribe_bytes(
                            segment.payload, content_type=event.content_type
                        )
                        yield TranscriptionResult(
                            call_id=event.call_id,
                            segment_id=segment.segment_id,
                            total_segments=total_segments,
                            text=text,
                            language=self._config.language or "",
                            average_logprob=None,
                            segments=(),
                            segment_start_time=segment.start_offset,
                        )
                    except TranscriptionError as exc:
                        logger.warning(
                            "Segment %d transcription failed for call %s: %s",
                            segment.segment_id,
                            event.call_id,
                            exc,
                        )
                        # Continue with other segments by yielding empty result
                        yield TranscriptionResult(
                            call_id=event.call_id,
                            segment_id=segment.segment_id,
                            total_segments=total_segments,
                            text="",
                            language=self._config.language or "",
                            average_logprob=None,
                            segments=(),
                            segment_start_time=segment.start_offset,
                        )
            except AudioSegmentationError as exc:
                logger.warning("Audio segmentation failed for call %s: %s", event.call_id, exc)
                # Fall back to single transcription
                async for result in self._finalize_single(_single_event_stream(event)):
                    yield result

    async def _finalize_single(
        self, audio_stream: AsyncIterator[AudioPayloadEvent]
    ) -> AsyncIterator[TranscriptionResult]:
        """Return a single transcription result for the entire audio stream (legacy behavior)."""
        buffer = io.BytesIO()
        call_id: str | None = None
        last_mime: str | None = None
        async for event in audio_stream:
            call_id = event.call_id
            buffer.write(event.payload)
            last_mime = event.content_type or last_mime
        if call_id is None:
            # With no audio events, preserve API semantics by returning an empty result.
            yield TranscriptionResult(
                call_id="",
                text="",
                language=self._config.language or "",
                average_logprob=None,
                segments=(),
            )
            return

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
        yield TranscriptionResult(
            call_id=call_id,
            text=text,
            language=self._config.language or "",
            average_logprob=None,
            segments=(),
        )

    def _segment_audio_by_silence(self, event: AudioPayloadEvent) -> list[AudioSegment]:
        """Segment audio by silence using the AudioSegmenter."""
        try:
            segmenter = AudioSegmenter(self._config)
            return list(segmenter.segment_event(event))
        except AudioSegmentationError as exc:
            logger.error("Failed to segment audio for call %s: %s", event.call_id, exc)
            raise

    async def _transcribe_bytes(self, data: bytes, *, content_type: str | None = None) -> str:
        """Call the provider transcription endpoint with the given bytes and return text.

        Uploads the aggregated audio as an AAC ``.m4a`` file using the ``audio/mp4``
        content type expected by the provider.
        """
        _ = content_type  # Retained for signature compatibility; currently unused.
        filename = "audio.m4a"

        # Use the OpenAI audio transcriptions endpoint via the files API.
        prompt = self._config.initial_prompt.strip()
        create = cast(Any, self._client.audio.transcriptions.create)
        response: object
        if prompt:
            response = await create(
                model=self._config.model,
                file=(filename, io.BytesIO(data), "audio/mp4"),
                language=self._config.language,
                prompt=prompt,
            )
        else:
            response = await create(
                model=self._config.model,
                file=(filename, io.BytesIO(data), "audio/mp4"),
                language=self._config.language,
            )

        text = getattr(cast(Any, response), "text", None)
        if not isinstance(text, str):
            raise TranscriptionError("Transcription provider response missing text")
        cleaned = text.strip()
        if not cleaned:
            raise TranscriptionError("Transcription provider returned empty text")
        return cleaned


def _single_event_stream(event: AudioPayloadEvent) -> AsyncIterator[AudioPayloadEvent]:
    """Create a single-event stream for fallback scenarios."""

    async def generator() -> AsyncIterator[AudioPayloadEvent]:
        yield event

    return generator()


def _supports_transcriptions_api(candidate: object) -> bool:
    """Return ``True`` when *candidate* satisfies :class:`OpenAIClientLike`."""
    if candidate is None:
        return False

    audio_ns = getattr(candidate, "audio", None)
    if audio_ns is None:
        return False

    transcriptions = getattr(audio_ns, "transcriptions", None)
    if transcriptions is None:
        return False

    create = getattr(transcriptions, "create", None)
    return callable(create)
