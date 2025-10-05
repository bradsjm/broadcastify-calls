"""Audio segmentation utilities for Whisper transcription optimization.

This module provides silence-based audio segmentation functionality specifically
designed to improve Whisper transcription performance by splitting longer audio
into smaller, more manageable chunks.
"""

from __future__ import annotations

import importlib
import io
import logging
from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Any

from .config import TranscriptionConfig
from .errors import BroadcastifyError
from .models import AudioPayloadEvent

logger = logging.getLogger(__name__)


class AudioSegmentationError(BroadcastifyError):
    """Raised when audio segmentation fails."""


@dataclass(frozen=True, slots=True)
class AudioSegment:
    """Represents a single audio segment with timing metadata."""

    payload: bytes
    start_offset: float
    end_offset: float
    segment_id: int
    total_segments: int

    @property
    def duration(self) -> float:
        """Return the duration of this segment in seconds."""
        return self.end_offset - self.start_offset


class AudioSegmenter:
    """Segments audio by silence detection for improved Whisper transcription."""

    _SUPPORTED_CONTENT_TYPES: tuple[str, ...] = ("audio/mp4", "audio/m4a", "audio/aac")

    def __init__(self, config: TranscriptionConfig) -> None:
        """Initialize the segmenter with Whisper-specific configuration."""
        self._config = config
        self._min_duration_ms = config.whisper_segment_min_duration_ms
        self._max_silence_ms = config.whisper_segment_max_silence_ms
        self._min_silence_ms = config.whisper_segment_min_silence_ms
        self._silence_threshold_db = config.whisper_silence_threshold_db

        # Dynamically import PyAV for audio processing
        try:
            self._av = importlib.import_module("av")
        except ImportError as exc:
            raise AudioSegmentationError(
                "PyAV is required for audio segmentation; install the 'audio-processing' extra"
            ) from exc

        try:
            self._np = importlib.import_module("numpy")
        except ImportError as exc:
            raise AudioSegmentationError(
                "NumPy is required for audio segmentation; install the 'audio-processing' extra"
            ) from exc

    def segment_event(self, event: AudioPayloadEvent) -> Sequence[AudioSegment]:
        """Segment a single AudioPayloadEvent by silence detection.

        Args:
            event: The audio event to segment

        Returns:
            Sequence of AudioSegment objects with sequential segment IDs

        Raises:
            AudioSegmentationError: If segmentation fails

        """
        if not event.payload:
            return []

        if event.content_type not in self._SUPPORTED_CONTENT_TYPES:
            raise AudioSegmentationError(
                f"Unsupported content type '{event.content_type}' for audio segmentation"
            )

        try:
            return self._segment_sync(event)
        except Exception as exc:
            raise AudioSegmentationError(
                f"Audio segmentation failed: {exc.__class__.__name__}: {exc}"
            ) from exc

    def _segment_sync(self, event: AudioPayloadEvent) -> Sequence[AudioSegment]:
        """Perform synchronous audio segmentation using PyAV."""
        # Decode audio to mono numpy array
        audio_array, sample_rate = self._decode_to_mono(event.payload, event.content_type)

        if audio_array.size == 0:
            return []

        # Find silence boundaries
        silence_boundaries = self._find_silence_boundaries(audio_array, sample_rate)

        if not silence_boundaries:
            # No significant silence found, return single segment
            return [
                AudioSegment(
                    payload=event.payload,
                    start_offset=event.start_offset,
                    end_offset=event.end_offset,
                    segment_id=0,
                    total_segments=1,
                )
            ]

        # Create segments based on silence boundaries
        segments = self._create_segments_from_boundaries(
            event, audio_array, sample_rate, silence_boundaries
        )

        return segments

    def _decode_to_mono(self, payload: bytes, content_type: str) -> tuple[Any, int]:
        """Decode audio payload to mono numpy array."""
        # Create in-memory file from payload
        buffer = io.BytesIO(payload)
        container = self._av.open(buffer, mode="r")

        # Find audio stream
        audio_stream = None
        for stream in container.streams:
            if stream.type == "audio":
                audio_stream = stream
                break

        if audio_stream is None:
            raise AudioSegmentationError("No audio stream found in payload")

        # Decode to resampled frames
        sample_rate = 16000  # Whisper expects 16kHz
        resampler = self._av.AudioResampler(
            format="s16",
            layout="mono",
            rate=sample_rate,
        )

        frames: list[Any] = []
        for frame in container.decode(audio_stream):
            resampled_frames = resampler.resample(frame)
            # resampler.resample() returns a list of frames
            for resampled_frame in resampled_frames:
                frames.append(resampled_frame)

        if not frames:
            raise AudioSegmentationError("No audio frames decoded")

        # Convert frames to numpy array, handling variable frame sizes
        frame_arrays: list[Any] = []
        for frame in frames:
            frame_array = frame.to_ndarray()
            # Ensure we have mono audio by taking the first channel if multi-channel
            if frame_array.ndim > 1:
                frame_array = frame_array[0]  # Take first channel
            frame_arrays.append(frame_array.flatten())

        # Concatenate all frame arrays
        if frame_arrays:
            audio_array = self._np.concatenate(frame_arrays)
            audio_array = audio_array.astype(self._np.float32) / 32768.0
        else:
            raise AudioSegmentationError("No valid audio frames to process")

        container.close()
        return audio_array, sample_rate

    def _find_silence_boundaries(self, audio: Any, sample_rate: int) -> list[int]:
        """Find sample indices where silence occurs for segmentation."""
        total_samples = len(audio)
        window_samples = max(1, int(sample_rate * 20 / 1000))  # 20ms window
        min_silence_samples = int(sample_rate * self._min_silence_ms / 1000)
        max_silence_samples = int(sample_rate * self._max_silence_ms / 1000)
        threshold_linear = 10 ** (self._silence_threshold_db / 20)

        if total_samples <= min_silence_samples * 2:
            return []

        # Calculate RMS for sliding windows
        silence_starts: list[int] = []
        in_silence = False
        silence_start = 0

        for start_idx in range(0, total_samples, window_samples):
            end_idx = min(start_idx + window_samples, total_samples)
            window = audio[start_idx:end_idx]

            if window.size == 0:
                continue

            rms = float(self._np.sqrt(self._np.mean(window * window)))

            if rms < threshold_linear:
                if not in_silence:
                    in_silence = True
                    silence_start = start_idx
            elif in_silence:
                silence_duration = start_idx - silence_start
                if min_silence_samples <= silence_duration <= max_silence_samples:
                    # Mark midpoint of silence as boundary
                    boundary = silence_start + silence_duration // 2
                    silence_starts.append(boundary)
                in_silence = False

        # Handle silence at end
        if in_silence:
            silence_duration = total_samples - silence_start
            if min_silence_samples <= silence_duration <= max_silence_samples:
                boundary = silence_start + silence_duration // 2
                silence_starts.append(boundary)

        return silence_starts

    def _create_segments_from_boundaries(
        self,
        event: AudioPayloadEvent,
        audio: Any,
        sample_rate: int,
        boundaries: list[int],
    ) -> Sequence[AudioSegment]:
        """Create AudioSegment objects from silence boundaries."""
        total_samples = len(audio)
        min_segment_samples = int(sample_rate * self._min_duration_ms / 1000)

        # Add start and end boundaries
        all_boundaries = [0, *boundaries, total_samples]
        segments: list[AudioSegment] = []

        for i in range(len(all_boundaries) - 1):
            start_sample = all_boundaries[i]
            end_sample = all_boundaries[i + 1]
            segment_samples = end_sample - start_sample

            # Skip segments that are too short
            if segment_samples < min_segment_samples:
                continue

            # Extract segment audio
            segment_audio = audio[start_sample:end_sample]

            # Convert back to bytes
            segment_payload = self._encode_segment(segment_audio, sample_rate, event.content_type)

            # Calculate timing offsets
            start_time = event.start_offset + (start_sample / sample_rate)
            end_time = event.start_offset + (end_sample / sample_rate)

            segment = AudioSegment(
                payload=segment_payload,
                start_offset=start_time,
                end_offset=end_time,
                segment_id=len(segments),
                total_segments=0,  # Will be updated after all segments created
            )
            segments.append(segment)

        # Update total_segments count
        total_segments = len(segments)

        updated_segments = [replace(segment, total_segments=total_segments) for segment in segments]

        return updated_segments

    def _encode_segment(self, audio: Any, sample_rate: int, content_type: str) -> bytes:
        """Encode audio segment back to bytes in the original format."""
        # Convert numpy array back to int16
        audio_int16 = (audio * 32768).astype(self._np.int16)

        # Create PyAV audio frame
        frame = self._av.AudioFrame.from_ndarray(
            audio_int16.reshape(1, -1), format="s16", layout="mono"
        )
        frame.sample_rate = sample_rate

        # Encode to bytes
        output_buffer = io.BytesIO()
        output_container = self._av.open(output_buffer, mode="w", format="mp4")
        output_stream = output_container.add_stream("aac", rate=sample_rate)

        for packet in output_stream.encode(frame):
            output_container.mux(packet)

        # Flush encoder
        for packet in output_stream.encode():
            output_container.mux(packet)

        output_container.close()
        return output_buffer.getvalue()


__all__ = [
    "AudioSegment",
    "AudioSegmentationError",
    "AudioSegmenter",
]
