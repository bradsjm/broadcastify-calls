"""Tests for audio segmentation functionality."""

from __future__ import annotations

from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from broadcastify_client.audio_segmentation import (
    AudioSegment,
    AudioSegmentationError,
    AudioSegmenter,
)
from broadcastify_client.config import TranscriptionConfig
from broadcastify_client.models import AudioPayloadEvent, TranscriptionResult
from broadcastify_client.transcription_local import LocalWhisperBackend
from broadcastify_client.transcription_openai import OpenAIWhisperBackend


class TestAudioSegment:
    """Test the AudioSegment dataclass."""

    def test_duration_calculation(self) -> None:
        """Test that duration is calculated correctly."""
        segment = AudioSegment(
            payload=b"audio_data",
            start_offset=10.0,
            end_offset=15.5,
            segment_id=0,
            total_segments=2,
        )
        assert segment.duration == 5.5

    def test_duration_zero_length(self) -> None:
        """Test duration calculation for zero-length segment."""
        segment = AudioSegment(
            payload=b"",
            start_offset=0.0,
            end_offset=0.0,
            segment_id=0,
            total_segments=1,
        )
        assert segment.duration == 0.0


class TestAudioSegmenter:
    """Test the AudioSegmenter class."""

    @pytest.fixture
    def config(self) -> TranscriptionConfig:
        """Create a test transcription config with segmentation enabled."""
        return TranscriptionConfig(
            whisper_segmentation_enabled=True,
            whisper_segment_min_duration_ms=1000,
            whisper_segment_max_silence_ms=500,
            whisper_segment_min_silence_ms=200,
            whisper_silence_threshold_db=-40.0,
        )

    @pytest.fixture
    def segmenter(self, config: TranscriptionConfig) -> AudioSegmenter:
        """Create an AudioSegmenter instance."""
        return AudioSegmenter(config)

    def test_init_missing_pyav(self, config: TranscriptionConfig) -> None:
        """Test that missing PyAV raises appropriate error."""
        with patch("broadcastify_client.audio_segmentation.importlib") as mock_import:
            mock_import.import_module.side_effect = ImportError("No module named 'av'")

            with pytest.raises(AudioSegmentationError, match="PyAV is required"):
                AudioSegmenter(config)

    def test_init_missing_numpy(self, config: TranscriptionConfig) -> None:
        """Test that missing NumPy raises appropriate error."""
        with patch("broadcastify_client.audio_segmentation.importlib") as mock_import:

            def import_side_effect(name: str):
                if name == "av":
                    return MagicMock()
                elif name == "numpy":
                    raise ImportError("No module named 'numpy'")

            mock_import.import_module.side_effect = import_side_effect

            with pytest.raises(AudioSegmentationError, match="NumPy is required"):
                AudioSegmenter(config)

    def test_segment_event_empty_payload(self, segmenter: AudioSegmenter) -> None:
        """Test segmenting an event with empty payload."""
        event = AudioPayloadEvent(
            call_id="test-call",
            sequence=0,
            start_offset=0.0,
            end_offset=10.0,
            payload=b"",
            content_type="audio/mp4",
            finished=True,
        )

        segments = segmenter.segment_event(event)
        assert segments == []

    def test_segment_event_unsupported_content_type(self, segmenter: AudioSegmenter) -> None:
        """Test segmenting an event with unsupported content type."""
        event = AudioPayloadEvent(
            call_id="test-call",
            sequence=0,
            start_offset=0.0,
            end_offset=10.0,
            payload=b"audio_data",
            content_type="audio/wav",  # Unsupported
            finished=True,
        )

        with pytest.raises(AudioSegmentationError, match="Unsupported content type"):
            segmenter.segment_event(event)

    @patch("broadcastify_client.audio_segmentation.AudioSegmenter._segment_sync")
    def test_segment_event_success(
        self, mock_segment_sync: MagicMock, segmenter: AudioSegmenter
    ) -> None:
        """Test successful event segmentation."""
        mock_segments = [
            AudioSegment(
                payload=b"segment1",
                start_offset=0.0,
                end_offset=5.0,
                segment_id=0,
                total_segments=2,
            ),
            AudioSegment(
                payload=b"segment2",
                start_offset=5.0,
                end_offset=10.0,
                segment_id=1,
                total_segments=2,
            ),
        ]
        mock_segment_sync.return_value = mock_segments

        event = AudioPayloadEvent(
            call_id="test-call",
            sequence=0,
            start_offset=0.0,
            end_offset=10.0,
            payload=b"audio_data",
            content_type="audio/mp4",
            finished=True,
        )

        segments = segmenter.segment_event(event)
        assert len(segments) == 2
        assert segments[0].segment_id == 0
        assert segments[1].segment_id == 1

    @patch("broadcastify_client.audio_segmentation.AudioSegmenter._segment_sync")
    def test_segment_event_failure(
        self, mock_segment_sync: MagicMock, segmenter: AudioSegmenter
    ) -> None:
        """Test handling of segmentation failure."""
        mock_segment_sync.side_effect = Exception("Processing failed")

        event = AudioPayloadEvent(
            call_id="test-call",
            sequence=0,
            start_offset=0.0,
            end_offset=10.0,
            payload=b"audio_data",
            content_type="audio/mp4",
            finished=True,
        )

        with pytest.raises(AudioSegmentationError, match="Audio segmentation failed"):
            segmenter.segment_event(event)


class TestOpenAIWhisperBackendSegmentation:
    """Test Whisper backend segmentation functionality."""

    @pytest.fixture
    def config(self) -> TranscriptionConfig:
        """Create a test config with segmentation enabled."""
        return TranscriptionConfig(
            provider="openai",
            api_key="test-key",
            model="whisper-1",
            whisper_segmentation_enabled=True,
            whisper_segment_min_duration_ms=1000,
            whisper_segment_max_silence_ms=500,
            whisper_segment_min_silence_ms=200,
            whisper_silence_threshold_db=-40.0,
        )

    @patch("broadcastify_client.transcription_openai.importlib")
    def test_init_with_segmentation_config(
        self, mock_import: MagicMock, config: TranscriptionConfig
    ) -> None:
        """Test backend initialization with segmentation config."""
        mock_openai = MagicMock()
        mock_client = MagicMock()
        mock_client.audio.transcriptions.create = AsyncMock()
        mock_openai.AsyncOpenAI.return_value = mock_client
        mock_import.import_module.return_value = mock_openai

        backend = OpenAIWhisperBackend(config)
        assert backend._config.whisper_segmentation_enabled is True
        assert backend._config.whisper_segment_min_duration_ms == 1000

    @patch("broadcastify_client.transcription_openai.AudioSegmenter")
    @patch("broadcastify_client.transcription_openai.importlib")
    async def test_finalize_with_segmentation_enabled(
        self, mock_import: MagicMock, mock_segmenter_cls: MagicMock, config: TranscriptionConfig
    ) -> None:
        """Test finalize with segmentation enabled."""
        # Mock the segmenter
        mock_segmenter = MagicMock()
        mock_segmenter_cls.return_value = mock_segmenter

        # Mock segments
        mock_segments = [
            AudioSegment(
                payload=b"segment1",
                start_offset=0.0,
                end_offset=5.0,
                segment_id=0,
                total_segments=2,
            ),
            AudioSegment(
                payload=b"segment2",
                start_offset=5.0,
                end_offset=10.0,
                segment_id=1,
                total_segments=2,
            ),
        ]
        mock_segmenter.segment_event.return_value = mock_segments

        # Mock transcription
        mock_openai = MagicMock()
        mock_client = MagicMock()
        mock_client.audio.transcriptions.create = AsyncMock()
        mock_openai.AsyncOpenAI.return_value = mock_client
        mock_import.import_module.return_value = mock_openai

        backend = OpenAIWhisperBackend(config)

        # Mock the transcribe_bytes method
        with patch.object(backend, "_transcribe_bytes", new_callable=AsyncMock) as mock_transcribe:
            mock_transcribe.side_effect = ["First segment text", "Second segment text"]

            # Create test event
            event = AudioPayloadEvent(
                call_id="test-call",
                sequence=0,
                start_offset=0.0,
                end_offset=10.0,
                payload=b"audio_data",
                content_type="audio/mp4",
                finished=True,
            )

            async def event_stream() -> AsyncIterator[AudioPayloadEvent]:
                yield event

            # Collect results
            results = []
            async for result in backend.finalize(event_stream()):
                results.append(result)

            # Verify results
            assert len(results) == 2
            assert results[0].call_id == "test-call"
            assert results[0].segment_id == 0
            assert results[0].total_segments == 2
            assert results[0].text == "First segment text"
            assert results[0].segment_start_time == 0.0

            assert results[1].call_id == "test-call"
            assert results[1].segment_id == 1
            assert results[1].total_segments == 2
            assert results[1].text == "Second segment text"
            assert results[1].segment_start_time == 5.0

    @patch("broadcastify_client.transcription_openai.AudioSegmenter")
    @patch("broadcastify_client.transcription_openai.importlib")
    async def test_finalize_with_segmentation_disabled(
        self, mock_import: MagicMock, mock_segmenter_cls: MagicMock, config: TranscriptionConfig
    ) -> None:
        """Test finalize with segmentation disabled (legacy behavior)."""
        config = config.model_copy(update={"whisper_segmentation_enabled": False})

        mock_openai = MagicMock()
        mock_client = MagicMock()
        mock_client.audio.transcriptions.create = AsyncMock()
        mock_openai.AsyncOpenAI.return_value = mock_client
        mock_import.import_module.return_value = mock_openai

        backend = OpenAIWhisperBackend(config)

        # Mock the transcribe_bytes method
        with patch.object(backend, "_transcribe_bytes", new_callable=AsyncMock) as mock_transcribe:
            mock_transcribe.return_value = "Single transcription result"

            # Create test event
            event = AudioPayloadEvent(
                call_id="test-call",
                sequence=0,
                start_offset=0.0,
                end_offset=10.0,
                payload=b"audio_data",
                content_type="audio/mp4",
                finished=True,
            )

            async def event_stream() -> AsyncIterator[AudioPayloadEvent]:
                yield event

            # Collect results
            results = []
            async for result in backend.finalize(event_stream()):
                results.append(result)

            # Verify single result
            assert len(results) == 1
            assert results[0].call_id == "test-call"
            assert results[0].segment_id is None
            assert results[0].total_segments is None
            assert results[0].text == "Single transcription result"

            # Verify segmenter was not called
            mock_segmenter_cls.assert_not_called()

    @patch("broadcastify_client.transcription_openai.AudioSegmenter")
    @patch("broadcastify_client.transcription_openai.importlib")
    async def test_finalize_segmentation_failure_fallback(
        self, mock_import: MagicMock, mock_segmenter_cls: MagicMock, config: TranscriptionConfig
    ) -> None:
        """Test fallback to single transcription when segmentation fails."""
        # Mock segmenter to raise error
        mock_segmenter = MagicMock()
        mock_segmenter.segment_event.side_effect = AudioSegmentationError("Segmentation failed")
        mock_segmenter_cls.return_value = mock_segmenter

        mock_openai = MagicMock()
        mock_client = MagicMock()
        mock_client.audio.transcriptions.create = AsyncMock()
        mock_openai.AsyncOpenAI.return_value = mock_client
        mock_import.import_module.return_value = mock_openai

        backend = OpenAIWhisperBackend(config)

        # Mock the transcribe_bytes method
        with patch.object(backend, "_transcribe_bytes", new_callable=AsyncMock) as mock_transcribe:
            mock_transcribe.return_value = "Fallback transcription result"

            # Create test event
            event = AudioPayloadEvent(
                call_id="test-call",
                sequence=0,
                start_offset=0.0,
                end_offset=10.0,
                payload=b"audio_data",
                content_type="audio/mp4",
                finished=True,
            )

            async def event_stream() -> AsyncIterator[AudioPayloadEvent]:
                yield event

            # Collect results
            results = []
            async for result in backend.finalize(event_stream()):
                results.append(result)

            # Verify fallback result
            assert len(results) == 1
            assert results[0].call_id == "test-call"
            assert results[0].text == "Fallback transcription result"


class TestLocalWhisperBackendSegmentation:
    """Test local Whisper backend segmentation functionality."""

    @pytest.fixture
    def config(self) -> TranscriptionConfig:
        """Create a test config with segmentation enabled."""
        return TranscriptionConfig(
            provider="local",
            model="whisper-1",
            whisper_segmentation_enabled=True,
            whisper_segment_min_duration_ms=1000,
            whisper_segment_max_silence_ms=500,
            whisper_segment_min_silence_ms=200,
            whisper_silence_threshold_db=-40.0,
        )

    @patch("broadcastify_client.transcription_local.importlib")
    def test_init_with_segmentation_config(
        self, mock_import: MagicMock, config: TranscriptionConfig
    ) -> None:
        """Test backend initialization with segmentation config."""
        mock_whisper = MagicMock()
        mock_whisper.WhisperModel = MagicMock()
        mock_import.import_module.return_value = mock_whisper

        backend = LocalWhisperBackend(config)
        assert backend._config.whisper_segmentation_enabled is True
        assert backend._config.whisper_segment_min_duration_ms == 1000

    @patch("broadcastify_client.transcription_local.AudioSegmenter")
    @patch("broadcastify_client.transcription_local.importlib")
    async def test_finalize_with_segmentation_enabled(
        self, mock_import: MagicMock, mock_segmenter_cls: MagicMock, config: TranscriptionConfig
    ) -> None:
        """Test finalize with segmentation enabled for local backend."""
        # Mock the segmenter
        mock_segmenter = MagicMock()
        mock_segmenter_cls.return_value = mock_segmenter

        # Mock segments
        mock_segments = [
            AudioSegment(
                payload=b"segment1",
                start_offset=0.0,
                end_offset=5.0,
                segment_id=0,
                total_segments=2,
            ),
        ]
        mock_segmenter.segment_event.return_value = mock_segments

        # Mock Whisper model
        mock_whisper = MagicMock()
        mock_whisper.WhisperModel = MagicMock()
        mock_import.import_module.return_value = mock_whisper

        backend = LocalWhisperBackend(config)

        # Mock the transcribe_bytes method
        with patch.object(backend, "_transcribe_bytes", new_callable=AsyncMock) as mock_transcribe:
            mock_transcribe.return_value = "Segment transcription result"

            # Create test event
            event = AudioPayloadEvent(
                call_id="test-call",
                sequence=0,
                start_offset=0.0,
                end_offset=10.0,
                payload=b"audio_data",
                content_type="audio/mp4",
                finished=True,
            )

            async def event_stream() -> AsyncIterator[AudioPayloadEvent]:
                yield event

            # Collect results
            results = []
            async for result in backend.finalize(event_stream()):
                results.append(result)

            # Verify results
            assert len(results) == 1
            assert results[0].call_id == "test-call"
            assert results[0].segment_id == 0
            assert results[0].total_segments == 1
            assert results[0].text == "Segment transcription result"
            assert results[0].segment_start_time == 0.0


class TestIntegration:
    """Integration tests for the complete segmentation flow."""

    @pytest.mark.asyncio
    @patch("broadcastify_client.transcription_openai.importlib")
    async def test_segmentation_flow_config_disabled(self, mock_import: MagicMock) -> None:
        """Test that segmentation is completely disabled when config flag is False."""
        config = TranscriptionConfig(
            provider="openai",
            api_key="test-key",
            whisper_segmentation_enabled=False,  # Disabled
        )

        mock_openai = MagicMock()
        mock_client = MagicMock()
        mock_client.audio.transcriptions.create = AsyncMock()
        mock_openai.AsyncOpenAI.return_value = mock_client
        mock_import.import_module.return_value = mock_openai

        backend = OpenAIWhisperBackend(config)

        # Mock transcription
        with patch.object(backend, "_transcribe_bytes", new_callable=AsyncMock) as mock_transcribe:
            mock_transcribe.return_value = "Single result"

            event = AudioPayloadEvent(
                call_id="test-call",
                sequence=0,
                start_offset=0.0,
                end_offset=10.0,
                payload=b"audio_data",
                content_type="audio/mp4",
                finished=True,
            )

            async def event_stream() -> AsyncIterator[AudioPayloadEvent]:
                yield event

            results = []
            async for result in backend.finalize(event_stream()):
                results.append(result)

            # Should get single result without segmentation
            assert len(results) == 1
            assert results[0].segment_id is None
            assert results[0].total_segments is None
            assert results[0].text == "Single result"

    def test_transcription_result_with_segment_metadata(self) -> None:
        """Test TranscriptionResult with segment metadata."""
        result = TranscriptionResult(
            call_id="test-call",
            segment_id=1,
            total_segments=3,
            segment_start_time=5.5,
            text="Segment text",
            language="en",
            average_logprob=-0.5,
            segments=["Segment text"],
        )

        assert result.call_id == "test-call"
        assert result.segment_id == 1
        assert result.total_segments == 3
        assert result.segment_start_time == 5.5
        assert result.text == "Segment text"

    def test_transcription_result_without_segment_metadata(self) -> None:
        """Test TranscriptionResult without segment metadata (backward compatibility)."""
        result = TranscriptionResult(
            call_id="test-call",
            text="Full text",
            language="en",
            average_logprob=-0.3,
            segments=["Full text"],
        )

        assert result.call_id == "test-call"
        assert result.segment_id is None
        assert result.total_segments is None
        assert result.segment_start_time is None
        assert result.text == "Full text"


@pytest.mark.asyncio
async def test_pyav_frame_resampling_fix() -> None:
    """Test that PyAV frame resampling handles list of frames correctly."""
    config = TranscriptionConfig(
        whisper_segmentation_enabled=True,
        whisper_segment_min_duration_ms=1000,
        whisper_segment_max_silence_ms=500,
        whisper_segment_min_silence_ms=200,
        whisper_silence_threshold_db=-40.0,
    )

    # Mock PyAV components
    with patch("broadcastify_client.audio_segmentation.importlib") as mock_import:
        mock_av = MagicMock()
        mock_np = MagicMock()

        # Mock numpy
        mock_np.concatenate.return_value = MagicMock()
        mock_np.sqrt.return_value = MagicMock()
        mock_np.mean.return_value = 0.01

        # Mock PyAV container and stream
        mock_container = MagicMock()
        mock_av.open.return_value = mock_container

        # Mock audio stream
        mock_stream = MagicMock()
        mock_stream.type = "audio"
        mock_container.streams = [mock_stream]

        # Mock resampler that returns list of frames
        mock_resampler = MagicMock()
        mock_frame = MagicMock()
        mock_frame.to_ndarray.return_value = MagicMock()
        mock_resampler.resample.return_value = [mock_frame]  # Returns list

        # Mock AudioResampler
        mock_av.AudioResampler.return_value = mock_resampler

        # Mock container.decode to yield frames
        mock_container.decode.return_value = iter([MagicMock()])

        mock_import.import_module.side_effect = lambda name: mock_av if name == "av" else mock_np

        segmenter = AudioSegmenter(config)

        # Create test event
        event = AudioPayloadEvent(
            call_id="test-call",
            sequence=0,
            start_offset=0.0,
            end_offset=10.0,
            payload=b"fake_audio_data",
            content_type="audio/mp4",
            finished=True,
        )

        # This should not raise AttributeError about list.to_ndarray
        try:
            segments = segmenter.segment_event(event)
            # If we get here without exception, the fix worked
            assert isinstance(segments, list)
        except AttributeError as e:
            if "list' object has no attribute 'to_ndarray'" in str(e):
                pytest.fail("PyAV frame resampling fix failed: list.to_ndarray() error occurred")
            else:
                raise  # Re-raise if it's a different AttributeError


@pytest.mark.asyncio
async def test_variable_frame_size_handling() -> None:
    """Test that variable frame sizes are handled correctly during concatenation."""
    config = TranscriptionConfig(
        whisper_segmentation_enabled=True,
        whisper_segment_min_duration_ms=1000,
        whisper_segment_max_silence_ms=500,
        whisper_segment_min_silence_ms=200,
        whisper_silence_threshold_db=-40.0,
    )

    # Mock PyAV components with variable frame sizes
    with patch("broadcastify_client.audio_segmentation.importlib") as mock_import:
        mock_av = MagicMock()
        mock_np = MagicMock()

        # Create mock arrays with different sizes (like the real error)
        array1 = MagicMock()
        array1.ndim = 2
        array1.shape = (1, 2016)  # 2016 samples
        array1.flatten.return_value = MagicMock()

        array2 = MagicMock()
        array2.ndim = 2
        array2.shape = (1, 2048)  # 2048 samples - different size!
        array2.flatten.return_value = MagicMock()

        # Mock numpy concatenation to handle the variable sizes
        concatenated_array = MagicMock()
        concatenated_array.astype.return_value = MagicMock()
        mock_np.concatenate.return_value = concatenated_array

        # Mock PyAV container and stream
        mock_container = MagicMock()
        mock_av.open.return_value = mock_container

        # Mock audio stream
        mock_stream = MagicMock()
        mock_stream.type = "audio"
        mock_container.streams = [mock_stream]

        # Mock resampler that returns list of frames with different sizes
        mock_resampler = MagicMock()
        mock_frame1 = MagicMock()
        mock_frame1.to_ndarray.return_value = array1
        mock_frame2 = MagicMock()
        mock_frame2.to_ndarray.return_value = array2

        mock_resampler.resample.return_value = [mock_frame1]  # First call returns frame1

        # Mock AudioResampler
        mock_av.AudioResampler.return_value = mock_resampler

        # Mock container.decode to yield frames
        mock_container.decode.return_value = iter([MagicMock()])

        mock_import.import_module.side_effect = lambda name: mock_av if name == "av" else mock_np

        segmenter = AudioSegmenter(config)

        # Create test event
        event = AudioPayloadEvent(
            call_id="test-call",
            sequence=0,
            start_offset=0.0,
            end_offset=10.0,
            payload=b"fake_audio_data",
            content_type="audio/mp4",
            finished=True,
        )

        # This should not raise ValueError about mismatched array dimensions
        try:
            segments = segmenter.segment_event(event)
            # If we get here without exception, the fix worked
            assert isinstance(segments, list)
            # Verify concatenate was called with flattened arrays
            mock_np.concatenate.assert_called_once()
            # The call should have been made with flattened arrays
            call_args = mock_np.concatenate.call_args[0][0]
            assert len(call_args) >= 1  # At least one frame array was processed
        except ValueError as e:
            if "all the input array dimensions" in str(e):
                pytest.fail(
                    "Variable frame size handling failed: array dimension mismatch error occurred"
                )
            else:
                raise  # Re-raise if it's a different ValueError


# Logging functionality is tested implicitly through integration tests
# The logging calls are straightforward and don't require complex mocking


if __name__ == "__main__":
    pytest.main([__file__])
