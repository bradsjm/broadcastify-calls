"""Tests for the Broadcastify CLI helpers and audio dump utilities."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from broadcastify_client.cli import (
    AudioDumpManager,
    parse_cli_args,
    resolve_audio_processing_config,
)
from broadcastify_client.config import AudioProcessingStage
from broadcastify_client.models import AudioPayloadEvent


def test_parse_cli_defaults_without_audio_dump(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Audio dumping is disabled by default and no directory is set."""
    monkeypatch.chdir(tmp_path)
    options = parse_cli_args(["--system-id", "123"])
    assert options.dump_audio is False
    assert options.dump_audio_dir is None
    assert options.audio_processing_cli_provided is False
    assert options.audio_processing_stages == ()
    assert options.audio_silence_threshold_db is None
    assert options.audio_min_silence_ms is None
    assert options.audio_analysis_window_ms is None
    assert options.audio_low_cut_hz is None
    assert options.audio_high_cut_hz is None


def test_parse_cli_enables_dump_with_default_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Enabling dump without a directory uses the cwd-derived default."""
    monkeypatch.chdir(tmp_path)
    options = parse_cli_args(["--system-id", "123", "--dump-audio"])
    assert options.dump_audio is True
    assert options.dump_audio_dir == tmp_path.resolve() / "audio-dumps"
    assert options.audio_processing_cli_provided is False
    assert options.audio_processing_stages == ()


def test_parse_cli_accepts_custom_dump_directory(tmp_path: Path) -> None:
    """Providing an explicit directory turns on dumping and resolves the path."""
    custom_dir = tmp_path / "captures"
    options = parse_cli_args(
        ["--system-id", "123", "--dump-audio-dir", str(custom_dir)]
    )
    assert options.dump_audio is True
    assert options.dump_audio_dir == custom_dir.resolve()
    assert options.audio_processing_cli_provided is False
    assert options.audio_processing_stages == ()


def test_resolve_audio_processing_config_cli_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI overrides enable audio processing and adjust thresholds."""
    monkeypatch.delenv("AUDIO_PROCESSING", raising=False)
    monkeypatch.delenv("AUDIO_SILENCE_THRESHOLD_DB", raising=False)
    monkeypatch.delenv("AUDIO_MIN_SILENCE_MS", raising=False)
    monkeypatch.delenv("AUDIO_ANALYSIS_WINDOW_MS", raising=False)

    silence_threshold_db = -42.5
    min_silence_ms = 150
    analysis_window_ms = 30
    low_cut_hz = 275.0
    high_cut_hz = 3600.0

    options = parse_cli_args(
        [
            "--system-id",
            "123",
            "--audio-processing",
            "trim,bandpass",
            "--audio-silence-threshold-db",
            str(silence_threshold_db),
            "--audio-min-silence-ms",
            str(min_silence_ms),
            "--audio-analysis-window-ms",
            str(analysis_window_ms),
            "--audio-low-cut-hz",
            str(low_cut_hz),
            "--audio-high-cut-hz",
            str(high_cut_hz),
        ]
    )
    assert options.audio_processing_cli_provided is True
    assert options.audio_processing_stages == (
        AudioProcessingStage.TRIM,
        AudioProcessingStage.BAND_PASS,
    )
    config = resolve_audio_processing_config(options, logging.getLogger("test"))
    assert config.stages == frozenset(
        {AudioProcessingStage.TRIM, AudioProcessingStage.BAND_PASS}
    )
    assert config.trim_enabled is True
    assert config.silence_threshold_db == silence_threshold_db
    assert config.min_silence_duration_ms == min_silence_ms
    assert config.analysis_window_ms == analysis_window_ms
    assert config.band_pass_enabled is True
    assert config.low_cut_hz == low_cut_hz
    assert config.high_cut_hz == high_cut_hz


def test_resolve_audio_processing_config_env_band_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Environment variables enable and configure the band-pass filter."""
    monkeypatch.setenv("AUDIO_PROCESSING", "bandpass")
    low_cut_env = 280.0
    high_cut_env = 3500.0
    monkeypatch.setenv("AUDIO_LOW_CUT_HZ", str(low_cut_env))
    monkeypatch.setenv("AUDIO_HIGH_CUT_HZ", str(high_cut_env))

    options = parse_cli_args(["--system-id", "123"])
    config = resolve_audio_processing_config(options, logging.getLogger("test"))
    assert config.stages == frozenset({AudioProcessingStage.BAND_PASS})
    assert config.band_pass_enabled is True
    assert config.low_cut_hz == low_cut_env
    assert config.high_cut_hz == high_cut_env


def test_resolve_audio_processing_config_env_invalid_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invalid cutoff ordering raises a validation error."""
    monkeypatch.setenv("AUDIO_PROCESSING", "bandpass")
    monkeypatch.setenv("AUDIO_LOW_CUT_HZ", "4000")
    monkeypatch.setenv("AUDIO_HIGH_CUT_HZ", "2000")

    options = parse_cli_args(["--system-id", "123"])
    with pytest.raises(ValueError):
        resolve_audio_processing_config(options, logging.getLogger("test"))


@pytest.mark.asyncio
async def test_audio_dump_manager_writes_combined_raw_payloads(tmp_path: Path) -> None:
    """Raw payload events buffered for a call are flushed into a single file when finished."""
    manager = AudioDumpManager(tmp_path, logging.getLogger("test"))
    call_id = "call-1"
    first_event = AudioPayloadEvent(
        call_id=call_id,
        sequence=0,
        start_offset=0.0,
        end_offset=0.5,
        payload=b"raw-part-1",
        content_type="audio/wav",
        finished=False,
    )
    second_event = AudioPayloadEvent(
        call_id=call_id,
        sequence=1,
        start_offset=0.5,
        end_offset=1.0,
        payload=b"raw-part-2",
        content_type="audio/wav",
        finished=True,
    )

    await manager.handle_raw_payload(first_event)
    await manager.handle_raw_payload(second_event)

    raw_path = tmp_path / "call-1_0001_raw.wav"
    assert raw_path.read_bytes() == b"raw-part-1raw-part-2"
