"""Tests for the Broadcastify CLI helpers and audio dump utilities."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from broadcastify_client.cli import AudioDumpManager, parse_cli_args
from broadcastify_client.models import AudioPayloadEvent


def test_parse_cli_defaults_without_audio_dump(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Audio dumping is disabled by default and no directory is set."""
    monkeypatch.chdir(tmp_path)
    options = parse_cli_args(["--system-id", "123"])
    assert options.dump_audio is False
    assert options.dump_audio_dir is None


def test_parse_cli_enables_dump_with_default_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Enabling dump without a directory uses the cwd-derived default."""
    monkeypatch.chdir(tmp_path)
    options = parse_cli_args(["--system-id", "123", "--dump-audio"])
    assert options.dump_audio is True
    assert options.dump_audio_dir == tmp_path.resolve() / "audio-dumps"


def test_parse_cli_accepts_custom_dump_directory(tmp_path: Path) -> None:
    """Providing an explicit directory turns on dumping and resolves the path."""
    custom_dir = tmp_path / "captures"
    options = parse_cli_args(
        ["--system-id", "123", "--dump-audio-dir", str(custom_dir)]
    )
    assert options.dump_audio is True
    assert options.dump_audio_dir == custom_dir.resolve()


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
