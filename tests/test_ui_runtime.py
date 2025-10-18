"""Tests for TUI runtime window sizing and scrolling behaviour."""

from __future__ import annotations

from datetime import UTC, datetime

from broadcastify_client.ui import runtime
from broadcastify_client.ui.view_model import UiCallRow

EXPECTED_OVERHEAD = 10


def _row(call_id: str, transcript: str | None = None) -> UiCallRow:
    return UiCallRow(
        call_id=call_id,
        received_at=datetime.now(UTC),
        system_id=1,
        system_name="System",
        talkgroup_id=1,
        talkgroup_label=None,
        unit_label="Unit",
        duration=10.0,
        frequency_mhz=123.45,
        status="transcribed",
        transcript=transcript,
        highlight_deadlines={},
    )


def test_compute_board_capacity_respects_console_height() -> None:
    """Capacity should align with the remaining console height after layout overhead."""
    height = 50
    capacity = runtime.compute_board_capacity(height, show_details=True)
    assert capacity == height - EXPECTED_OVERHEAD


def test_compute_board_window_scrolls_for_multi_line_transcripts() -> None:
    """Selecting rows beneath multi-line transcripts must adjust the offset."""
    rows = [
        _row("r0"),
        _row("r1"),
        _row("r2", transcript="alpha\nbeta\ngamma"),
        _row("r3"),
        _row("r4"),
    ]
    available_height = 6  # lines
    offset, window = runtime.compute_board_window(
        rows, selected_index=4, offset=0, available_height=available_height
    )
    assert offset == len(rows) - len(window)
    assert [row.call_id for row in window] == ["r3", "r4"]


def test_compute_board_window_includes_selected_when_large_row() -> None:
    """Even when a single row exceeds capacity it should still be visible."""
    tall_row = _row("r0", transcript="\n".join(str(i) for i in range(10)))
    rows = [tall_row]
    offset, window = runtime.compute_board_window(
        rows, selected_index=0, offset=0, available_height=2
    )
    assert offset == 0
    assert window == rows
