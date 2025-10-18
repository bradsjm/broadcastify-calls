"""Typed UI view model for the Rich dashboard.

Separates async event ingestion from rendering. The builder holds mutable state
under an asyncio-friendly lock, while snapshots are immutable dataclasses
consumed by pure renderer functions.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta

from ..models import LiveCallEnvelope, TranscriptionResult


@dataclass(frozen=True, slots=True)
class UiCallRow:
    """Represents a single row on the live board."""

    call_id: str
    received_at: datetime
    system_id: int
    system_name: str | None
    talkgroup_id: int
    talkgroup_label: str | None
    unit_label: str
    duration: float | None
    frequency_mhz: float | None
    status: str  # "awaiting" | "transcribed" | "error"
    transcript: str | None = None
    # Monotonic deadline per-field for highlight fading (seconds from loop.time())
    highlight_deadlines: Mapping[str, float] = field(default_factory=dict[str, float])


@dataclass(frozen=True, slots=True)
class UiSnapshot:
    """Immutable snapshot fed to the renderer."""

    generated_at: datetime
    header_text: str
    transcription_enabled: bool
    rows: list[UiCallRow]
    # Producer and telemetry are rendered from external telemetry sink; optional here.


class UiStateBuilder:
    """Mutable UI state with retention and change highlighting."""

    def __init__(self, *, max_rows: int, retention_seconds: int) -> None:
        """Create a builder with bounded rows and time-based retention."""
        self._rows: dict[str, UiCallRow] = {}
        self._max_rows = int(max_rows)
        self._retention = timedelta(seconds=int(retention_seconds))
        self._lock = asyncio.Lock()

    async def apply_call_event(self, event: LiveCallEnvelope) -> None:
        """Upsert a call row from a live event."""
        call = event.call
        unit_label = _unit_label(call.source)
        now_mono = asyncio.get_running_loop().time()
        async with self._lock:
            row = self._rows.get(call.call_id)
            if row is None:
                deadlines = {"all": now_mono + 3.0}
                row = UiCallRow(
                    call_id=call.call_id,
                    received_at=call.received_at.astimezone(UTC),
                    system_id=call.system_id,
                    system_name=call.system_name,
                    talkgroup_id=call.talkgroup_id,
                    talkgroup_label=call.talkgroup_label,
                    unit_label=unit_label,
                    duration=call.duration_seconds,
                    frequency_mhz=call.frequency_mhz,
                    status="awaiting",
                    transcript=None,
                    highlight_deadlines=deadlines,
                )
            else:
                deadlines = dict(row.highlight_deadlines)
                # Update fields that changed and mark highlight
                if row.duration != call.duration_seconds:
                    deadlines["duration"] = now_mono + 3.0
                if row.frequency_mhz != call.frequency_mhz:
                    deadlines["frequency_mhz"] = now_mono + 3.0
                if row.unit_label != unit_label:
                    deadlines["unit_label"] = now_mono + 3.0
                row = replace(
                    row,
                    duration=call.duration_seconds,
                    frequency_mhz=call.frequency_mhz,
                    unit_label=unit_label,
                    highlight_deadlines=deadlines,
                )
            self._rows[call.call_id] = row
            self._trim_locked()

    async def apply_transcript(self, result: TranscriptionResult) -> None:
        """Append transcript text for a call as segments arrive.

        Concatenates incoming `result.text` to any existing transcript so the UI can
        show progressive updates. Marks the transcript field for highlight fading.
        """
        now_mono = asyncio.get_running_loop().time()
        async with self._lock:
            row = self._rows.get(result.call_id)
            if row is None:
                return
            deadlines = dict(row.highlight_deadlines)
            deadlines["transcript"] = now_mono + 5.0
            separator = "\n" if row.transcript else ""
            new_text = f"{row.transcript or ''}{separator}{result.text}".strip()
            status = "transcribed" if new_text else row.status
            self._rows[result.call_id] = replace(
                row,
                transcript=new_text,
                status=status,
                highlight_deadlines=deadlines,
            )

    async def snapshot(self, *, header_text: str, transcription_enabled: bool) -> UiSnapshot:
        """Return an immutable snapshot of current rows honoring retention and max size."""
        async with self._lock:
            now = datetime.now(UTC)
            # Drop expired rows
            cutoff = now - self._retention
            to_delete = [cid for cid, row in self._rows.items() if row.received_at < cutoff]
            for cid in to_delete:
                self._rows.pop(cid, None)
            # Order newest first
            rows = sorted(self._rows.values(), key=lambda r: r.received_at, reverse=True)
            if len(rows) > self._max_rows:
                rows = rows[: self._max_rows]
        return UiSnapshot(
            generated_at=now,
            header_text=header_text,
            transcription_enabled=transcription_enabled,
            rows=list(rows),
        )

    def _trim_locked(self) -> None:
        """Trim row count when exceeding max size; newest rows are retained."""
        if len(self._rows) <= self._max_rows:
            return
        # Build list ordered by time and drop oldest extras
        ordered = sorted(self._rows.values(), key=lambda r: r.received_at, reverse=True)
        for row in ordered[self._max_rows :]:
            self._rows.pop(row.call_id, None)


def _unit_label(source: object) -> str:
    """Return a human-friendly unit label from SourceDescriptor-like object."""
    label = str(getattr(source, "label", "") or "").strip()
    identifier = getattr(source, "identifier", None)
    ident_str = "-" if identifier is None else str(identifier)
    if label:
        return f"{label} ({ident_str})"
    if identifier is not None:
        return f"Unit ({ident_str})"
    return "Source (-)"
