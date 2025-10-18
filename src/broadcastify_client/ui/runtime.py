"""Dashboard runtime wiring Rich Live with the Broadcastify client."""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal as _signal
import sys
import termios
import tty
from collections.abc import Sequence
from typing import Any, cast

from rich.console import Console
from rich.live import Live

from ..client import BroadcastifyClient
from ..eventbus import ConsumerCallback
from ..models import LiveCallEnvelope, TranscriptionResult
from .renderer import render_layout
from .view_model import UiCallRow, UiSnapshot, UiStateBuilder


async def run_ui(
    client: BroadcastifyClient,
    *,
    options: object,
    transcription_enabled: bool,
    max_rows: int,
    retention_seconds: int,
) -> None:
    """Run the Rich dashboard until shutdown signal is received."""
    vm = UiStateBuilder(max_rows=max_rows, retention_seconds=retention_seconds)
    stop_event = asyncio.Event()

    async def _printer(event: object) -> None:
        if isinstance(event, LiveCallEnvelope):
            await vm.apply_call_event(event)

    async def _stt_consumer(event: object) -> None:
        # Avoid tight import coupling to models in this module
        if isinstance(event, TranscriptionResult):
            await vm.apply_transcript(event)

    # Subscribe to the same topics as the non-UI path
    await _register_live_consumers(client, options, _printer)
    if transcription_enabled:
        await client.register_consumer("transcription.complete", _stt_consumer)

    # UI loop
    console = Console()
    header = _header_from_options(options)
    refresh_hz = 10.0
    selected_index = 0
    show_details = True
    offset = 0  # top row index of the visible window
    key_queue: asyncio.Queue[str] = asyncio.Queue()

    # Setup keyboard reader in cbreak mode
    kb = _KeyboardReader(key_queue)
    kb.start()

    async def _shutdown_watcher() -> None:
        await _wait_for_shutdown_signal()
        stop_event.set()

    watcher = asyncio.create_task(_shutdown_watcher())
    try:
        with Live(console=console, refresh_per_second=int(refresh_hz), screen=True) as live:
            while not stop_event.is_set():
                snap = await vm.snapshot(
                    header_text=header, transcription_enabled=transcription_enabled
                )
                # Process all pending keys first so selection/offset are correct this frame
                while not key_queue.empty():
                    key = key_queue.get_nowait()
                    selected_index, show_details, should_quit = _process_key(
                        key, selected_index, show_details, len(snap.rows)
                    )
                    if should_quit:
                        stop_event.set()
                        break
                selected_index, selected_call_id = _compute_selection(snap.rows, selected_index)
                available_height = compute_board_capacity(
                    console.size.height, show_details=show_details
                )
                offset, visible_rows = compute_board_window(
                    snap.rows,
                    selected_index,
                    offset,
                    available_height=available_height,
                )
                vsnap = UiSnapshot(
                    generated_at=snap.generated_at,
                    header_text=snap.header_text,
                    transcription_enabled=snap.transcription_enabled,
                    rows=list(visible_rows),
                )
                live.update(
                    render_layout(
                        vsnap,
                        now_monotonic=asyncio.get_running_loop().time(),
                        selected_call_id=selected_call_id,
                        show_details=show_details,
                    )
                )
                await asyncio.sleep(1.0 / refresh_hz)
    finally:
        kb.stop()
        watcher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watcher


async def _register_live_consumers(
    client: BroadcastifyClient, options: object, callback: ConsumerCallback
) -> None:
    # Introspection to access fields without importing CliOptions type
    playlist_id = getattr(options, "playlist_id", None)
    system_id = getattr(options, "system_id", None)
    talkgroup_ids = cast(tuple[int, ...], getattr(options, "talkgroup_ids", ()))
    if playlist_id is not None:
        await client.register_consumer(f"calls.live.playlist.{playlist_id}", callback)
        return
    assert system_id is not None
    if talkgroup_ids:
        for tg in talkgroup_ids:
            await client.register_consumer(f"calls.live.{system_id}.{tg}", callback)
    else:
        await client.register_consumer(f"calls.live.system.{system_id}", callback)


def _header_from_options(options: object) -> str:
    playlist_id = getattr(options, "playlist_id", None)
    if playlist_id is not None:
        return f"Playlist {playlist_id}"
    system_id = getattr(options, "system_id", None)
    tgs = getattr(options, "talkgroup_ids", ())
    tg_list = ",".join(str(t) for t in tgs) if tgs else "*"
    return f"System {system_id} TGs {tg_list}"


async def _wait_for_shutdown_signal() -> None:
    # Reuse the CLI's signal handling by waiting on SIGINT/SIGTERM via a minimal clone
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered: list[int] = []
    for signum in (_signal.SIGINT, _signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop_event.set)
        except (NotImplementedError, RuntimeError):
            continue
        registered.append(int(signum))
    try:
        await stop_event.wait()
    finally:
        for signum in registered:
            loop.remove_signal_handler(signum)


class _KeyboardReader:
    """Minimal raw-mode keyboard reader that emits simple key tokens to a queue."""

    def __init__(self, queue: asyncio.Queue[str]) -> None:
        self._queue = queue
        self._orig_attrs: Any | None = None
        self._loop = asyncio.get_running_loop()
        self._fd = sys.stdin.fileno()
        self._running = False

    def start(self) -> None:
        if not sys.stdin.isatty():  # no TTY, skip key handling
            return
        self._orig_attrs = termios.tcgetattr(self._fd)
        tty.setcbreak(self._fd)
        self._running = True
        self._loop.add_reader(self._fd, self._on_readable)

    def stop(self) -> None:
        if not sys.stdin.isatty():
            return
        if self._running:
            self._loop.remove_reader(self._fd)
            self._running = False
        if self._orig_attrs is not None:
            termios.tcsetattr(self._fd, termios.TCSADRAIN, self._orig_attrs)  # type: ignore[arg-type]
            self._orig_attrs = None

    def _on_readable(self) -> None:
        try:
            data = os.read(self._fd, 8)
        except OSError:
            return
        for token in _parse_keys(data):
            try:
                self._queue.put_nowait(token)
            except asyncio.QueueFull:
                pass


_ESC = 0x1B


def _parse_keys(data: bytes) -> list[str]:
    """Parse raw bytes into simple tokens: UP, DOWN, DETAILS, QUIT, j, k, d."""
    out: list[str] = []
    i = 0
    n = len(data)
    while i < n:
        b = data[i]
        if b == _ESC and i + 2 < n and data[i + 1 : i + 3] in (b"[A", b"[B"):
            seq = data[i + 1 : i + 3]
            if seq == b"[A":
                out.append("UP")
            else:
                out.append("DOWN")
            i += 3
            continue
        if b in (ord("j"), ord("k")):
            out.append(chr(b))
            i += 1
            continue
        if b in (ord("q"),):
            out.append("QUIT")
            i += 1
            continue
        if b in (ord("d"),):
            out.append("DETAILS")
            i += 1
            continue
        i += 1
    return out


def _compute_selection(rows: list[UiCallRow], selected_index: int) -> tuple[int, str | None]:
    """Clamp selection within available rows and return (index, call_id)."""
    if not rows:
        return 0, None
    idx = max(0, min(selected_index, len(rows) - 1))
    return idx, rows[idx].call_id


def _process_key(
    key: str, selected_index: int, show_details: bool, rows_len: int
) -> tuple[int, bool, bool]:
    """Return updated (selected_index, show_details, should_quit) for a keypress."""
    if key in ("UP", "k"):
        return max(0, selected_index - 1), show_details, False
    if key in ("DOWN", "j"):
        return min(selected_index + 1, max(rows_len - 1, 0)), show_details, False
    if key in ("DETAILS", "d"):
        return selected_index, not show_details, False
    if key in ("QUIT", "q"):
        return selected_index, show_details, True
    return selected_index, show_details, False


_LAYOUT_OVERHEAD = 10
_MIN_VISIBLE_LINES = 2


def compute_board_capacity(height: int, *, show_details: bool) -> int:
    """Return the available line budget for the board panel."""
    overhead = _LAYOUT_OVERHEAD - (1 if not show_details else 0)
    usable = height - overhead
    return max(_MIN_VISIBLE_LINES, usable)


def compute_board_window(
    rows: Sequence[UiCallRow],
    selected_index: int,
    offset: int,
    *,
    available_height: int,
) -> tuple[int, list[UiCallRow]]:
    """Return a window of rows sized to the available height while keeping selection visible."""
    if not rows:
        return 0, []
    capacity = max(_MIN_VISIBLE_LINES, available_height)
    # Clamp indices inside the range of rows.
    selected = max(0, min(selected_index, len(rows) - 1))
    offset = max(0, min(offset, len(rows) - 1))
    offset = min(offset, selected)
    # Slide offset forward until the selected row fits within the height budget.
    accumulated = 0
    for idx in range(offset, selected + 1):
        accumulated += _row_height(rows[idx])
        while accumulated > capacity and offset < idx:
            accumulated -= _row_height(rows[offset])
            offset += 1
    # Collect rows that fit inside the capacity starting from the resolved offset.
    visible: list[UiCallRow] = []
    used = 0
    for idx in range(offset, len(rows)):
        height = _row_height(rows[idx])
        if visible and used + height > capacity:
            break
        visible.append(rows[idx])
        used += height
    return offset, visible


def _row_height(row: UiCallRow) -> int:
    """Return the approximate rendered height in terminal lines for a board row."""
    transcript = row.transcript or ""
    content_lines = max(1, transcript.count("\n") + 1)
    # ``show_lines=True`` introduces a separator between rows, so add one line of padding.
    return content_lines + 1
