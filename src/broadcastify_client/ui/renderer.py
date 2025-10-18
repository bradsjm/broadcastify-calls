"""Pure Rich renderer for the dashboard layout.

Converts UiSnapshot into Rich renderables with change highlighting and
color banding by talkgroup.
"""

from __future__ import annotations

from collections.abc import Iterable

from rich.align import Align
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from .view_model import UiCallRow, UiSnapshot


def render_layout(
    snapshot: UiSnapshot,
    *,
    now_monotonic: float,
    selected_call_id: str | None = None,
    show_details: bool = True,
) -> Layout:
    """Return a Rich Layout for the given snapshot."""
    layout = Layout(name="root")
    layout.split_column(
        Layout(name="header", size=3), Layout(name="body", ratio=1), Layout(name="footer", size=1)
    )
    if show_details:
        layout["body"].split_row(Layout(name="board", ratio=3), Layout(name="side", ratio=1))
    else:
        layout["body"].split_row(Layout(name="board", ratio=1))
    layout["header"].update(_render_header(snapshot))
    layout["board"].update(
        _render_board(snapshot.rows, now_monotonic=now_monotonic, selected_call_id=selected_call_id)
    )
    if show_details:
        selected_row = next((r for r in snapshot.rows if r.call_id == selected_call_id), None)
        layout["side"].update(_render_side(selected_row))
    layout["footer"].update(
        Align.left(Text("Arrows j/k: navigate • d: details • q: quit • Ctrl-C: exit", style="dim"))
    )
    return layout


def _render_header(snapshot: UiSnapshot) -> Panel:
    title = Text(snapshot.header_text, style="bold")
    stt = "ON" if snapshot.transcription_enabled else "OFF"
    subtitle = Text.assemble("STT ", (stt, "bold green" if stt == "ON" else "bold red"))
    inner = Table.grid(expand=True)
    inner.add_column(ratio=3)
    inner.add_column(ratio=1, justify="right")
    inner.add_row(title, subtitle)
    return Panel(inner, title="Broadcastify Live", border_style="cyan")


def _render_board(
    rows: Iterable[UiCallRow], *, now_monotonic: float, selected_call_id: str | None
) -> Panel:
    table = Table(expand=True, show_edge=False, show_lines=True, pad_edge=False)
    table.add_column("Received", no_wrap=True)
    table.add_column("System", ratio=2)
    table.add_column("Talkgroup", ratio=2)
    table.add_column("Unit", ratio=2)
    table.add_column("Dur", no_wrap=True, min_width=3)
    table.add_column("Status", no_wrap=True, min_width=11)
    table.add_column("Transcript", ratio=6, overflow="ellipsis")

    for row in rows:
        tg_style = _color_for_talkgroup(row.talkgroup_id)
        row_style = _selected_row_style() if (selected_call_id == row.call_id) else ""
        cells = [
            Text(
                row.received_at.astimezone().strftime("%H:%M:%S"),
                style=_style_for(row, "received_at", now_monotonic),
            ),
            Text(_sys_label(row), style=_style_for(row, "system_name", now_monotonic)),
            Text(_tg_label(row), style=tg_style),
            Text(row.unit_label, style=_style_for(row, "unit_label", now_monotonic)),
            Text(_fmt_dur(row.duration), style=_style_for(row, "duration", now_monotonic)),
            Text(_status_text(row.status, now_monotonic), style=_status_style(row.status)),
            Text(
                _transcript_content(row, now_monotonic),
                style=_combine_styles(
                    _color_for_talkgroup(row.talkgroup_id),
                    _style_for(row, "transcript", now_monotonic),
                ),
            ),
        ]
        table.add_row(*cells, style=row_style)

    return Panel(table, title="Live Calls", border_style="blue")


def _render_side(selected: UiCallRow | None) -> Panel:
    if selected is None:
        body = Text("No selection", style="dim")
        return Panel(body, title="Details", border_style="magenta")
    grid = Table.grid(expand=True)
    grid.add_column(justify="right", style="cyan")
    grid.add_column()
    grid.add_row("Call ID:", selected.call_id)
    grid.add_row("Received:", selected.received_at.astimezone().isoformat())
    grid.add_row("System: ", _sys_label(selected))
    grid.add_row("Talkgroup: ", _tg_label(selected))
    grid.add_row("Unit: ", selected.unit_label)
    grid.add_row("Duration: ", _fmt_dur(selected.duration))
    grid.add_row("Frequency: ", _fmt_freq(selected.frequency_mhz))
    grid.add_row("Status: ", selected.status.title())
    if selected.transcript:
        grid.add_row(
            "Transcript: ",
            Text(selected.transcript, style=_color_for_talkgroup(selected.talkgroup_id)),
        )
    return Panel(grid, title="Details", border_style="magenta")


def _sys_label(row: UiCallRow) -> str:
    return row.system_name + f" ({row.system_id})" if row.system_name else f"System {row.system_id}"


def _tg_label(row: UiCallRow) -> str:
    return (
        row.talkgroup_label + f" ({row.talkgroup_id})"
        if row.talkgroup_label
        else f"TG {row.talkgroup_id}"
    )


def _fmt_dur(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{int(value)}s" if float(value).is_integer() else f"{value:.1f}s"


def _fmt_freq(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.6f}".rstrip("0").rstrip(".") + " MHz"


_TG_COLORS = (
    "cyan",
    "green",
    "yellow",
    "magenta",
    "bright_cyan",
    "bright_green",
    "bright_yellow",
    "bright_magenta",
)


def _color_for_talkgroup(tg: int) -> str:
    return _TG_COLORS[abs(int(tg)) % len(_TG_COLORS)]


def _style_for(row: UiCallRow, field: str, now_monotonic: float) -> str:
    deadline = row.highlight_deadlines.get(field) or row.highlight_deadlines.get("all")
    if deadline is None:
        return ""
    remaining = deadline - now_monotonic
    if remaining <= 0:
        return ""
    # Fade tiers: strong -> medium -> light
    if remaining > _HIGHLIGHT_STRONG:
        return "bold on #30385a"
    if remaining > _HIGHLIGHT_MEDIUM:
        return "bold on #23283b"
    return "on #1b2033"


def _status_style(status: str) -> str:
    if status == "transcribed":
        return "bold green"
    if status == "error":
        return "bold red"
    return "yellow"


_HIGHLIGHT_STRONG = 2.0
_HIGHLIGHT_MEDIUM = 1.0


def _status_text(status: str, now_monotonic: float) -> str:
    """Return static status text; dots are rendered in transcript column."""
    return status.title()


def _typing_dots(now_monotonic: float) -> str:
    """Return animated typing dots string cycling across '', '.', '..', '...'."""
    phase = int(now_monotonic * 3.0) % 4
    return "." * phase


def _transcript_content(row: UiCallRow, now_monotonic: float) -> str:
    """Return transcript content with animated dots while awaiting."""
    base = row.transcript or ""
    if row.status == "awaiting":
        return base + _typing_dots(now_monotonic)
    return base


def _combine_styles(*styles: str) -> str:
    """Combine multiple Rich style strings into one."""
    return " ".join(s for s in styles if s)


def _selected_row_style() -> str:
    """Return a subtle background for the selected row (no reverse/bright)."""
    return "on #1f2330"
