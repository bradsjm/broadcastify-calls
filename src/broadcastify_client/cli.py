"""Command-line interface for streaming Broadcastify live call events."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import secrets
import signal
import sys
import traceback
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, cast

from dotenv import find_dotenv, load_dotenv

from .client import BroadcastifyClient, BroadcastifyClientDependencies
from .config import (
    Credentials,
    TranscriptionConfig,
    load_credentials_from_environment,
)
from .errors import AuthenticationError, BroadcastifyError
from .eventbus import ConsumerCallback
from .models import (
    AudioChunkEvent,
    CallMetadata,
    LiveCallEnvelope,
    SourceDescriptor,
    TranscriptionSegment,
)

LOG_LEVELS: Final[dict[str, int]] = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}


@dataclass(frozen=True, slots=True)
class CliOptions:
    """Parsed command-line options for the Broadcastify CLI.

    Exactly one of (playlist_id) or (system_id + talkgroup_ids) must be provided.
    """

    system_id: int | None
    talkgroup_ids: tuple[int, ...]
    playlist_id: str | None
    initial_position: float | None
    history: int
    dotenv_path: Path | None
    log_level: int
    metadata_limit: int
    transcription: bool
    dump_audio: bool = field(default=False)
    dump_audio_dir: Path | None = field(default=None)


async def run_async(options: CliOptions) -> int:
    """Execute the CLI workflow and return the process exit code."""
    logger = _setup_logging(options.log_level)

    # Load .env early so subsequent resolution sees overrides. Use override=True to
    # ensure .env values take precedence over existing environment variables.
    dotenv_file = (
        str(options.dotenv_path) if options.dotenv_path is not None else find_dotenv(usecwd=True)
    )
    if dotenv_file:
        load_dotenv(dotenv_file, override=True)
        logger.info("Loaded environment from %s (override=True)", dotenv_file)
    else:
        logger.debug("No .env file found; relying on process environment only")

    try:
        credentials = _resolve_credentials(options)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    # Emit minimal diagnostics about effective env (avoid secrets)
    openai_key_present = "OPENAI_API_KEY" in os.environ and bool(os.environ.get("OPENAI_API_KEY"))
    openai_base_url = os.environ.get("OPENAI_BASE_URL")
    logger.debug(
        "Env resolution: LOGIN=%s, PASSWORD=%s, OPENAI_API_KEY=%s, OPENAI_BASE_URL=%s",
        "set" if os.environ.get("LOGIN") else "unset",
        "set" if os.environ.get("PASSWORD") else "unset",
        "set" if openai_key_present else "unset",
        openai_base_url or "unset",
    )

    transcription_cfg = _resolve_transcription_config(options.transcription, logger)
    client = _build_client(transcription_cfg)
    token_acquired = False
    try:
        await client.authenticate(credentials)
        token_acquired = True
        await _setup_producers(client, options)
        await _register_live_consumers(client, options, options.metadata_limit, transcription_cfg)
        await _maybe_register_audio_dumpers(client, options, logger)
        await client.start()
        logger.info(_streaming_banner(options))
        await _wait_for_shutdown_signal()
        logger.info("Shutdown signal received; stopping client")
    except AuthenticationError as exc:
        logger.error("Authentication failed: %s", exc)
        print("Authentication failed. Check your credentials.", file=sys.stderr)
        return 1
    except BroadcastifyError as exc:
        logger.error("Broadcastify client error: %s", exc)
        print(f"Broadcastify client error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as exc:  # pragma: no cover - defensive path
        logger.exception("Unexpected error in CLI execution")
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 1
    finally:
        if token_acquired:
            try:
                await client.logout()
            except BroadcastifyError as exc:
                logger.warning("Failed to logout cleanly: %s", exc)
        await client.shutdown()
    return 0


def _setup_logging(log_level: int) -> logging.Logger:
    """Configure logging and return the CLI logger.

    Reduces noise from network libraries at non-DEBUG levels.
    """
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if log_level > logging.DEBUG:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
    return logging.getLogger("broadcastify_client.cli")


def parse_cli_args(argv: Sequence[str] | None = None) -> CliOptions:
    """Parse command-line arguments into :class:`CliOptions`."""
    parser = argparse.ArgumentParser(
        prog="broadcastify_calls",
        description=(
            "Stream Broadcastify live call events for a system/talkgroup until interrupted."
        ),
    )
    parser.add_argument(
        "--system-id",
        type=int,
        required=False,
        default=None,
        help="Broadcastify system identifier to monitor (mutually exclusive with --playlist-id)",
    )
    parser.add_argument(
        "--talkgroup-id",
        dest="talkgroup_ids",
        type=int,
        action="append",
        required=False,
        default=None,
        help="Talkgroup identifier to subscribe to (repeat for multiple)",
    )
    parser.add_argument(
        "--playlist-id",
        dest="playlist_id",
        type=str,
        default=None,
        help="Playlist GUID to monitor (mutually exclusive with --system-id/--talkgroup-id)",
    )
    parser.add_argument(
        "--initial-position",
        type=float,
        default=None,
        help="Initial cursor position (seconds) to resume from",
    )
    parser.add_argument(
        "--history",
        type=int,
        default=0,
        help=("Number of historical calls to emit on first fetch (0 = live only)."),
    )
    parser.add_argument(
        "--dotenv",
        type=Path,
        default=None,
        help="Optional path to a .env file containing LOGIN and PASSWORD",
    )
    parser.add_argument(
        "--log-level",
        choices=tuple(LOG_LEVELS.keys()),
        default="INFO",
        help="Log level for diagnostic output",
    )
    parser.add_argument(
        "--metadata-limit",
        type=int,
        default=3,
        help="Maximum number of metadata key/value pairs to display per event",
    )
    parser.add_argument(
        "--transcription",
        action="store_true",
        help=(
            "Enable speech-to-text transcription (requires OPENAI_API_KEY; "
            "respects OPENAI_BASE_URL)"
        ),
    )
    parser.add_argument(
        "--dump-audio",
        action="store_true",
        help="Persist raw and processed audio for inspection",
    )
    parser.add_argument(
        "--dump-audio-dir",
        type=Path,
        default=None,
        help=(
            "Directory for dumped audio files (defaults to ./audio-dumps when --dump-audio is set)"
        ),
    )

    namespace = parser.parse_args(argv)
    # Normalise talkgroup list; argparse yields None if not provided.
    raw_talkgroups = cast(list[int] | None, namespace.talkgroup_ids) or []
    talkgroup_ids: tuple[int, ...] = tuple(dict.fromkeys(raw_talkgroups))
    if namespace.metadata_limit < 0:
        parser.error("--metadata-limit must be zero or positive")
    if namespace.history < 0:
        parser.error("--history must be zero or positive")

    # Validate mutually exclusive modes
    playlist_id: str | None = namespace.playlist_id
    has_playlist = playlist_id is not None
    has_system = namespace.system_id is not None
    has_talkgroups = len(talkgroup_ids) > 0

    if has_playlist and (has_system or has_talkgroups):
        parser.error("--playlist-id cannot be used with --system-id or --talkgroup-id")
    if not has_playlist:
        if not has_system:
            parser.error("--system-id is required when not using --playlist-id")

    dump_audio_requested = bool(getattr(namespace, "dump_audio", False))
    dump_audio_dir: Path | None = getattr(namespace, "dump_audio_dir", None)
    if dump_audio_dir is not None and not dump_audio_requested:
        dump_audio_requested = True
    resolved_dump_dir: Path | None = None
    if dump_audio_requested:
        candidate = dump_audio_dir or (Path.cwd() / "audio-dumps")
        resolved_dump_dir = candidate.expanduser().resolve()

    return CliOptions(
        system_id=namespace.system_id if not has_playlist else None,
        talkgroup_ids=() if has_playlist else talkgroup_ids,
        playlist_id=playlist_id,
        initial_position=namespace.initial_position,
        history=int(namespace.history),
        dotenv_path=namespace.dotenv,
        log_level=LOG_LEVELS[namespace.log_level],
        metadata_limit=namespace.metadata_limit,
        transcription=bool(getattr(namespace, "transcription", False)),
        dump_audio=dump_audio_requested,
        dump_audio_dir=resolved_dump_dir,
    )


def format_call_event(event: LiveCallEnvelope, *, metadata_limit: int) -> str:
    """Return a single-line summary for *event* limited to *metadata_limit* entries."""
    call = event.call
    timestamp = _format_timestamp(call.received_at)
    system_text = _format_system(call.system_name, call.system_id)
    group_text = _format_group(call.talkgroup_label, call.talkgroup_id)
    source_text = _format_source(call.source)
    duration_text = _format_duration(call.duration_seconds)
    frequency = _format_frequency(call.frequency_mhz)
    cursor = _format_cursor(event.cursor)
    expires_at = _format_expiration(call.ttl_seconds)
    components = [
        timestamp,
        system_text,
        group_text,
        source_text,
        f"call {call.call_id}",
        f"duration {duration_text}",
        f"freq {frequency}",
        f"cursor {cursor}",
    ]
    if expires_at is not None:
        components.append(f"expires {expires_at}")
    header = " | ".join(components)
    detail_lines: list[str] = []
    # Additional human-friendly talkgroup detail expected by tests
    if call.talkgroup_description:
        detail_lines.append(f"  talkgroup {call.talkgroup_id} ({call.talkgroup_description})")
    if call.talkgroup_description:
        detail_lines.append(f"  description: {call.talkgroup_description}")
    metadata_text = _format_metadata(call.metadata, metadata_limit)
    if metadata_text:
        detail_lines.append(f"  metadata: {metadata_text}")
    if detail_lines:
        return "\n".join([header, *detail_lines])
    return header


def _resolve_transcription_config(
    requested: bool, logger: logging.Logger, *, dotenv_path: Path | None = None
) -> TranscriptionConfig:
    """Return a transcription config based on environment and user request.

    The config is enabled only when the ``--transcription`` flag is set and an
    ``OPENAI_API_KEY`` is available in the environment (directly or via .env).
    """
    cfg = TranscriptionConfig.from_environment()
    if not requested:
        return cfg
    if cfg.api_key:
        logger.info("Transcription enabled (provider=openai, model=%s)", cfg.model)
        return cfg.model_copy(update={"enabled": True})
    logger.warning(
        "--transcription requested but OPENAI_API_KEY not set; transcription disabled",
    )
    return cfg


def _build_client(cfg: TranscriptionConfig) -> BroadcastifyClient:
    """Construct a BroadcastifyClient, enabling transcription when configured."""
    if cfg.enabled:
        deps = BroadcastifyClientDependencies(transcription_config=cfg)
        return BroadcastifyClient(dependencies=deps)
    return BroadcastifyClient()


async def _setup_producers(client: BroadcastifyClient, options: CliOptions) -> None:
    """Create live producers based on CLI options."""
    if options.playlist_id is not None:
        await client.create_playlist_producer(
            options.playlist_id,
            position=options.initial_position,
            initial_history=options.history,
        )
        return
    assert options.system_id is not None
    if options.talkgroup_ids:
        for talkgroup_id in options.talkgroup_ids:
            await client.create_live_producer(
                options.system_id,
                talkgroup_id,
                position=options.initial_position,
                initial_history=options.history,
            )
    else:
        await client.create_system_producer(
            options.system_id,
            position=options.initial_position,
            initial_history=options.history,
        )


def _streaming_banner(options: CliOptions) -> str:
    """Return a user-facing banner describing the current subscription."""
    if options.playlist_id is not None:
        return f"Streaming live calls for playlist {options.playlist_id}"
    assert options.system_id is not None
    tg_list = ",".join(str(tg) for tg in options.talkgroup_ids) if options.talkgroup_ids else "*"
    return f"Streaming live calls for system {options.system_id} talkgroup(s) {tg_list}"


def _create_event_printer(metadata_limit: int) -> ConsumerCallback:
    """Return a coroutine callback that prints call events with limited metadata."""
    print_lock = asyncio.Lock()

    async def _printer(event: object) -> None:
        if not isinstance(event, LiveCallEnvelope):
            return
        # Print a compact, single-line header that prioritizes the most useful info
        # for live monitoring. We intentionally omit verbose fields (cursor, expires,
        # extra detail lines) to keep output readable during active incidents.
        line = _format_event_header(event)
        async with print_lock:
            print(line, flush=True)

    return _printer


def _create_transcript_segment_printer() -> ConsumerCallback:
    """Return a coroutine callback that prints per-segment transcription text."""
    print_lock = asyncio.Lock()

    async def _printer(event: object) -> None:
        if not isinstance(event, TranscriptionSegment):
            return
        text = event.text.strip()
        if not text:
            return
        line = f"  -> {text}"
        async with print_lock:
            print(line, flush=True)

    return _printer


def _format_event_header(event: LiveCallEnvelope) -> str:
    """Return a compact, single-line header summarizing the call event."""
    call = event.call
    timestamp = _format_timestamp(call.received_at)
    system_text = _format_system(call.system_name, call.system_id)
    group_text = _format_group(call.talkgroup_label, call.talkgroup_id)
    source_text = _format_source(call.source)
    duration_text = _format_duration(call.duration_seconds)
    components = [
        timestamp,
        system_text,
        group_text,
        source_text,
        f"duration {duration_text}",
    ]
    return " | ".join(components)


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _format_frequency(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.6f}".rstrip("0").rstrip(".") + " MHz"


def _format_cursor(value: float | None) -> str:
    if value is None:
        return "-"
    if float(value).is_integer():
        return f"{int(value)}"
    return f"{value:.3f}"


def _format_expiration(value: float | None) -> str | None:
    if value is None:
        return None
    try:
        expires_at = datetime.fromtimestamp(value, UTC)
    except (OSError, OverflowError, ValueError):  # pragma: no cover - defensive path
        return None
    return expires_at.isoformat()


def _format_system(system_name: str | None, system_id: int) -> str:
    if system_name:
        return f"{system_name} ({system_id})"
    return f"System {system_id}"


def _format_group(talkgroup_label: str | None, talkgroup_id: int) -> str:
    if talkgroup_label:
        return f"{talkgroup_label} ({talkgroup_id})"
    return f"Talkgroup {talkgroup_id}"


def _format_source(source: SourceDescriptor) -> str:
    identifier = "-" if source.identifier is None else str(source.identifier)
    if source.label:
        return f"{source.label} ({identifier})"
    if source.identifier is not None:
        return f"Unit ({identifier})"
    return f"Source ({identifier})"


def _format_duration(value: float | None) -> str:
    if value is None:
        return "-"
    if float(value).is_integer():
        return f"{int(value)}s"
    return f"{value:.1f}s"


def _format_metadata(metadata: CallMetadata, limit: int) -> str:
    if limit == 0:
        return ""
    items: list[tuple[str, str]] = []
    if metadata.agency and metadata.agency.name:
        items.append(("agency", metadata.agency.name))
    if metadata.channel and metadata.channel.talkgroup_name:
        items.append(("talkgroup_name", metadata.channel.talkgroup_name))
    if metadata.channel and metadata.channel.service_tag:
        items.append(("service_tag", metadata.channel.service_tag))
    if metadata.location and metadata.location.city:
        items.append(("city", metadata.location.city))
    if metadata.playlist:
        items.append(("playlist", metadata.playlist.name or metadata.playlist.playlist_id))
    items.extend(sorted(metadata.extras.items(), key=lambda item: item[0]))
    items = sorted(items, key=lambda item: item[0])
    if limit > 0:
        items = items[:limit]
    return ",".join(f"{key}={value}" for key, value in items)


def _resolve_credentials(options: CliOptions) -> Credentials:
    return load_credentials_from_environment()


async def _wait_for_shutdown_signal() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered: list[signal.Signals] = []
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop_event.set)
        except (NotImplementedError, RuntimeError):
            continue
        registered.append(signum)
    try:
        await stop_event.wait()
    finally:
        for signum in registered:
            loop.remove_signal_handler(signum)


async def _register_live_consumers(
    client: BroadcastifyClient,
    options: CliOptions,
    metadata_limit: int,
    transcription_cfg: TranscriptionConfig,
) -> None:
    """Register event and transcription consumers based on options."""
    printer = _create_event_printer(metadata_limit)
    if options.playlist_id is not None:
        topic = f"calls.live.playlist.{options.playlist_id}"
        await client.register_consumer(topic, printer)
    else:
        assert options.system_id is not None
        if options.talkgroup_ids:
            for talkgroup_id in options.talkgroup_ids:
                topic = f"calls.live.{options.system_id}.{talkgroup_id}"
                await client.register_consumer(topic, printer)
        else:
            topic = f"calls.live.system.{options.system_id}"
            await client.register_consumer(topic, printer)

    if transcription_cfg.enabled:
        await client.register_consumer(
            "transcription.segment",
            _create_transcript_segment_printer(),
        )
        # Final concatenated transcript printer (simple one-line output)
        async def _final_printer(event: object) -> None:
            # Final result concatenated across segments; avoid top-level import
            if event.__class__.__name__ != "TranscriptionResult":
                return
            text = str(getattr(event, "text", "")).strip()
            if not text:
                return
            print(f"  --> {text}", flush=True)

        await client.register_consumer("transcription.complete", _final_printer)


async def _maybe_register_audio_dumpers(
    client: BroadcastifyClient, options: CliOptions, logger: logging.Logger
) -> None:
    """Register consumers that dump raw and processed audio when requested."""
    if not options.dump_audio or options.dump_audio_dir is None:
        return
    dump_dir = options.dump_audio_dir
    dump_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Audio dumping enabled; writing files to %s", dump_dir)
    manager = _AudioDumpManager(dump_dir, logger)
    await client.register_consumer("calls.audio.raw", manager.handle_raw_chunk)
    await client.register_consumer("calls.audio", manager.handle_processed_chunk)


def _empty_bytes_list() -> list[bytes]:
    """Return a new list for accumulating byte payloads."""

    return []


@dataclass(slots=True)
class _AudioDumpState:
    raw_parts: list[bytes] = field(default_factory=_empty_bytes_list)
    raw_content_type: str | None = None
    raw_dumped: bool = False
    processed_parts: list[bytes] = field(default_factory=_empty_bytes_list)
    processed_content_type: str | None = None
    processed_dumped: bool = False


class _AudioDumpManager:
    """Accumulates audio chunks and writes raw/processed payloads to disk."""

    def __init__(self, directory: Path, logger: logging.Logger) -> None:
        self._directory = directory
        self._logger = logger
        self._states: dict[str, _AudioDumpState] = {}
        self._lock = asyncio.Lock()

    async def handle_raw_chunk(self, event: object) -> None:
        if not isinstance(event, AudioChunkEvent):
            return
        await self._handle_chunk(event, kind="raw")

    async def handle_processed_chunk(self, event: object) -> None:
        if not isinstance(event, AudioChunkEvent):
            return
        await self._handle_chunk(event, kind="processed")

    async def _handle_chunk(self, chunk: AudioChunkEvent, *, kind: str) -> None:
        async with self._lock:
            state = self._states.setdefault(chunk.call_id, _AudioDumpState())
            if kind == "raw":
                state.raw_parts.append(chunk.payload)
                if state.raw_content_type is None:
                    state.raw_content_type = chunk.content_type
                should_dump = chunk.finished and not state.raw_dumped
                parts = list(state.raw_parts) if should_dump else []
                content_type = state.raw_content_type if should_dump else None
                if should_dump:
                    state.raw_parts.clear()
                    state.raw_dumped = True
            else:
                state.processed_parts.append(chunk.payload)
                if state.processed_content_type is None:
                    state.processed_content_type = chunk.content_type
                should_dump = chunk.finished and not state.processed_dumped
                parts = list(state.processed_parts) if should_dump else []
                content_type = state.processed_content_type if should_dump else None
                if should_dump:
                    state.processed_parts.clear()
                    state.processed_dumped = True
            cleanup = state.raw_dumped and state.processed_dumped

        if not should_dump or content_type is None:
            if cleanup:
                await self._cleanup(chunk.call_id)
            return

        payload = b"".join(parts)
        label = "raw" if kind == "raw" else "processed"
        await self._write_call_audio(chunk.call_id, payload, content_type, label)
        if cleanup:
            await self._cleanup(chunk.call_id)

    async def _cleanup(self, call_id: str) -> None:
        async with self._lock:
            self._states.pop(call_id, None)

    async def _write_call_audio(
        self, call_id: str, data: bytes, content_type: str, label: str
    ) -> None:
        if not data:
            self._logger.debug(
                "Skipping %s audio dump for call %s due to empty payload", label, call_id
            )
            return
        safe_call_id = _sanitize_filename_fragment(call_id)
        suffix = _suffix_for_content_type(content_type)
        filename = f"{safe_call_id}_{label}{suffix}"
        target = _dedupe_path(self._directory / filename)
        self._logger.debug(
            "Writing %s audio dump for call %s to %s", label, call_id, target
        )
        await asyncio.to_thread(target.write_bytes, data)
        self._logger.info("Wrote %s audio dump for call %s", label, call_id)


_FILENAME_SAFE_PATTERN: Final[re.Pattern[str]] = re.compile(r"[^A-Za-z0-9_.-]+")


def _sanitize_filename_fragment(value: str) -> str:
    """Return a filesystem-safe fragment derived from *value*."""
    cleaned = _FILENAME_SAFE_PATTERN.sub("_", value)
    return cleaned or "call"


def _suffix_for_content_type(content_type: str) -> str:
    """Return a file suffix for *content_type*, defaulting to .bin."""
    mapping: dict[str, str] = {
        "audio/mpeg": ".mp3",
        "audio/mp3": ".mp3",
        "audio/wav": ".wav",
        "audio/x-wav": ".wav",
        "audio/aac": ".aac",
        "audio/mp4": ".m4a",
        "audio/x-m4a": ".m4a",
        "audio/ogg": ".ogg",
        "audio/x-flac": ".flac",
        "audio/flac": ".flac",
    }
    base = content_type.split(";", 1)[0].strip().lower()
    return mapping.get(base, ".bin")


def _dedupe_path(candidate: Path) -> Path:
    """Return a unique path by appending a numeric suffix when needed."""
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    parent = candidate.parent
    for index in range(1, 10_000):
        attempt = parent / f"{stem}_{index}{suffix}"
        if not attempt.exists():
            return attempt
    return parent / f"{stem}_{secrets.token_hex(4)}{suffix}"  # pragma: no cover - fallback


def main(argv: Sequence[str] | None = None) -> None:
    """Entry point for the ``broadcastify_calls`` console script."""
    options = parse_cli_args(argv)
    try:
        exit_code = asyncio.run(run_async(options))
    except KeyboardInterrupt:
        exit_code = 130
    except Exception:  # noqa: BLE001
        traceback.print_exc(limit=1)
        exit_code = 1
    raise SystemExit(exit_code)
