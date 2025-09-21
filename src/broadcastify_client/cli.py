"""Command-line interface for streaming Broadcastify live call events."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

from .client import BroadcastifyClient
from .config import Credentials, load_credentials_from_environment
from .errors import AuthenticationError, BroadcastifyError
from .eventbus import ConsumerCallback
from .models import CallMetadata, LiveCallEnvelope

LOG_LEVELS: Final[dict[str, int]] = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}


@dataclass(frozen=True, slots=True)
class CliOptions:
    """Parsed command-line options for the Broadcastify CLI."""

    system_id: int
    talkgroup_ids: tuple[int, ...]
    initial_position: float | None
    dotenv_path: Path | None
    log_level: int
    metadata_limit: int


async def run_async(options: CliOptions) -> int:
    """Execute the CLI workflow and return the process exit code."""
    logging.basicConfig(
        level=options.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger = logging.getLogger("broadcastify_client.cli")

    try:
        credentials = _resolve_credentials(options)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    client = BroadcastifyClient()
    token_acquired = False
    try:
        await client.authenticate(credentials)
        token_acquired = True
        for talkgroup_id in options.talkgroup_ids:
            await client.create_live_producer(
                options.system_id,
                talkgroup_id,
                position=options.initial_position,
            )
        printer = _create_event_printer(options.metadata_limit)
        for talkgroup_id in options.talkgroup_ids:
            topic = f"calls.live.{options.system_id}.{talkgroup_id}"
            await client.register_consumer(topic, printer)
        if not options.talkgroup_ids:
            await client.register_consumer("calls.live", printer)
        await client.start()
        logger.info(
            "Streaming live calls for system %s talkgroup(s) %s",
            options.system_id,
            ",".join(str(tg) for tg in options.talkgroup_ids) if options.talkgroup_ids else "*",
        )
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
        required=True,
        help="Broadcastify system identifier to monitor",
    )
    parser.add_argument(
        "--talkgroup-id",
        dest="talkgroup_ids",
        type=int,
        action="append",
        required=True,
        help="Talkgroup identifier to subscribe to (repeat for multiple)",
    )
    parser.add_argument(
        "--initial-position",
        type=float,
        default=None,
        help="Initial cursor position (seconds) to resume from",
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

    namespace = parser.parse_args(argv)
    talkgroup_ids = tuple(dict.fromkeys(namespace.talkgroup_ids))
    if namespace.metadata_limit < 0:
        parser.error("--metadata-limit must be zero or positive")

    return CliOptions(
        system_id=namespace.system_id,
        talkgroup_ids=talkgroup_ids,
        initial_position=namespace.initial_position,
        dotenv_path=namespace.dotenv,
        log_level=LOG_LEVELS[namespace.log_level],
        metadata_limit=namespace.metadata_limit,
    )


def format_call_event(event: LiveCallEnvelope, *, metadata_limit: int) -> str:
    """Return a single-line summary for *event* limited to *metadata_limit* entries."""
    call = event.call
    timestamp = _format_timestamp(call.received_at)
    frequency = _format_frequency(call.frequency_hz)
    cursor = _format_optional_float(event.cursor)
    ttl = _format_optional_float(call.ttl_seconds, suffix="s")
    metadata_text = _format_metadata(call.metadata, metadata_limit)
    parts = [
        f"{timestamp}",
        f"call={call.call_id}",
        f"system={call.system_id}",
        f"talkgroup={call.talkgroup_id}",
        f"freq={frequency}",
        f"cursor={cursor}",
        f"ttl={ttl}",
    ]
    if metadata_text:
        parts.append(f"metadata={metadata_text}")
    return " ".join(parts)


def main(argv: Sequence[str] | None = None) -> None:
    """Entry point for the ``broadcastify_calls`` console script."""
    options = parse_cli_args(argv)
    try:
        exit_code = asyncio.run(run_async(options))
    except KeyboardInterrupt:
        exit_code = 130
    raise SystemExit(exit_code)


def _create_event_printer(metadata_limit: int) -> ConsumerCallback:
    """Return a coroutine callback that prints call events with limited metadata."""
    print_lock = asyncio.Lock()

    async def _printer(event: object) -> None:
        if not isinstance(event, LiveCallEnvelope):
            return
        line = format_call_event(event, metadata_limit=metadata_limit)
        async with print_lock:
            print(line, flush=True)

    return _printer


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _format_frequency(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value / 1_000_000:.6f}MHz"


def _format_optional_float(value: float | None, *, suffix: str | None = None) -> str:
    if value is None:
        return "-"
    formatted = f"{value:.3f}"
    if suffix:
        return f"{formatted}{suffix}"
    return formatted


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
    return load_credentials_from_environment(dotenv_path=options.dotenv_path)


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
