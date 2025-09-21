from __future__ import annotations

from datetime import UTC, datetime

import pytest

from broadcastify_client.cli import format_call_event, parse_cli_args
from broadcastify_client.models import Call, CallEvent


def test_parse_cli_args_deduplicates_talkgroups() -> None:
    options = parse_cli_args(
        ["--system-id", "99", "--talkgroup-id", "42", "--talkgroup-id", "42"]
    )
    assert options.talkgroup_ids == (42,)


def test_parse_cli_args_rejects_negative_metadata_limit() -> None:
    with pytest.raises(SystemExit):
        parse_cli_args([
            "--system-id",
            "1",
            "--talkgroup-id",
            "2",
            "--metadata-limit",
            "-1",
        ])


def test_format_call_event_renders_expected_fields() -> None:
    call = Call(
        call_id=123,
        system_id=9,
        talkgroup_id=456,
        received_at=datetime(2025, 9, 21, 12, 30, tzinfo=UTC),
        frequency_hz=851_012_500.0,
        metadata={"alpha_tag": "Fire", "agency": "City"},
        ttl_seconds=25.0,
    )
    event = CallEvent(
        call=call,
        cursor=12.345,
        received_at=datetime(2025, 9, 21, 12, 30, 1, tzinfo=UTC),
        shard_key=(9, 456),
    )
    line = format_call_event(event, metadata_limit=2)
    assert "call=123" in line
    assert "system=9" in line
    assert "talkgroup=456" in line
    assert "freq=851.012500MHz" in line
    assert "cursor=12.345" in line
    assert "ttl=25.000s" in line
    assert "metadata=agency=City,alpha_tag=Fire" in line
