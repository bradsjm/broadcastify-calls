"""Pydantic schemas for Broadcastify HTTP interactions."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class LiveCallEntry(BaseModel):
    """Canonical representation of a live call payload."""

    model_config = ConfigDict(extra="ignore")

    id: str = Field(description="Composite identifier formatted as 'systemId-talkgroupId'.")
    system_id: int = Field(alias="systemId", description="Numeric system identifier.")
    sid: int = Field(description="System identifier repeated as 'sid'.")
    call_tg: int = Field(alias="call_tg", description="Talkgroup identifier for the call.")
    metadata: Mapping[str, Any] = Field(
        default_factory=dict,
        description="Structured metadata provided alongside the call.",
    )
    grouping: str | None = Field(
        default=None,
        alias="grouping",
        description="Human-readable system or agency name associated with the call.",
    )
    display: str | None = Field(
        default=None,
        alias="display",
        description="Talkgroup alias presented in the Broadcastify UI.",
    )
    descr: str | None = Field(
        default=None,
        alias="descr",
        description="Talkgroup descriptive label used in the Broadcastify UI.",
    )
    call_duration: float | None = Field(
        default=None,
        alias="call_duration",
        description="Length of the call in seconds as emitted by Broadcastify.",
    )
    call_freq: float | None = Field(
        default=None,
        alias="call_freq",
        description="Reported frequency in megahertz.",
    )
    call_src: int | None = Field(
        default=None,
        alias="call_src",
        description="Identifier for the originating source unit.",
    )
    call_src_descr: str | None = Field(
        default=None,
        alias="call_src_descr",
        description="Optional human-readable label for the source unit.",
    )
    call_type: int | None = Field(
        default=None,
        alias="call_type",
        description="Broadcastify call classification code.",
    )
    tag: int | None = Field(
        default=None,
        alias="tag",
        description="Service tag identifier as displayed in the Broadcastify UI.",
    )
    call_ttl: float | None = Field(
        default=None,
        alias="call-ttl",
        description="Unix timestamp indicating call expiration.",
    )
    ts: int = Field(description="Epoch second marking when Broadcastify logged the call.")
    pos: float | None = Field(
        default=None,
        alias="pos",
        description="Cursor position emitted by the backend when present.",
    )


class LiveCallsResponse(BaseModel):
    """Response envelope for the live-calls endpoint."""

    model_config = ConfigDict(extra="ignore")

    calls: list[LiveCallEntry] = Field(description="List of live call records.")
    server_time: int = Field(
        alias="serverTime",
        description="Server time when the response was generated.",
    )
    last_pos: float = Field(
        alias="lastPos",
        description="Cursor to resume polling from.",
    )
    session_key: str | None = Field(
        default=None,
        alias="sessionKey",
        description="Optional session key returned by the backend.",
    )


class ArchiveCallEntry(BaseModel):
    """Canonical representation of an archive call payload."""

    model_config = ConfigDict(extra="ignore")

    id: str = Field(description="Composite identifier formatted as 'systemId-talkgroupId'.")
    system_id: int = Field(alias="systemId", description="Numeric system identifier.")
    sid: int = Field(description="System identifier repeated as 'sid'.")
    call_tg: int = Field(alias="call_tg", description="Talkgroup identifier for the call.")
    metadata: Mapping[str, Any] = Field(
        default_factory=dict,
        description="Structured metadata provided alongside the call.",
    )
    call_freq: float | None = Field(
        default=None,
        alias="call_freq",
        description="Reported frequency in megahertz.",
    )
    call_ttl: float | None = Field(
        default=None,
        alias="call-ttl",
        description="Unix timestamp indicating call expiration.",
    )
    ts: int = Field(description="Epoch second marking when Broadcastify logged the call.")


class ArchiveCallsResponse(BaseModel):
    """Response envelope for archivecall.php results."""

    model_config = ConfigDict(extra="ignore")

    calls: list[ArchiveCallEntry] = Field(
        description="Archive call records within the requested window.",
    )
    server_time: int = Field(
        alias="serverTime",
        description="Server time when the response was generated.",
    )
    start: int = Field(description="Start of the archive window in epoch seconds.")
    end: int = Field(description="End of the archive window in epoch seconds.")
