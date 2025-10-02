"""Exception hierarchy for the Broadcastify client library."""

from __future__ import annotations

from dataclasses import dataclass


class BroadcastifyError(Exception):
    """Base exception for all Broadcastify client errors."""


class AuthenticationError(BroadcastifyError):
    """Raised when authentication with Broadcastify fails."""


class TransportError(BroadcastifyError):
    """Raised when an HTTP transport request cannot be completed."""


class ResponseParsingError(BroadcastifyError):
    """Raised when a Broadcastify response cannot be parsed into typed models."""


class CacheError(BroadcastifyError):
    """Raised when the cache backend encounters an operational error."""


class LiveSessionError(BroadcastifyError):
    """Raised when a live call session fails to start or continue."""


class AudioDownloadError(BroadcastifyError):
    """Raised when call audio payloads cannot be retrieved."""


class TranscriptionError(BroadcastifyError):
    """Raised when transcription of call audio fails."""


@dataclass(frozen=True, slots=True)
class RetryHint:
    """Describes how long a failed operation should back off before retrying."""

    seconds: float
    reason: str
