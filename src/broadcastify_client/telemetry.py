"""Telemetry hook interfaces for structured logging and metrics."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol


class TelemetrySink(Protocol):
    """Protocol for emitting structured telemetry events."""

    def record_metric(
        self,
        name: str,
        value: float,
        *,
        attributes: Mapping[str, object] | None = None,
    ) -> None:  # pragma: no cover - protocol
        """Record a metric value with optional attributes."""

        ...

    def record_event(
        self,
        name: str,
        *,
        attributes: Mapping[str, object] | None = None,
    ) -> None:  # pragma: no cover - protocol
        """Record a structured event for diagnostics."""

        ...


class NullTelemetrySink(TelemetrySink):
    """Telemetry sink that drops all events."""

    def record_metric(
        self,
        name: str,
        value: float,
        *,
        attributes: Mapping[str, object] | None = None,
    ) -> None:
        """Drop the metric without side effects."""

    def record_event(
        self,
        name: str,
        *,
        attributes: Mapping[str, object] | None = None,
    ) -> None:
        """Drop the event without side effects."""
