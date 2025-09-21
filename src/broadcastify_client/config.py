"""Configuration schemas for the Broadcastify client."""

from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, NonNegativeInt, PositiveFloat


class Credentials(BaseModel):
    """Login credentials for Broadcastify authentication."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    username: str = Field(..., min_length=1, description="Broadcastify account username")
    password: str = Field(..., min_length=1, description="Broadcastify account password")


class HttpClientConfig(BaseModel):
    """HTTP client tuning parameters for Broadcastify requests."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    base_url: HttpUrl = Field(
        default=HttpUrl("https://www.broadcastify.com"),
        description="Root URL for Broadcastify endpoints",
    )
    user_agent: str = Field(
        default=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        ),
        description="Browser-mimicking user agent",
    )
    accept_language: str = Field(
        default="en-US,en;q=0.9", description="Accept-Language header for outbound requests"
    )
    max_connections: NonNegativeInt = Field(
        default=10, description="Maximum concurrent HTTP connections"
    )
    enable_http2: bool = Field(
        default=True, description="Whether HTTP/2 should be attempted when available"
    )


class LiveProducerConfig(BaseModel):
    """Runtime tuning parameters for the live call producer."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    poll_interval: PositiveFloat = Field(
        default=2.0, description="Polling interval in seconds between Broadcastify requests"
    )
    rate_limit_per_minute: NonNegativeInt = Field(
        default=60, description="Maximum number of Broadcastify requests per minute"
    )
    initial_position: int | None = Field(
        default=None,
        description="Optional cursor for the first poll when resuming a call feed",
    )


class CacheConfig(BaseModel):
    """Configuration for the archive cache backend."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    ttl: timedelta = Field(default=timedelta(minutes=15), description="TTL for cached entries")
    namespace: str = Field(default="broadcastify", description="Cache namespace prefix")


class TranscriptionConfig(BaseModel):
    """Settings for the optional transcription pipeline."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    provider: Literal["openai", "external"] = Field(
        default="openai", description="Identifier for the transcription provider"
    )
    enabled: bool = Field(default=False, description="Whether transcription is enabled")
    endpoint: HttpUrl | None = Field(
        default=None, description="Override URL for the transcription endpoint"
    )


def load_credentials_from_environment(
    *,
    env: Mapping[str, str] | None = None,
    dotenv_path: str | Path | None = None,
) -> Credentials:
    """Load Broadcastify credentials from environment variables or a .env file.

    Environment keys ``LOGIN`` and ``PASSWORD`` are used. When the keys are missing
    from *env* the function attempts to read them from *dotenv_path* (default
    ``.env`` in the current working directory). Values parsed from the dotenv file
    are merged with any existing environment entries, preferring explicit values
    from *env*.

    Raises:
        ValueError: If the required keys cannot be resolved.
    """

    resolved_env = dict(env or os.environ)
    if "LOGIN" not in resolved_env or "PASSWORD" not in resolved_env:
        path = Path(dotenv_path) if dotenv_path is not None else Path.cwd() / ".env"
        if path.is_file():
            resolved_env = {**_parse_dotenv(path), **resolved_env}

    username = resolved_env.get("LOGIN")
    password = resolved_env.get("PASSWORD")
    if username is None or password is None:
        missing = [key for key in ("LOGIN", "PASSWORD") if key not in resolved_env]
        raise ValueError(f"Missing credential keys: {', '.join(missing)}")
    return Credentials(username=username, password=password)


def _parse_dotenv(path: Path) -> dict[str, str]:
    """Parse *path* as a dotenv file and return key/value pairs."""

    variables: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            variables[key] = value
    return variables
