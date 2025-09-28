"""Configuration schemas for the Broadcastify client."""

from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import timedelta
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
        default=2.0,
        description="Polling interval in seconds between Broadcastify requests",
    )
    jitter_ratio: float = Field(
        default=0.1,
        ge=0.0,
        description=("Fractional jitter applied to poll interval (e.g. 0.1 => +/-10% variance)"),
    )
    queue_maxsize: NonNegativeInt = Field(
        default=256,
        description="Maximum queued call events before back-pressure halts polling",
    )
    initial_backoff: PositiveFloat = Field(
        default=1.0, description="Initial retry delay (seconds) after a failed poll"
    )
    max_backoff: PositiveFloat = Field(
        default=30.0, description="Upper bound on exponential backoff (seconds)"
    )
    max_retry_attempts: NonNegativeInt | None = Field(
        default=None,
        description="Optional cap on consecutive retry attempts before surfacing an error",
    )
    rate_limit_per_minute: NonNegativeInt | None = Field(
        default=None,
        description="Maximum number of Broadcastify requests per minute (None => unlimited)",
    )
    metrics_interval: PositiveFloat = Field(
        default=60.0,
        description="Frequency (seconds) for emitting producer health metrics",
    )
    initial_position: float | None = Field(
        default=None,
        ge=0.0,
        description="Optional cursor for the first poll when resuming a call feed",
    )
    initial_history: NonNegativeInt | None = Field(
        default=None,
        description=(
            "Optional cap on historical calls to dispatch on the first fetch. "
            "None preserves all history (library default). 0 emits live-only (skip all), "
            "and a positive value keeps only the last N calls from the first batch."
        ),
    )


class CacheConfig(BaseModel):
    """Configuration for the archive cache backend."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    ttl: timedelta = Field(default=timedelta(minutes=15), description="TTL for cached entries")
    namespace: str = Field(default="broadcastify", description="Cache namespace prefix")


class TranscriptionConfig(BaseModel):
    """Settings for the optional speech-to-text (STT) pipeline.

    The default provider targets an OpenAI-compatible Whisper endpoint. Configure the
    ``endpoint`` and ``api_key`` to point at a compatible server (e.g. OpenAI, local service).
    When no API key is configured, the client falls back to a local model powered by the
    faster-whisper package (imported dynamically) retaining generic 'whisper' naming. Local
    fields `device` and `compute_type` tune hardware and precision.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    provider: Literal["openai", "external", "local"] = Field(
        default="openai", description="Identifier for the transcription provider"
    )
    enabled: bool = Field(default=False, description="Whether transcription is enabled")
    endpoint: HttpUrl | None = Field(
        default=None, description="Base URL for an OpenAI-compatible API"
    )
    api_key: str | None = Field(
        default=None,
        description=(
            "API key for the transcription provider. If omitted, the environment "
            "variable OPENAI_API_KEY is used when provider is 'openai'."
        ),
    )
    model: str = Field(
        default="whisper-1",
        description=(
            "Model identifier to use for speech-to-text (e.g. 'whisper-1') "
            "variable OPENAI_WHISPER_MODEL is used as a default."
        ),
    )
    device: str = Field(
        default="cpu",
        description=(
            "Hardware device for local faster-whisper models (e.g. 'cpu', 'cuda', 'auto'). "
            "Ignored for remote providers."
        ),
    )
    compute_type: str = Field(
        default="int8",
        description=(
            "Compute/precision type for local faster-whisper models "
            "(e.g. 'int8', 'int8_float16', 'float16', 'float32'). Ignored for remote providers."
        ),
    )
    language: str | None = Field(
        default="en",
        description="Optional BCP-47 language hint to pass to the provider",
    )
    chunk_seconds: int = Field(
        default=10,
        ge=1,
        description="Approximate seconds of audio to batch per partial transcription",
    )
    max_concurrency: int = Field(
        default=2,
        ge=1,
        description="Maximum concurrent transcription requests to the provider",
    )
    emit_partial_results: bool = Field(
        default=True,
        description="Whether to emit partial transcripts while audio is streaming",
    )
    min_batch_seconds: PositiveFloat = Field(
        default=0.5,
        description=(
            "Minimum aggregated audio duration required (in seconds) before a batch is sent "
            "to the transcription provider. Chunks shorter than this are skipped."
        ),
    )
    min_batch_bytes: NonNegativeInt = Field(
        default=4096,
        description=(
            "Minimum total payload size (in bytes) required before a batch is sent to the "
            "transcription provider. Batches below this threshold are skipped."
        ),
    )

    @classmethod
    def from_environment(cls, *, env: Mapping[str, str] | None = None) -> TranscriptionConfig:
        """Build a configuration from environment variables with safe defaults.

        Recognised variables:
            - ``OPENAI_BASE_URL`` → ``endpoint`` when provider is ``openai``
            - ``OPENAI_API_KEY`` → ``api_key`` when provider is ``openai``
            - ``OPENAI_WHISPER_MODEL`` → ``model`` (defaults to ``whisper-1``)
        """
        source = dict(os.environ if env is None else env)
        endpoint = source.get("OPENAI_BASE_URL")
        api_key = source.get("OPENAI_API_KEY")
        model = source.get("OPENAI_WHISPER_MODEL", "whisper-1")
        return cls(
            provider="openai",
            enabled=False,
            endpoint=HttpUrl(endpoint) if endpoint else None,
            api_key=api_key,
            model=model,
        )


class AudioPreprocessConfig(BaseModel):
    """Settings controlling optional in-memory audio pre-processing.

    Phase 1 focuses on voice-band limiting and tail-silence trimming to remove
    squelch closures and courtesy tones/beeps at the end of recordings. All
    processing occurs in-memory via PyAV (libav) without external executables
    or temporary files.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = Field(
        default=False,
        description=(
            "Enable audio pre-processing (band-limit and tail silence trimming)."
        ),
    )
    sample_rate: int = Field(
        default=16_000,
        ge=8000,
        description="Target sample rate (Hz) for processed audio",
    )
    mono: bool = Field(default=True, description="Downmix to mono if input is multi-channel")
    highpass_hz: int = Field(
        default=250, description="High-pass cutoff frequency (Hz) to remove rumble/squelch"
    )
    lowpass_hz: int = Field(
        default=3400, description="Low-pass cutoff frequency (Hz) to match P25 voice band"
    )
    tail_silence_threshold_db: float = Field(
        default=-35.0, description="Silence threshold (dB) for tail trimming"
    )
    tail_silence_min_ms: int = Field(
        default=200, ge=0, description="Minimum trailing silence (ms) to trim from tail"
    )


def load_credentials_from_environment(*, env: Mapping[str, str] | None = None) -> Credentials:
    """Load Broadcastify credentials from environment variables.

    Requires environment keys ``LOGIN`` and ``PASSWORD`` to be set. The CLI loads
    ``.env`` via python-dotenv prior to calling this function, so no file parsing
    occurs here.

    Raises:
        ValueError: If the required keys cannot be resolved.

    """
    resolved_env = dict(os.environ if env is None else env)

    username = resolved_env.get("LOGIN")
    password = resolved_env.get("PASSWORD")
    if username is None or password is None:
        missing = [key for key in ("LOGIN", "PASSWORD") if key not in resolved_env]
        raise ValueError(f"Missing credential keys: {', '.join(missing)}")
    return Credentials(username=username, password=password)
