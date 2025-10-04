"""Configuration schemas for the Broadcastify client."""

from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import timedelta
from enum import Enum
from typing import ClassVar, Literal, TypedDict

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    NonNegativeInt,
    PositiveFloat,
    model_validator,
)


class AudioProcessingStage(str, Enum):
    """Represents an individual audio post-processing stage."""

    TRIM = "trim"
    BAND_PASS = "bandpass"  # noqa: S105


class _AudioProcessingOverrides(TypedDict, total=False):
    """Typed override map for :class:`AudioProcessingConfig` initialisation."""

    stages: frozenset[AudioProcessingStage]
    silence_threshold_db: float
    min_silence_duration_ms: int
    analysis_window_ms: int
    low_cut_hz: float
    high_cut_hz: float
    notch_enabled: bool
    notch_center_hz: float
    notch_q: float
    notch_gain_db: float
    normalization_enabled: bool
    normalization_target_dbfs: float
    normalization_max_gain_db: float


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


def _empty_stage_set() -> frozenset[AudioProcessingStage]:
    """Return an empty, typed stage selection."""
    return frozenset()


_FALSE_STRINGS: frozenset[str] = frozenset({"0", "false", "no", "off"})


def _parse_bool_flag(raw: str) -> bool:
    """Interpret configuration flags accepting common "disabled" spellings."""
    return raw.strip().lower() not in _FALSE_STRINGS


class AudioProcessingConfig(BaseModel):
    """Configuration toggles for optional audio post-processing stages."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    _ALL_STAGES: ClassVar[tuple[AudioProcessingStage, ...]] = (
        AudioProcessingStage.TRIM,
        AudioProcessingStage.BAND_PASS,
    )

    stages: frozenset[AudioProcessingStage] = Field(
        default_factory=_empty_stage_set,
        description=(
            "Enabled audio processing stages. Use an empty set for no processing or include "
            "'trim' and/or 'bandpass'."
        ),
    )
    silence_threshold_db: float = Field(
        default=-50.0,
        le=0.0,
        description=(
            "RMS threshold in decibels below which audio is considered silence for trimming."
        ),
    )
    min_silence_duration_ms: NonNegativeInt = Field(
        default=200,
        description=(
            "Minimum consecutive milliseconds below the threshold required "
            "to classify a region as silence."
        ),
    )
    analysis_window_ms: NonNegativeInt = Field(
        default=20,
        ge=1,
        description="Sliding window size in milliseconds for silence analysis",
    )
    low_cut_hz: PositiveFloat = Field(
        default=250.0,
        description=(
            "Lower cutoff frequency (Hz) for the band-pass filter. Frequencies below this "
            "threshold are attenuated."
        ),
    )
    high_cut_hz: PositiveFloat = Field(
        default=3800.0,
        description=(
            "Upper cutoff frequency (Hz) for the band-pass filter. Frequencies above this "
            "threshold are attenuated."
        ),
    )
    notch_enabled: bool = Field(
        default=True,
        description=(
            "Enable a narrow notch to suppress persistent telemetry tones inside the voice band."
        ),
    )
    notch_center_hz: PositiveFloat = Field(
        default=1010.0,
        description="Center frequency (Hz) of the applied notch filter when enabled.",
    )
    notch_q: PositiveFloat = Field(
        default=35.0,
        description=(
            "Quality factor controlling notch bandwidth. Higher values yield a tighter notch."
        ),
    )
    notch_gain_db: float = Field(
        default=-25.0,
        le=0.0,
        description=(
            "Gain applied at the notch center in dB. "
            "Use negative values to attenuate telemetry tones."
        ),
    )
    normalization_enabled: bool = Field(
        default=True,
        description=(
            "Normalize post-filter levels toward a target RMS before handing audio to the "
            "transcriber."
        ),
    )
    normalization_target_dbfs: float = Field(
        default=-20.0,
        description=(
            "Desired RMS level in dBFS for post-processed audio. Values must remain below 0 dBFS."
        ),
        lt=0.0,
    )
    normalization_max_gain_db: PositiveFloat = Field(
        default=6.0,
        description=(
            "Maximum upward gain (dB) allowed during normalization to avoid exaggerating noise."
        ),
    )

    @model_validator(mode="after")
    def _ensure_cutoff_bounds(self) -> AudioProcessingConfig:
        """Validate frequency boundaries when band-pass filtering is enabled."""
        if self.band_pass_enabled and self.low_cut_hz >= self.high_cut_hz:
            msg = "low_cut_hz must be strictly less than high_cut_hz when bandpass is enabled"
            raise ValueError(msg)
        if self.band_pass_enabled and self.notch_enabled:
            if self.notch_center_hz <= self.low_cut_hz or self.notch_center_hz >= self.high_cut_hz:
                msg = (
                    "notch_center_hz must fall within the band-pass range when the notch is enabled"
                )
                raise ValueError(msg)
        return self

    @property
    def trim_enabled(self) -> bool:
        """Return ``True`` when silence trimming is enabled."""
        return AudioProcessingStage.TRIM in self.stages

    @property
    def band_pass_enabled(self) -> bool:
        """Return ``True`` when band-pass filtering is enabled."""
        return AudioProcessingStage.BAND_PASS in self.stages

    @classmethod
    def parse_stage_selection(cls, value: str) -> frozenset[AudioProcessingStage]:
        """Parse a comma-separated stage selection string into a stage set."""
        selector = value.strip().lower()
        if not selector or selector in {"none", "off"}:
            return frozenset()
        if selector == "all":
            return frozenset(cls._ALL_STAGES)
        stages: set[AudioProcessingStage] = set()
        for part in value.split(","):
            item = part.strip().lower()
            if not item:
                raise ValueError("Audio processing selection contains an empty stage name")
            if item in {AudioProcessingStage.TRIM.value, "trim"}:
                stages.add(AudioProcessingStage.TRIM)
                continue
            if item in {AudioProcessingStage.BAND_PASS.value, "band-pass"}:
                stages.add(AudioProcessingStage.BAND_PASS)
                continue
            raise ValueError(f"Unknown audio processing stage '{item}'")
        return frozenset(stages)

    @classmethod
    def ordered_stage_tuple(
        cls, stages: frozenset[AudioProcessingStage]
    ) -> tuple[AudioProcessingStage, ...]:
        """Return a tuple of *stages* respecting the canonical stage ordering."""
        return tuple(stage for stage in cls._ALL_STAGES if stage in stages)

    @classmethod
    def from_environment(cls, *, env: Mapping[str, str] | None = None) -> AudioProcessingConfig:
        """Construct a configuration from environment variables.

        Recognised variables:
            - ``AUDIO_PROCESSING`` selects enabled stages (``all``, ``none``, or comma list)
            - ``AUDIO_SILENCE_THRESHOLD_DB`` overrides the silence threshold (float)
            - ``AUDIO_MIN_SILENCE_MS`` overrides the minimum silence duration (integer)
            - ``AUDIO_ANALYSIS_WINDOW_MS`` overrides the analysis window size (integer)
            - ``AUDIO_LOW_CUT_HZ`` overrides the lower cutoff frequency (float)
            - ``AUDIO_HIGH_CUT_HZ`` overrides the upper cutoff frequency (float)
            - ``AUDIO_NOTCH_ENABLED`` toggles the telemetry notch filter (bool)
            - ``AUDIO_NOTCH_CENTER_HZ`` overrides the notch center frequency (float)
            - ``AUDIO_NOTCH_Q`` overrides the notch quality factor (float)
            - ``AUDIO_NOTCH_GAIN_DB`` sets notch attenuation (float, negative for cuts)
            - ``AUDIO_NORMALIZATION_ENABLED`` toggles RMS normalization (bool)
            - ``AUDIO_NORMALIZATION_TARGET_DBFS`` sets the target RMS level (float, < 0)
            - ``AUDIO_NORMALIZATION_MAX_GAIN_DB`` caps upward normalization gain (float)
        """
        source = dict(os.environ if env is None else env)
        updates: _AudioProcessingOverrides = {}

        processing_raw = source.get("AUDIO_PROCESSING")
        if processing_raw is not None:
            updates["stages"] = cls.parse_stage_selection(processing_raw)

        float_overrides: dict[str, tuple[str, str]] = {
            "AUDIO_SILENCE_THRESHOLD_DB": (
                "silence_threshold_db",
                "AUDIO_SILENCE_THRESHOLD_DB must be a floating point value",
            ),
            "AUDIO_LOW_CUT_HZ": (
                "low_cut_hz",
                "AUDIO_LOW_CUT_HZ must be a floating point value",
            ),
            "AUDIO_HIGH_CUT_HZ": (
                "high_cut_hz",
                "AUDIO_HIGH_CUT_HZ must be a floating point value",
            ),
            "AUDIO_NOTCH_CENTER_HZ": (
                "notch_center_hz",
                "AUDIO_NOTCH_CENTER_HZ must be a floating point value",
            ),
            "AUDIO_NOTCH_Q": (
                "notch_q",
                "AUDIO_NOTCH_Q must be a floating point value",
            ),
            "AUDIO_NOTCH_GAIN_DB": (
                "notch_gain_db",
                "AUDIO_NOTCH_GAIN_DB must be a floating point value",
            ),
            "AUDIO_NORMALIZATION_TARGET_DBFS": (
                "normalization_target_dbfs",
                "AUDIO_NORMALIZATION_TARGET_DBFS must be a floating point value",
            ),
            "AUDIO_NORMALIZATION_MAX_GAIN_DB": (
                "normalization_max_gain_db",
                "AUDIO_NORMALIZATION_MAX_GAIN_DB must be a floating point value",
            ),
        }

        for env_key, (field, error_message) in float_overrides.items():
            raw = source.get(env_key)
            if raw is None:
                continue
            try:
                updates[field] = float(raw)
            except ValueError as exc:
                raise ValueError(error_message) from exc

        int_overrides: dict[str, tuple[str, str]] = {
            "AUDIO_MIN_SILENCE_MS": (
                "min_silence_duration_ms",
                "AUDIO_MIN_SILENCE_MS must be an integer",
            ),
            "AUDIO_ANALYSIS_WINDOW_MS": (
                "analysis_window_ms",
                "AUDIO_ANALYSIS_WINDOW_MS must be an integer",
            ),
        }

        for env_key, (field, error_message) in int_overrides.items():
            raw = source.get(env_key)
            if raw is None:
                continue
            try:
                updates[field] = int(raw)
            except ValueError as exc:
                raise ValueError(error_message) from exc

        bool_overrides = {
            "AUDIO_NOTCH_ENABLED": "notch_enabled",
            "AUDIO_NORMALIZATION_ENABLED": "normalization_enabled",
        }
        for env_key, field in bool_overrides.items():
            raw = source.get(env_key)
            if raw is None:
                continue
            updates[field] = _parse_bool_flag(raw)

        return cls(**updates)


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
