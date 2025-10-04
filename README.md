# Broadcastify Calls

Async-first client scaffolding for integrating with Broadcastify live call feeds, archive metadata, and optional transcription pipelines.

## Environment Requirements

- [uv](https://docs.astral.sh/uv/) 0.4 or newer
- CPython 3.13.x (uv will manage the runtime automatically)

## Getting Started

```bash
uv sync --group dev
```

The command above will

- create a local `.venv` pinned to Python 3.13,
- install runtime dependencies from `pyproject.toml`, and
- add the development toolchain defined in the `dev` group (pytest, ruff, coverage).

To add optional capabilities:

```bash
uv sync --group dev --group transcription   # Whisper/OpenAI integration
uv sync --group dev --group transcription_local   # Local Whisper (faster-whisper)
uv sync --group dev --group telemetry       # OpenTelemetry instrumentation
```

If you enable transcription without configuring an API key, the client falls back to the
locally hosted Whisper backend powered by the `faster-whisper` package.

## Transcription Behavior

- Final-only: the pipeline aggregates the full call audio and submits a single request to the provider, emitting exactly one `transcription.complete` event per call.
- Raw pass-through: the downloaded AAC audio is uploaded as an `.m4a` (`audio/mp4`) stream with no intermediate re-encoding.
- Strict error surfacing: any provider failure (missing/empty transcript, HTTP error, unsupported format) raises a `TranscriptionError` to the caller so issues are visible in logs instead of silently returning empty text.

Dumping audio (`--dump-audio`) writes the raw payload fetched from Broadcastify without modification.

## Audio Processing (Optional)

- Disable by default; enable stages with `--audio-processing` (e.g. `--audio-processing all`,
  `--audio-processing trim,bandpass`) or set `AUDIO_PROCESSING=...`.
- Trimming thresholds are configurable via CLI flags (`--audio-silence-threshold-db`, `--audio-min-silence-ms`, `--audio-analysis-window-ms`) or environment variables (`AUDIO_SILENCE_THRESHOLD_DB`, `AUDIO_MIN_SILENCE_MS`, `AUDIO_ANALYSIS_WINDOW_MS`).
- When enabled, optional band-pass filtering attenuates content outside the 250–3800 Hz voice band. Toggle with `--audio-band-pass`/`AUDIO_BAND_PASS_ENABLED=1` and adjust cutoffs via `--audio-low-cut-hz`/`AUDIO_LOW_CUT_HZ` and `--audio-high-cut-hz`/`AUDIO_HIGH_CUT_HZ`.
- A narrow notch filter (enabled by default) suppresses telemetry tones around 1 kHz; tune or disable it via `AUDIO_NOTCH_*` variables.
- Post-filter RMS normalization (enabled by default) targets −20 dBFS with a 6 dB upward gain cap to stabilise transcription levels; configure it via `AUDIO_NORMALIZATION_*` variables.
- Install PyAV by running `uv sync --group audio-processing` (or add the `audio-processing` optional dependency) to enable the silence trimmer. Without PyAV the client logs a warning and leaves payloads unchanged.
- When the trimmer is active the client logs that the PyAV silence processor is enabled and reports trimming metrics at DEBUG level.

## Common Tasks

| Task                   | Command                                                        |
| ---------------------- | -------------------------------------------------------------- |
| Run tests              | `uv run pytest`                                                |
| Lint + format          | `uv run ruff check .` and `uv run ruff format .`               |
| Type checking          | `uv tool install pyright` _(once)_, then `uv tool run pyright` |
| Coverage report        | `uv run coverage run -m pytest && uv run coverage report`      |
| Add a dependency       | `uv add <package>`                                             |
| Update locked versions | `uv lock --upgrade`                                            |

## Project Layout

```
src/
  broadcastify_client/    # library package (async event-driven core)
    __init__.py
    py.typed
 tests/                   # pytest-based async tests (placeholder)
```
