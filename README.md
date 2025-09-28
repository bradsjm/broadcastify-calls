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

- Final-only: streaming partials are removed. The client emits per‑segment updates and a final transcript.
- Segmentation: calls are preprocessed (band‑limited, tail trimmed), then split by silence (P25 half‑duplex) into short segments.
- Events:
  - `transcription.segment` — one event per segment (near real time as segments complete)
  - `transcription.complete` — final, concatenated transcript per call
- Preprocessing is enabled by default when transcription is enabled.
- Broadcastify audio is typically served as M4A (AAC in MP4); the pipeline re-encodes segments to WAV/PCM16 in memory for provider compatibility.

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

## Next Steps

- Implement the async HTTP client abstractions, authentication, and event bus described in `API.md`.
- Define Pydantic configuration models under `config.py` to validate credentials and runtime tuning.
- Add contract and integration tests covering header spoofing, rate limiting, and transcription flows.
