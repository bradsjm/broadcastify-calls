# Repository Guidelines

## Project Structure & Module Organization

- `src/broadcastify_client/` holds the async client, cache, telemetry, and transport layers; components map 1:1 with runtime responsibilities (auth, archives, live producer).
- `tests/` contains Pytest suites mirroring module names (`test_client.py`) for fast validation of orchestration and configuration helpers.
- `pyproject.toml`, `pyrightconfig.json`, and `uv.lock` define tooling, dependency locking, and strict type-checker settings.

## Build, Test, and Development Commands

- `uv run ruff check` — lint the entire project with the configured rule set.
- `uv run pyright` — execute strict static type checking (Python 3.13 target).
- `uv run pytest` — run asynchronous test suites with coverage flags from `pyproject.toml`.

## Coding Style & Naming Conventions

- Python code targets 3.13 with strict typing; prefer unions via `X | Y`, `collections.abc` imports, and PEP 695 generics.
- Use `ruff format` defaults (spaces, double quotes, 100-column soft wrap) and respect protocol docstrings plus `...` bodies for abstract methods.
- Modules expose public APIs via `__all__` and dependency containers (`BroadcastifyClientDependencies`) for overrides.

## Testing Guidelines

- Write Pytest coroutines under `tests/`, naming files `test_<subject>.py` and async tests with `@pytest.mark.asyncio`.
- Arrange stubs/fakes locally; prefer typed casts when interacting with protocols.
- Ensure new behaviour is covered and re-run `uv run pytest`, `uv run ruff check`, and `uv run pyright` before submitting.

## Commit & Pull Request Guidelines

- Phrase commit subjects in imperative mood (e.g., “Add cache TTL validation”) mirroring existing history.
- Each PR should summarize scope, list validation commands run, and link to relevant issues; include screenshots only when UI changes occur.
- Keep diffs focused—update documentation (including this guide) whenever behaviour or tooling expectations change.
