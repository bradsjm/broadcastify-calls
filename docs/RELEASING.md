# Manual Release Guide

This project ships to PyPI using a manually dispatched GitHub Actions workflow. The process ensures every build originates from a clean CI environment and that the published artifacts were validated with our full quality gate.

## Prerequisites
- Update `pyproject.toml` with the new semantic version before publishing.
- Push the version bump to the default branch so the workflow runs from an auditable commit.
- Store the following GitHub secrets:
  - `PYPI_API_TOKEN`: API token from PyPI with project publish permissions.
  - `TESTPYPI_API_TOKEN` (optional): API token for TestPyPI if you plan to stage releases first.
- Ensure the repository has a `pypi` (and optionally `testpypi`) environment configured with required approvals if your organization mandates them.

## Local Validation (recommended)
1. Sync dependencies with the release tooling: `uv sync --group dev --group release`.
2. Run linters and tests:
   - `uv run ruff check`
   - `uv run pyright`
   - `uv run pytest`
3. Build and inspect artifacts locally:
   - `rm -rf dist`
   - `uv run python -m build`
   - `uv run twine check dist/*`
4. Commit any remaining changes (version bump, changelog) and push to the remote repository.

## Triggering the Manual Workflow
1. Navigate to **Actions → Manual Publish → Run workflow** in GitHub.
2. Provide the version exactly as recorded in `pyproject.toml` (e.g., `0.2.0`). The workflow will fail fast if the values diverge.
3. Select the publication target:
   - `pypi` publishes to https://pypi.org using `PYPI_API_TOKEN`.
   - `testpypi` publishes to https://test.pypi.org using `TESTPYPI_API_TOKEN`.
4. Decide whether to enable `skip_checks`. Leave it unchecked to run ruff, pyright, and pytest before the build. Skipping should only be used when you have already run the checks on the same commit.
5. Confirm the dispatch. The workflow will:
   - Install uv and restore cached dependencies.
   - Recreate the virtual environment using the locked dependencies (including release tooling).
   - Re-run the quality gate unless you opted out.
   - Build the wheel and source distribution.
   - Verify the artifacts with `twine check`.
   - Publish to the selected index.

## After Publishing
- For PyPI releases, create a Git tag (e.g., `v0.2.0`) and GitHub Release notes if desired.
- Announce the release or notify downstream consumers as needed.
- Monitor the workflow run and PyPI project page to confirm the version is live.

## Troubleshooting
- `Version mismatch`: Ensure the workflow input matches `project.version` in `pyproject.toml`.
- `401 Unauthorized`: Confirm the relevant API token secret is present and has scope for the project.
- `Artifact validation failed`: Inspect the failing `twine check` logs. Fix the packaging issue locally, rebuild, and rerun the workflow.
