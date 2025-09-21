from __future__ import annotations

from pathlib import Path

import pytest

from broadcastify_client import Credentials, load_credentials_from_environment

ENV_USERNAME = "env-user"
ENV_CREDENTIAL_VALUE = "env-pass"
FILE_USERNAME = "file-user"
FILE_CREDENTIAL_VALUE = "file-pass"


def test_load_credentials_prefers_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LOGIN", ENV_USERNAME)
    monkeypatch.setenv("PASSWORD", ENV_CREDENTIAL_VALUE)
    dotenv_path = tmp_path / "prefers" / ".env"
    dotenv_path.parent.mkdir(parents=True, exist_ok=True)
    dotenv_path.write_text("LOGIN=file-user\nPASSWORD=file-pass\n")

    credentials = load_credentials_from_environment(dotenv_path=dotenv_path)

    assert isinstance(credentials, Credentials)
    assert credentials.username == ENV_USERNAME
    assert credentials.password == ENV_CREDENTIAL_VALUE


def test_load_credentials_reads_dotenv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOGIN", raising=False)
    monkeypatch.delenv("PASSWORD", raising=False)
    dotenv_path = tmp_path / "dotenv" / ".env"
    dotenv_path.parent.mkdir(parents=True, exist_ok=True)
    dotenv_path.write_text(f"LOGIN={FILE_USERNAME}\nPASSWORD={FILE_CREDENTIAL_VALUE}\n")

    credentials = load_credentials_from_environment(dotenv_path=dotenv_path)

    assert credentials.username == FILE_USERNAME
    assert credentials.password == FILE_CREDENTIAL_VALUE


def test_load_credentials_missing_keys_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("LOGIN", raising=False)
    monkeypatch.delenv("PASSWORD", raising=False)

    with pytest.raises(ValueError):
        load_credentials_from_environment(dotenv_path=tmp_path / "nonexistent.env")
