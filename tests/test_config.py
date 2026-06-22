"""Settings validation."""

from __future__ import annotations

import pytest

from airflow_mcp_server.config import Settings


def _settings(**overrides: object) -> Settings:
    kwargs: dict[str, object] = {
        "base_url": "http://airflow.test",
        "username": "u",
        "password": "p",
    }
    kwargs.update(overrides)
    return Settings(_env_file=None, **kwargs)  # type: ignore[arg-type]


def test_base_url_trailing_slash_is_stripped() -> None:
    assert _settings(base_url="http://airflow.test/").base_url == "http://airflow.test"


def test_token_only_is_valid() -> None:
    s = Settings(_env_file=None, base_url="http://x", api_token="tok")  # type: ignore[call-arg]
    assert s.api_token == "tok"


def test_missing_credentials_rejected() -> None:
    with pytest.raises(ValueError):
        Settings(_env_file=None, base_url="http://x")  # type: ignore[call-arg]


def test_partial_basic_auth_rejected() -> None:
    with pytest.raises(ValueError):
        Settings(_env_file=None, base_url="http://x", username="u")  # type: ignore[call-arg]
