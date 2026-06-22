"""Shared test fixtures."""

from __future__ import annotations

import os

import pytest


@pytest.fixture(autouse=True)
def _clean_airflow_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate tests from any AIRFLOW_MCP_* vars set in the real environment."""
    for key in list(os.environ):
        if key.startswith("AIRFLOW_MCP_"):
            monkeypatch.delenv(key, raising=False)
