"""Client transport, error mapping, and the Airflow-2 guard (HTTP mocked)."""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from airflow_mcp_server.client import AirflowClient
from airflow_mcp_server.config import Settings
from airflow_mcp_server.errors import (
    AirflowAuthError,
    AirflowError,
    AirflowNotFoundError,
)

BASE = "http://airflow.test"


def _client(**overrides: object) -> AirflowClient:
    kwargs: dict[str, object] = {"base_url": BASE, "username": "u", "password": "p"}
    kwargs.update(overrides)
    return AirflowClient(Settings(_env_file=None, **kwargs))  # type: ignore[arg-type]


def test_get_version(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{BASE}/api/v1/version",
        json={"version": "2.9.3", "git_version": "abc123"},
    )
    with _client() as client:
        info = client.get_version()
    assert info.version == "2.9.3"
    assert info.git_version == "abc123"


def test_get_health(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{BASE}/api/v1/health",
        json={
            "metadatabase": {"status": "healthy"},
            "scheduler": {"status": "healthy"},
        },
    )
    with _client() as client:
        health = client.get_health()
    assert health.scheduler is not None and health.scheduler.status == "healthy"


def test_404_maps_to_not_found(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{BASE}/api/v1/version", status_code=404, json={"detail": "missing"}
    )
    with _client() as client:
        with pytest.raises(AirflowNotFoundError, match="missing"):
            client.get_version()


def test_401_maps_to_auth_error(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{BASE}/api/v1/version", status_code=401, json={"detail": "bad creds"}
    )
    with _client() as client:
        with pytest.raises(AirflowAuthError, match="bad creds"):
            client.get_version()


def test_ensure_supported_accepts_airflow2(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(url=f"{BASE}/api/v1/version", json={"version": "2.10.0"})
    with _client() as client:
        client.ensure_supported()  # must not raise


def test_ensure_supported_rejects_airflow3(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(url=f"{BASE}/api/v1/version", json={"version": "3.0.1"})
    with _client() as client:
        with pytest.raises(AirflowError, match="Airflow 2 only"):
            client.ensure_supported()


def test_ensure_supported_explains_missing_v1(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{BASE}/api/v1/version", status_code=404, json={"detail": "nope"}
    )
    with _client() as client:
        with pytest.raises(AirflowError, match="Airflow 2"):
            client.ensure_supported()
