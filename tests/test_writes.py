"""Write client methods (HTTP mocked) and the read-only gate."""

from __future__ import annotations

import json

import pytest
from pytest_httpx import HTTPXMock

from airflow_mcp_server.client import AirflowClient
from airflow_mcp_server.config import Settings

BASE = "http://airflow.test"


def _client(**overrides: object) -> AirflowClient:
    kwargs: dict[str, object] = {"base_url": BASE, "username": "u", "password": "p"}
    kwargs.update(overrides)
    return AirflowClient(Settings(_env_file=None, **kwargs))  # type: ignore[arg-type]


def test_trigger_dag_run_sends_conf_and_note(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        json={"dag_run_id": "manual__1", "dag_id": "etl", "state": "queued"}
    )
    with _client() as client:
        run = client.trigger_dag_run("etl", conf={"k": "v"}, note="hi")

    assert run.dag_run_id == "manual__1"
    req = httpx_mock.get_request()
    assert req is not None and req.method == "POST"
    assert req.url.path == "/api/v1/dags/etl/dagRuns"
    body = json.loads(req.content)
    assert body["conf"] == {"k": "v"}
    assert body["note"] == "hi"


def test_trigger_dag_run_defaults_to_empty_body(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        json={"dag_run_id": "manual__2", "dag_id": "etl", "state": "queued"}
    )
    with _client() as client:
        client.trigger_dag_run("etl")

    req = httpx_mock.get_request()
    assert req is not None
    assert json.loads(req.content) == {}


def test_set_dag_paused(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(json={"dag_id": "etl", "is_paused": True})
    with _client() as client:
        dag = client.set_dag_paused("etl", True)

    assert dag.is_paused is True
    req = httpx_mock.get_request()
    assert req is not None and req.method == "PATCH"
    assert req.url.path == "/api/v1/dags/etl"
    assert json.loads(req.content) == {"is_paused": True}


def test_clear_task_instances_dry_run(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        json={"task_instances": [{"task_id": "t1", "dag_id": "etl"}]}
    )
    with _client() as client:
        result = client.clear_task_instances(
            "etl", dag_run_id="r1", only_failed=True, dry_run=True
        )

    assert result.task_instances[0].task_id == "t1"
    req = httpx_mock.get_request()
    assert req is not None and req.method == "POST"
    assert req.url.path == "/api/v1/dags/etl/clearTaskInstances"
    body = json.loads(req.content)
    assert body["dry_run"] is True
    assert body["only_failed"] is True
    assert body["dag_run_id"] == "r1"


def test_clear_task_instances_reopens_finished_run_by_default(
    httpx_mock: HTTPXMock,
) -> None:
    """The REST API defaults reset_dag_runs to False, which leaves a finished
    run terminal and its cleared tasks unscheduled. Send it explicitly."""
    httpx_mock.add_response(json={"task_instances": []})
    with _client() as client:
        client.clear_task_instances("etl", dag_run_id="r1")

    req = httpx_mock.get_request()
    assert req is not None
    assert json.loads(req.content)["reset_dag_runs"] is True


def test_clear_task_instances_reset_dag_runs_can_be_disabled(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(json={"task_instances": []})
    with _client() as client:
        client.clear_task_instances("etl", dag_run_id="r1", reset_dag_runs=False)

    req = httpx_mock.get_request()
    assert req is not None
    assert json.loads(req.content)["reset_dag_runs"] is False


def test_require_writable_blocks_in_read_only(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastmcp.exceptions import ToolError

    from airflow_mcp_server import app

    class _ReadOnly:
        read_only = True

    monkeypatch.setattr(app, "get_client", lambda: _ReadOnly())
    with pytest.raises(ToolError, match="read-only"):
        app.require_writable()


def test_require_writable_allows_when_writable(monkeypatch: pytest.MonkeyPatch) -> None:
    from airflow_mcp_server import app

    class _Writable:
        read_only = False

    monkeypatch.setattr(app, "get_client", lambda: _Writable())
    app.require_writable()  # must not raise
