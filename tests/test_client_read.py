"""Read endpoints: field curation, shape flattening, and path encoding (mocked)."""

from __future__ import annotations

from pytest_httpx import HTTPXMock

from airflow_mcp_server.client import AirflowClient
from airflow_mcp_server.config import Settings

BASE = "http://airflow.test"


def _client(**overrides: object) -> AirflowClient:
    kwargs: dict[str, object] = {"base_url": BASE, "username": "u", "password": "p"}
    kwargs.update(overrides)
    return AirflowClient(Settings(_env_file=None, **kwargs))  # type: ignore[arg-type]


def test_list_dags_flattens_tags_and_schedule(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        json={
            "dags": [
                {
                    "dag_id": "etl",
                    "is_paused": False,
                    "schedule_interval": {"__type": "CronExpression", "value": "0 2 * * *"},
                    "tags": [{"name": "daily"}, {"name": "core"}],
                    "owners": ["airflow"],
                }
            ],
            "total_entries": 1,
        }
    )
    with _client() as client:
        result = client.list_dags(tags=["daily"], paused=False)

    assert result.total_entries == 1
    dag = result.dags[0]
    assert dag.schedule_interval == "0 2 * * *"
    assert dag.tags == ["daily", "core"]

    req = httpx_mock.get_request()
    assert req is not None
    assert req.url.path == "/api/v1/dags"
    assert req.url.params["limit"] == "50"
    assert req.url.params["paused"] == "false"
    assert req.url.params.get_list("tags") == ["daily"]


def test_get_dag(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        json={"dag_id": "etl", "is_paused": True, "tags": [{"name": "core"}]}
    )
    with _client() as client:
        dag = client.get_dag("etl")
    assert dag.dag_id == "etl"
    assert dag.is_paused is True
    assert dag.tags == ["core"]


def test_list_dag_runs_sets_order_and_state(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        json={"dag_runs": [{"dag_run_id": "r1", "state": "success"}], "total_entries": 1}
    )
    with _client() as client:
        result = client.list_dag_runs("etl", state=["success"])

    assert result.dag_runs[0].dag_run_id == "r1"
    req = httpx_mock.get_request()
    assert req is not None
    assert req.url.params["order_by"] == "-execution_date"
    assert req.url.params.get_list("state") == ["success"]


def test_get_dag_run_encodes_run_id(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        json={"dag_run_id": "manual__x", "dag_id": "etl", "state": "success"}
    )
    with _client() as client:
        run = client.get_dag_run("etl", "manual__2024-01-01T00:00:00+00:00")

    assert run.state == "success"
    req = httpx_mock.get_request()
    assert req is not None
    # ':' and '+' in the run id must be percent-encoded in the path
    assert "manual__2024-01-01T00%3A00%3A00%2B00%3A00" in str(req.url)


def test_list_task_instances(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        json={
            "task_instances": [{"task_id": "t1", "state": "failed", "try_number": 2}],
            "total_entries": 1,
        }
    )
    with _client() as client:
        result = client.list_task_instances("etl", "r1", state=["failed"])

    ti = result.task_instances[0]
    assert ti.task_id == "t1"
    assert ti.try_number == 2


def test_get_task_logs_returns_text_with_plain_accept(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(text="line 1\nERROR boom\n")
    with _client() as client:
        logs = client.get_task_logs("etl", "r1", "t1", 1)

    assert "ERROR boom" in logs
    req = httpx_mock.get_request()
    assert req is not None
    assert req.url.path == "/api/v1/dags/etl/dagRuns/r1/taskInstances/t1/logs/1"
    assert req.headers["accept"] == "text/plain"


def test_list_import_errors(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        json={
            "import_errors": [
                {"import_error_id": 1, "filename": "/dags/bad.py", "stack_trace": "Traceback"}
            ],
            "total_entries": 1,
        }
    )
    with _client() as client:
        result = client.list_import_errors()
    assert result.import_errors[0].filename == "/dags/bad.py"


def test_list_pools(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        json={
            "pools": [{"name": "default_pool", "slots": 128, "open_slots": 120}],
            "total_entries": 1,
        }
    )
    with _client() as client:
        result = client.list_pools()
    pool = result.pools[0]
    assert pool.name == "default_pool"
    assert pool.open_slots == 120


def test_list_variables(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        json={
            "variables": [{"key": "env", "value": "prod", "description": "stage"}],
            "total_entries": 1,
        }
    )
    with _client() as client:
        result = client.list_variables()
    var = result.variables[0]
    assert var.key == "env"
    assert var.value == "prod"


def test_get_variable_encodes_key(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(json={"key": "a/b", "value": "v"})
    with _client() as client:
        var = client.get_variable("a/b")
    assert var.value == "v"
    req = httpx_mock.get_request()
    assert req is not None
    assert "/api/v1/variables/a%2Fb" in str(req.url)
