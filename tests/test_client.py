import httpx
import pytest

from airflow_mcp.client import AirflowApiError, AirflowClient

BASE_URL = "http://localhost:8080/api/v1"


def make_client(transport: httpx.MockTransport | None = None) -> AirflowClient:
    client = AirflowClient(BASE_URL, "test", "test")
    if transport is not None:
        client._http = httpx.AsyncClient(
            base_url=BASE_URL,
            auth=httpx.BasicAuth("test", "test"),
            timeout=30.0,
            transport=transport,
        )
    return client


def mock_transport(responses: dict[tuple[str, str], tuple[int, dict | list]]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        for (method, path_prefix), (status, body) in responses.items():
            if request.method == method and request.url.path.startswith(path_prefix):
                return httpx.Response(status, json=body, request=request)
        return httpx.Response(404, json={"detail": "not found"}, request=request)

    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_list_dags():
    transport = mock_transport({
        ("GET", "/api/v1/dags"): (200, {
            "dags": [
                {"dag_id": "test_dag", "is_paused": False, "is_active": True}
            ],
            "total_entries": 1,
        }),
    })
    client = make_client(transport=transport)
    result = await client.list_dags()
    assert result["dags"][0]["dag_id"] == "test_dag"
    await client.close()


@pytest.mark.asyncio
async def test_get_dag():
    transport = mock_transport({
        ("GET", "/api/v1/dags/my_dag"): (200, {"dag_id": "my_dag", "is_paused": False}),
    })
    client = make_client(transport=transport)
    result = await client.get_dag("my_dag")
    assert result["dag_id"] == "my_dag"
    await client.close()


@pytest.mark.asyncio
async def test_set_dag_paused():
    transport = mock_transport({
        ("PATCH", "/api/v1/dags/my_dag"): (200, {"dag_id": "my_dag", "is_paused": True}),
    })
    client = make_client(transport=transport)
    result = await client.set_dag_paused("my_dag", is_paused=True)
    assert result["is_paused"] is True
    await client.close()


@pytest.mark.asyncio
async def test_trigger_dag():
    transport = mock_transport({
        ("POST", "/api/v1/dags/my_dag/dagRuns"): (200, {
            "dag_id": "my_dag",
            "dag_run_id": "manual__2026-03-27",
            "state": "queued",
        }),
    })
    client = make_client(transport=transport)
    result = await client.trigger_dag("my_dag", conf={"key": "value"})
    assert result["state"] == "queued"
    await client.close()


@pytest.mark.asyncio
async def test_list_dag_runs():
    transport = mock_transport({
        ("GET", "/api/v1/dags/my_dag/dagRuns"): (200, {
            "dag_runs": [
                {"dag_run_id": "run1", "state": "success"},
                {"dag_run_id": "run2", "state": "failed"},
            ],
            "total_entries": 2,
        }),
    })
    client = make_client(transport=transport)
    result = await client.list_dag_runs("my_dag")
    assert len(result["dag_runs"]) == 2
    await client.close()


@pytest.mark.asyncio
async def test_get_dag_run():
    transport = mock_transport({
        ("GET", "/api/v1/dags/my_dag/dagRuns/run1"): (200, {
            "dag_run_id": "run1", "state": "success",
        }),
    })
    client = make_client(transport=transport)
    result = await client.get_dag_run("my_dag", "run1")
    assert result["state"] == "success"
    await client.close()


@pytest.mark.asyncio
async def test_list_task_instances():
    transport = mock_transport({
        ("GET", "/api/v1/dags/my_dag/dagRuns/run1/taskInstances"): (200, {
            "task_instances": [
                {"task_id": "task_a", "state": "success", "duration": 5.0},
                {"task_id": "task_b", "state": "failed", "duration": 10.0},
            ],
            "total_entries": 2,
        }),
    })
    client = make_client(transport=transport)
    result = await client.list_task_instances("my_dag", "run1")
    assert len(result["task_instances"]) == 2
    await client.close()


@pytest.mark.asyncio
async def test_get_task_instance():
    transport = mock_transport({
        ("GET", "/api/v1/dags/my_dag/dagRuns/run1/taskInstances/task_a"): (200, {
            "task_id": "task_a", "state": "success", "duration": 5.0,
        }),
    })
    client = make_client(transport=transport)
    result = await client.get_task_instance("my_dag", "run1", "task_a")
    assert result["task_id"] == "task_a"
    await client.close()


@pytest.mark.asyncio
async def test_clear_task_instances():
    transport = mock_transport({
        ("POST", "/api/v1/dags/my_dag/clearTaskInstances"): (200, {
            "task_instances": [
                {"task_id": "task_a", "dag_run_id": "run1", "state": "cleared"},
                {"task_id": "task_b", "dag_run_id": "run1", "state": "cleared"},
            ],
        }),
    })
    client = make_client(transport=transport)
    result = await client.clear_task_instances(
        "my_dag", task_ids=["task_a"], dag_run_id="run1", downstream=True,
    )
    assert len(result["task_instances"]) == 2
    await client.close()


@pytest.mark.asyncio
async def test_list_variables():
    transport = mock_transport({
        ("GET", "/api/v1/variables"): (200, {
            "variables": [{"key": "env", "value": "prod"}],
            "total_entries": 1,
        }),
    })
    client = make_client(transport=transport)
    result = await client.list_variables()
    assert result["variables"][0]["key"] == "env"
    await client.close()


@pytest.mark.asyncio
async def test_get_variable():
    transport = mock_transport({
        ("GET", "/api/v1/variables/env"): (200, {"key": "env", "value": "prod"}),
    })
    client = make_client(transport=transport)
    result = await client.get_variable("env")
    assert result["value"] == "prod"
    await client.close()


@pytest.mark.asyncio
async def test_api_error():
    transport = mock_transport({
        ("GET", "/api/v1/dags/missing"): (404, {"detail": "DAG not found"}),
    })
    client = make_client(transport=transport)
    with pytest.raises(AirflowApiError) as exc_info:
        await client.get_dag("missing")
    assert exc_info.value.status_code == 404
    assert "DAG not found" in exc_info.value.detail
    await client.close()
