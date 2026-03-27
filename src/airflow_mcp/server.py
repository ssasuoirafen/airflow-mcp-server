import json
import os
import sys

from mcp.server.fastmcp import FastMCP

from airflow_mcp.client import AirflowClient

mcp = FastMCP(
    "airflow-mcp",
    instructions=(
        "Apache Airflow MCP server. Provides tools for managing DAGs, DAG runs, "
        "task instances, and variables via the Airflow REST API.\n"
        "DAGs: list_dags, get_dag, pause_dag, unpause_dag.\n"
        "Runs: trigger_dag, list_dag_runs, get_dag_run.\n"
        "Tasks: list_task_instances, get_task_instance, get_task_log, clear_task.\n"
        "Variables: list_variables, get_variable."
    ),
)

client: AirflowClient | None = None


def _init() -> AirflowClient:
    global client
    base_url = os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080/api/v1")
    username = os.environ.get("AIRFLOW_USERNAME", "airflow")
    password = os.environ.get("AIRFLOW_PASSWORD", "airflow")
    client = AirflowClient(base_url=base_url, username=username, password=password)
    return client


def _client() -> AirflowClient:
    if client is None:
        _init()
    return client


def _json(data: dict | list | str) -> str:
    if isinstance(data, str):
        return data
    return json.dumps(data, ensure_ascii=False, indent=2)


# --- DAGs ---


@mcp.tool()
async def list_dags(
    dag_id_pattern: str | None = None,
    only_active: bool = True,
    paused: bool | None = None,
    tags: list[str] | None = None,
    limit: int = 25,
) -> str:
    """List DAGs with optional filtering.

    dag_id_pattern: regex to filter DAG IDs (e.g. "sharepoint.*").
    tags: filter by tags.
    paused: filter by pause state (true/false/null for all).
    """
    data = await _client().list_dags(
        limit=limit,
        dag_id_pattern=dag_id_pattern,
        only_active=only_active,
        paused=paused,
        tags=tags,
    )
    dags = data.get("dags", [])
    result = []
    for d in dags:
        result.append({
            "dag_id": d.get("dag_id"),
            "is_paused": d.get("is_paused"),
            "is_active": d.get("is_active"),
            "schedule_interval": d.get("schedule_interval"),
            "tags": [t.get("name") for t in d.get("tags", [])],
        })
    return _json(result)


@mcp.tool()
async def get_dag(dag_id: str) -> str:
    """Get details of a specific DAG."""
    return _json(await _client().get_dag(dag_id))


@mcp.tool()
async def pause_dag(dag_id: str) -> str:
    """Pause a DAG."""
    data = await _client().set_dag_paused(dag_id, is_paused=True)
    return _json({"dag_id": dag_id, "is_paused": data.get("is_paused")})


@mcp.tool()
async def unpause_dag(dag_id: str) -> str:
    """Unpause a DAG."""
    data = await _client().set_dag_paused(dag_id, is_paused=False)
    return _json({"dag_id": dag_id, "is_paused": data.get("is_paused")})


# --- DAG Runs ---


@mcp.tool()
async def trigger_dag(
    dag_id: str,
    conf: str | None = None,
    logical_date: str | None = None,
    note: str | None = None,
) -> str:
    """Trigger a new DAG run.

    conf: JSON string with configuration parameters (e.g. '{"key": "value"}').
    logical_date: ISO datetime for the logical date.
    note: optional note for the DAG run.
    """
    conf_dict = json.loads(conf) if conf else None
    data = await _client().trigger_dag(
        dag_id, conf=conf_dict, logical_date=logical_date, note=note
    )
    return _json({
        "dag_id": data.get("dag_id"),
        "dag_run_id": data.get("dag_run_id"),
        "state": data.get("state"),
        "logical_date": data.get("logical_date"),
        "execution_date": data.get("execution_date"),
    })


@mcp.tool()
async def list_dag_runs(
    dag_id: str,
    limit: int = 10,
    state: str | None = None,
) -> str:
    """List recent DAG runs.

    state: filter by state (success, failed, running, queued).
    """
    data = await _client().list_dag_runs(dag_id, limit=limit, state=state)
    runs = data.get("dag_runs", [])
    result = []
    for r in runs:
        result.append({
            "dag_run_id": r.get("dag_run_id"),
            "state": r.get("state"),
            "execution_date": r.get("execution_date"),
            "start_date": r.get("start_date"),
            "end_date": r.get("end_date"),
        })
    return _json(result)


@mcp.tool()
async def get_dag_run(dag_id: str, dag_run_id: str) -> str:
    """Get details of a specific DAG run."""
    return _json(await _client().get_dag_run(dag_id, dag_run_id))


# --- Task Instances ---


@mcp.tool()
async def list_task_instances(
    dag_id: str,
    dag_run_id: str,
    state: str | None = None,
) -> str:
    """List task instances for a DAG run.

    state: filter by state (success, failed, running, upstream_failed, skipped).
    """
    data = await _client().list_task_instances(
        dag_id, dag_run_id, state=state
    )
    tasks = data.get("task_instances", [])
    result = []
    for t in tasks:
        result.append({
            "task_id": t.get("task_id"),
            "state": t.get("state"),
            "start_date": t.get("start_date"),
            "end_date": t.get("end_date"),
            "duration": t.get("duration"),
            "try_number": t.get("try_number"),
        })
    return _json(result)


@mcp.tool()
async def get_task_instance(dag_id: str, dag_run_id: str, task_id: str) -> str:
    """Get details of a specific task instance."""
    return _json(
        await _client().get_task_instance(dag_id, dag_run_id, task_id)
    )


@mcp.tool()
async def get_task_log(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int = 1,
) -> str:
    """Get logs for a task instance.

    try_number: which attempt to get logs for (default: 1).
    """
    return await _client().get_task_log(
        dag_id, dag_run_id, task_id, try_number=try_number
    )


@mcp.tool()
async def clear_task(
    dag_id: str,
    task_id: str,
    dag_run_id: str,
    downstream: bool = True,
    upstream: bool = False,
    only_failed: bool = False,
) -> str:
    """Clear a task instance to trigger re-execution.

    Clears the specified task (and optionally downstream/upstream) in a specific DAG run.
    This is the primary way to re-run tasks.

    downstream: also clear downstream tasks (default: true).
    upstream: also clear upstream tasks (default: false).
    only_failed: only clear failed tasks (default: false).
    """
    data = await _client().clear_task_instances(
        dag_id=dag_id,
        task_ids=[task_id],
        dag_run_id=dag_run_id,
        downstream=downstream,
        upstream=upstream,
        only_failed=only_failed,
    )
    tasks = data.get("task_instances", [])
    result = []
    for t in tasks:
        result.append({
            "task_id": t.get("task_id"),
            "dag_run_id": t.get("dag_run_id"),
            "state": t.get("state"),
        })
    return _json(result)


# --- Variables ---


@mcp.tool()
async def list_variables(limit: int = 100) -> str:
    """List Airflow variables."""
    data = await _client().list_variables(limit=limit)
    variables = data.get("variables", [])
    result = []
    for v in variables:
        result.append({
            "key": v.get("key"),
            "value": v.get("value"),
            "description": v.get("description"),
        })
    return _json(result)


@mcp.tool()
async def get_variable(key: str) -> str:
    """Get a specific Airflow variable by key."""
    return _json(await _client().get_variable(key))


def main():
    mcp.run()


if __name__ == "__main__":
    main()
