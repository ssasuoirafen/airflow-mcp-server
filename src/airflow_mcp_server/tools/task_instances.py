"""Tools for inspecting task instances within a DAG run."""

from __future__ import annotations

from ..app import airflow_errors, get_client, mcp
from ..models import TaskInstance, TaskInstanceList


@mcp.tool(annotations={"readOnlyHint": True})
def list_task_instances(
    dag_id: str,
    dag_run_id: str,
    limit: int = 100,
    offset: int = 0,
    state: list[str] | None = None,
) -> TaskInstanceList:
    """List task instances in a DAG run, with their states and timings.

    Args:
        dag_id: The DAG.
        dag_run_id: The run to inspect.
        limit: Max task instances to return (Airflow caps this at 100).
        offset: Task instances to skip.
        state: Filter by task state, e.g. ["failed", "upstream_failed"].
    """
    with airflow_errors():
        return get_client().list_task_instances(
            dag_id, dag_run_id, limit=limit, offset=offset, state=state
        )


@mcp.tool(annotations={"readOnlyHint": True})
def get_task_instance(dag_id: str, dag_run_id: str, task_id: str) -> TaskInstance:
    """Get a single task instance by dag_id, dag_run_id, and task_id."""
    with airflow_errors():
        return get_client().get_task_instance(dag_id, dag_run_id, task_id)
