"""Tools for inspecting DAG runs."""

from __future__ import annotations

from ..app import airflow_errors, get_client, mcp
from ..models import DagRun, DagRunList


@mcp.tool(annotations={"readOnlyHint": True})
def list_dag_runs(
    dag_id: str,
    limit: int = 50,
    offset: int = 0,
    state: list[str] | None = None,
    order_by: str = "-execution_date",
) -> DagRunList:
    """List runs of a DAG, most recent first.

    Args:
        dag_id: The DAG to list runs for.
        limit: Max runs to return (Airflow caps this at 100).
        offset: Runs to skip.
        state: Filter by run state, e.g. ["failed", "running"].
        order_by: Sort field; defaults to newest first.
    """
    with airflow_errors():
        return get_client().list_dag_runs(
            dag_id, limit=limit, offset=offset, state=state, order_by=order_by
        )


@mcp.tool(annotations={"readOnlyHint": True})
def get_dag_run(dag_id: str, dag_run_id: str) -> DagRun:
    """Get a single DAG run by dag_id and dag_run_id."""
    with airflow_errors():
        return get_client().get_dag_run(dag_id, dag_run_id)
