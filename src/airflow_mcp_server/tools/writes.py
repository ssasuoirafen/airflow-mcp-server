"""Write tools: trigger runs, pause/unpause DAGs, clear (retry) task instances.

Each is gated by ``require_writable`` so it refuses in read-only mode, and
carries ``readOnlyHint=False`` so hosts can prompt before running it.
"""

from __future__ import annotations

from typing import Any

from ..app import airflow_errors, get_client, mcp, require_writable
from ..models import DagRun, DagSummary, TaskInstanceList


@mcp.tool(annotations={"readOnlyHint": False, "destructiveHint": False})
def trigger_dag_run(
    dag_id: str,
    conf: dict[str, Any] | None = None,
    logical_date: str | None = None,
    dag_run_id: str | None = None,
    note: str | None = None,
) -> DagRun:
    """Trigger a new run of a DAG.

    Args:
        dag_id: The DAG to run.
        conf: Optional run configuration passed to the DAG.
        logical_date: Optional ISO-8601 logical date; defaults to now.
        dag_run_id: Optional explicit run id; Airflow generates one if omitted.
        note: Optional note attached to the run.
    """
    with airflow_errors():
        require_writable()
        return get_client().trigger_dag_run(
            dag_id,
            conf=conf,
            logical_date=logical_date,
            dag_run_id=dag_run_id,
            note=note,
        )


@mcp.tool(
    annotations={
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
    }
)
def set_dag_paused(dag_id: str, is_paused: bool) -> DagSummary:
    """Pause or unpause a DAG.

    Args:
        dag_id: The DAG to update.
        is_paused: True to pause (stop scheduling), False to unpause.
    """
    with airflow_errors():
        require_writable()
        return get_client().set_dag_paused(dag_id, is_paused)


@mcp.tool(annotations={"readOnlyHint": False, "destructiveHint": True})
def clear_task_instances(
    dag_id: str,
    dag_run_id: str | None = None,
    task_ids: list[str] | None = None,
    include_downstream: bool = False,
    include_upstream: bool = False,
    only_failed: bool = False,
    dry_run: bool = False,
) -> TaskInstanceList:
    """Clear task instances so they re-run (retry).

    Clearing resets task state and the scheduler re-runs them, so it is
    destructive - it can re-execute work. Use ``dry_run=True`` first to preview
    exactly which task instances would be affected without changing anything.

    Args:
        dag_id: The DAG.
        dag_run_id: Restrict to a single run (recommended).
        task_ids: Restrict to specific task ids; omit for all tasks in scope.
        include_downstream: Also clear downstream tasks.
        include_upstream: Also clear upstream tasks.
        only_failed: Only clear failed task instances.
        dry_run: If True, report what would be cleared without doing it.
    """
    with airflow_errors():
        require_writable()
        return get_client().clear_task_instances(
            dag_id,
            dag_run_id=dag_run_id,
            task_ids=task_ids,
            include_downstream=include_downstream,
            include_upstream=include_upstream,
            only_failed=only_failed,
            dry_run=dry_run,
        )
