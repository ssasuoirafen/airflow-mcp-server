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
    """Trigger a new run of a DAG; returns the created run, normally "queued".

    If the DAG is paused, the run is created but stays queued until the DAG is
    unpaused (set_dag_paused). Airflow rejects a dag_run_id or logical_date that
    already has a run (HTTP 409).

    Args:
        dag_id: The DAG to run.
        conf: Optional run configuration passed to the DAG.
        logical_date: Optional ISO-8601 logical date with a timezone offset,
            e.g. "2026-09-24T00:00:00+00:00"; naive datetimes are rejected.
            Defaults to now.
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

    Pausing stops new runs and stops runs already in progress from scheduling
    further tasks; tasks already running finish. Unpausing a DAG with catchup
    enabled creates runs for every schedule interval missed while paused.

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
    reset_dag_runs: bool = True,
    dry_run: bool = False,
) -> TaskInstanceList:
    """Clear task instances so they re-run (retry).

    Clearing resets task state and the scheduler re-runs them, so it is
    destructive - it can re-execute work. Use ``dry_run=True`` first to preview
    exactly which task instances would be affected without changing anything.

    Scope: without dag_run_id the clear spans every run of the DAG, and without
    task_ids every task in those runs. only_failed defaults to False, so tasks
    in any state are cleared: a call with only dag_id re-runs the DAG's entire
    history.

    Args:
        dag_id: The DAG.
        dag_run_id: Restrict to one run. Omit only to clear across all runs.
        task_ids: Restrict to these task ids; omit for every task in the
            selected run(s).
        include_downstream: With task_ids, also clear their downstream tasks.
        include_upstream: With task_ids, also clear their upstream tasks.
        only_failed: Only clear failed task instances. Defaults to False.
        reset_dag_runs: Reopen a finished DAG run so the scheduler picks the
            cleared tasks up. Airflow only touches runs already in a finished
            state, so this is inert on a run that is still going. Without it a
            clear on a finished run resets the tasks but nothing ever starts.
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
            reset_dag_runs=reset_dag_runs,
            dry_run=dry_run,
        )
