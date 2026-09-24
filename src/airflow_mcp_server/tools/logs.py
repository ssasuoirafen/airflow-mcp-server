"""Tool for reading task logs."""

from __future__ import annotations

from ..app import airflow_errors, get_client, mcp

_DEFAULT_TAIL = 20_000


@mcp.tool(annotations={"readOnlyHint": True})
def get_task_logs(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int = 1,
    tail_chars: int = _DEFAULT_TAIL,
) -> str:
    """Read the log for one task attempt.

    Task logs can be large, so by default only the trailing portion is returned
    (where the error and traceback usually are). Mapped task instances
    (map_index >= 0 in list_task_instances) are not supported: Airflow answers
    404 for them.

    Args:
        dag_id: The DAG.
        dag_run_id: The run.
        task_id: The task.
        try_number: Which attempt (1-based). Defaults to 1, the first attempt;
            a retried task's latest attempt is the try_number reported by
            list_task_instances or get_task_instance.
        tail_chars: Return at most this many trailing characters. 0 = full log.
    """
    with airflow_errors():
        text = get_client().get_task_logs(dag_id, dag_run_id, task_id, try_number)
    if tail_chars and len(text) > tail_chars:
        omitted = len(text) - tail_chars
        header = f"... [truncated {omitted} leading chars of {len(text)} total] ...\n"
        return header + text[-tail_chars:]
    return text
