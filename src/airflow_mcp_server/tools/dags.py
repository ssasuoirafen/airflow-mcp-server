"""Tools for listing/inspecting DAGs and surfacing DAG import errors."""

from __future__ import annotations

from ..app import airflow_errors, get_client, mcp
from ..models import DagList, DagSummary, ImportErrorList


@mcp.tool(annotations={"readOnlyHint": True})
def list_dags(
    limit: int = 50,
    offset: int = 0,
    only_active: bool = True,
    paused: bool | None = None,
    tags: list[str] | None = None,
    dag_id_pattern: str | None = None,
) -> DagList:
    """List DAGs with their pause state, schedule, owners, and tags.

    Args:
        limit: Max DAGs to return (Airflow caps this at 100).
        offset: DAGs to skip, for paging through a large catalog.
        only_active: Exclude DAGs whose source files were deleted.
        paused: Filter by paused state; omit to include both.
        tags: Only DAGs carrying any of these tags.
        dag_id_pattern: Case-insensitive substring match on dag_id.
    """
    with airflow_errors():
        return get_client().list_dags(
            limit=limit,
            offset=offset,
            only_active=only_active,
            paused=paused,
            tags=tags,
            dag_id_pattern=dag_id_pattern,
        )


@mcp.tool(annotations={"readOnlyHint": True})
def get_dag(dag_id: str) -> DagSummary:
    """Get a single DAG's details by dag_id."""
    with airflow_errors():
        return get_client().get_dag(dag_id)


@mcp.tool(annotations={"readOnlyHint": True})
def list_import_errors(limit: int = 50, offset: int = 0) -> ImportErrorList:
    """List DAG import errors (parse failures), with filename and stack trace.

    The quickest way to find why a DAG is missing from the list or broken.
    """
    with airflow_errors():
        return get_client().list_import_errors(limit=limit, offset=offset)
