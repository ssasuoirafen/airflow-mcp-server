"""System / diagnostic tools: version and health."""

from __future__ import annotations

from ..app import airflow_errors, get_client, mcp
from ..models import HealthStatus, PoolList, VersionInfo


@mcp.tool(annotations={"readOnlyHint": True})
def get_airflow_version() -> VersionInfo:
    """Return the Airflow version reported by the API.

    Handy for confirming connectivity and which Airflow major version is in use.
    """
    with airflow_errors():
        return get_client().get_version()


@mcp.tool(annotations={"readOnlyHint": True})
def get_airflow_health() -> HealthStatus:
    """Return Airflow component health: metadatabase, scheduler, triggerer, dag-processor."""
    with airflow_errors():
        return get_client().get_health()


@mcp.tool(annotations={"readOnlyHint": True})
def list_pools(limit: int = 50, offset: int = 0) -> PoolList:
    """List worker pools with their slot usage (occupied/running/queued/open)."""
    with airflow_errors():
        return get_client().list_pools(limit=limit, offset=offset)
