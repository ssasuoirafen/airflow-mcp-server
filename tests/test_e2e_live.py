"""Opt-in end-to-end test against a REAL Airflow 2.

Exercises the full path: MCP protocol -> tool -> client -> live Airflow, using
FastMCP's in-memory client. Deselected by default (marked ``e2e``) and skipped
unless a ``.env`` with ``AIRFLOW_MCP_*`` values exists at the project root.

Run it explicitly (``-s`` shows the printed payloads):

    uv run pytest -m e2e -s
"""

from __future__ import annotations

import asyncio
import pathlib
from typing import Any

import pytest

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        not ENV_FILE.exists(),
        reason="live test needs a .env with AIRFLOW_MCP_* values at the project root",
    ),
]


def _items(obj: Any, key: str) -> list[Any]:
    """Read a list field from a tool result (dict or reconstructed model)."""
    if obj is None:
        return []
    if isinstance(obj, dict):
        return obj.get(key) or []
    return getattr(obj, key, None) or []


def _attr(obj: Any, name: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)


def test_live_read_chain() -> None:
    from fastmcp import Client

    import airflow_mcp_server.server  # noqa: F401  -- registers the tools
    from airflow_mcp_server.app import mcp

    async def _run() -> dict[str, Any]:
        out: dict[str, Any] = {}
        async with Client(mcp) as client:

            async def call(name: str, args: dict[str, Any] | None = None) -> Any:
                return (await client.call_tool(name, args or {})).data

            out["version"] = await call("get_airflow_version")
            out["health"] = await call("get_airflow_health")
            out["pools"] = await call("list_pools", {"limit": 5})
            out["import_errors"] = await call("list_import_errors", {"limit": 5})
            out["dags"] = await call("list_dags", {"limit": 5})

            # Drill down through the read chain when there's data to use.
            dags = _items(out["dags"], "dags")
            if dags:
                dag_id = _attr(dags[0], "dag_id")
                out["runs"] = await call("list_dag_runs", {"dag_id": dag_id, "limit": 3})
                runs = _items(out["runs"], "dag_runs")
                if runs:
                    run_id = _attr(runs[0], "dag_run_id")
                    out["task_instances"] = await call(
                        "list_task_instances",
                        {"dag_id": dag_id, "dag_run_id": run_id, "limit": 5},
                    )
        return out

    result = asyncio.run(_run())

    for key, value in result.items():
        print(f"\n[e2e] {key}: {value}")

    # Always-true invariants on any Airflow 2 instance.
    assert result["version"] is not None
    assert result["health"] is not None
    assert result["dags"] is not None
