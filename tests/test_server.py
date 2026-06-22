"""The server wires up and registers the expected tools."""

from __future__ import annotations

import asyncio


def test_expected_tools_are_registered() -> None:
    import airflow_mcp_server.server  # noqa: F401  -- triggers tool registration
    from airflow_mcp_server.app import mcp

    tools = asyncio.run(mcp.list_tools())
    names = {tool.name for tool in tools}

    expected = {
        "get_airflow_version",
        "get_airflow_health",
        "list_pools",
        "list_dags",
        "get_dag",
        "list_import_errors",
        "list_dag_runs",
        "get_dag_run",
        "list_task_instances",
        "get_task_instance",
        "get_task_logs",
        "trigger_dag_run",
        "set_dag_paused",
        "clear_task_instances",
    }
    assert expected <= names
