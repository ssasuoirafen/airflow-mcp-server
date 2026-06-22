"""The FastMCP app plus the lazily-built, memoized Airflow client.

Kept separate from ``server.py`` (which imports the tool modules) so tool
modules can import ``mcp`` and ``get_client`` without an import cycle.
"""

from __future__ import annotations

import atexit
import functools
from collections.abc import Iterator
from contextlib import contextmanager

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

from .client import AirflowClient
from .config import Settings
from .errors import AirflowError

mcp = FastMCP(
    name="airflow-mcp-server",
    instructions=(
        "Inspect and operate Apache Airflow 2 over its REST API. Read tools are "
        "always safe. Write tools (trigger, pause, clear) honor a server-side "
        "read-only switch and refuse when it is enabled."
    ),
)


@functools.lru_cache(maxsize=1)
def get_client() -> AirflowClient:
    """Build and cache the Airflow client on first use.

    Settings come from the environment and the Airflow-2 guard check runs here,
    so a misconfigured or unreachable Airflow shows up as an actionable tool
    error rather than crashing server startup. Failures are not cached, so a
    later call retries cleanly once the problem is fixed.
    """
    client = AirflowClient(Settings())
    try:
        client.ensure_supported()
    except Exception:
        client.close()
        raise
    return client


@atexit.register
def _close_client() -> None:
    if get_client.cache_info().currsize:
        get_client().close()


@contextmanager
def airflow_errors() -> Iterator[None]:
    """Translate client exceptions into user-visible MCP tool errors.

    ``ToolError`` messages are surfaced to the caller verbatim, unlike generic
    exceptions which FastMCP may mask.
    """
    try:
        yield
    except AirflowError as exc:
        raise ToolError(str(exc)) from exc


def require_writable() -> None:
    """Refuse a write when the server is configured read-only (defense in depth).

    Write tools also carry ``readOnlyHint=False`` so hosts can prompt, but this
    is the hard stop regardless of host behavior.
    """
    if get_client().read_only:
        raise ToolError(
            "Server is in read-only mode (AIRFLOW_MCP_READ_ONLY=true); "
            "this write operation is refused."
        )
