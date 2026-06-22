"""Entry point: wire tools onto the app and run the stdio server."""

from __future__ import annotations

from . import tools  # noqa: F401  -- import registers tools via side effects
from .app import mcp


def main() -> None:
    """Run the MCP server over stdio (the console-script entry point)."""
    mcp.run()


__all__ = ["main", "mcp"]
