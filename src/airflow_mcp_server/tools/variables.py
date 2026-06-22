"""Tools for reading Airflow Variables.

Heads-up: variable values can contain secrets. Airflow masks values for
sensitive-looking keys (containing password/secret/token/api_key/...), but
secrets stored under ordinary key names are returned as-is. This server returns
values by default.
"""

from __future__ import annotations

from ..app import airflow_errors, get_client, mcp
from ..models import Variable, VariableList


@mcp.tool(annotations={"readOnlyHint": True})
def list_variables(limit: int = 50, offset: int = 0) -> VariableList:
    """List Airflow Variables (key, value, description).

    Values can contain secrets. Airflow masks sensitive-looking keys, but not
    secrets stored under ordinary key names.
    """
    with airflow_errors():
        return get_client().list_variables(limit=limit, offset=offset)


@mcp.tool(annotations={"readOnlyHint": True})
def get_variable(key: str) -> Variable:
    """Get one Airflow Variable by key, including its value.

    See the secrets note on `list_variables` - the value may be sensitive.
    """
    with airflow_errors():
        return get_client().get_variable(key)
