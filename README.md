# airflow-mcp-server

An MCP server that lets Claude inspect and operate **Apache Airflow** over its
REST API. It exposes safe, curated tools (read + a few guarded writes) rather
than mirroring the whole API.

Targets **Airflow 2** (stable REST API `/api/v1`). Airflow 3 is intentionally
out of scope.

Runs as a local stdio server: each user runs it on their own machine with their
own Airflow credentials, which keeps Airflow RBAC intact.

## Status

Early. Currently implemented: connectivity tools (`get_airflow_version`,
`get_airflow_health`) and the client foundation (auth, error mapping, an
Airflow-2 guard). DAG / run / task-instance / logs tools and safe writes are
next - see the roadmap below.

## Configuration

All settings come from `AIRFLOW_MCP_*` environment variables (prefixed to avoid
clashing with Airflow's own env). See [`.env.example`](.env.example).

| Variable | Required | Default | Notes |
| --- | --- | --- | --- |
| `AIRFLOW_MCP_BASE_URL` | yes | - | e.g. `http://localhost:8080` (no `/api` suffix) |
| `AIRFLOW_MCP_USERNAME` / `AIRFLOW_MCP_PASSWORD` | one auth method | - | Basic auth |
| `AIRFLOW_MCP_API_TOKEN` | one auth method | - | Bearer token; wins over basic auth |
| `AIRFLOW_MCP_READ_ONLY` | no | `false` | `true` disables every write tool |
| `AIRFLOW_MCP_VERIFY_SSL` | no | `true` | |
| `AIRFLOW_MCP_TIMEOUT` | no | `30` | seconds |

## Use with Claude

Until published to PyPI, point your MCP client at the local checkout:

```json
{
  "mcpServers": {
    "airflow": {
      "command": "uv",
      "args": ["run", "--directory", "C:\\path\\to\\airflow-mcp-server", "airflow-mcp-server"],
      "env": {
        "AIRFLOW_MCP_BASE_URL": "http://localhost:8080",
        "AIRFLOW_MCP_USERNAME": "airflow",
        "AIRFLOW_MCP_PASSWORD": "airflow"
      }
    }
  }
}
```

Once published, this simplifies to `"command": "uvx", "args": ["airflow-mcp-server"]`.

## Development

```bash
uv sync            # install deps
uv run pytest      # run the test suite
uv run airflow-mcp-server   # run the server (expects an MCP client on stdio)
```

## Roadmap

1. Foundation + connectivity (version, health). **done**
2. Read tools: DAGs, DAG runs, task instances, logs, import errors, pools.
3. Safe writes: trigger DAG, pause/unpause, clear/retry tasks (gated by read-only).
4. Variables (with secret masking), packaging, publish.
