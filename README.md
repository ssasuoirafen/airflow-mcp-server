# airflow-mcp-server

MCP server for Apache Airflow 2.x REST API.

## Tools

| Tool | Description |
|------|-------------|
| `list_dags` | List DAGs with filtering by pattern, tags, pause state |
| `get_dag` | Get DAG details |
| `pause_dag` | Pause a DAG |
| `unpause_dag` | Unpause a DAG |
| `trigger_dag` | Trigger a new DAG run |
| `list_dag_runs` | List recent DAG runs |
| `get_dag_run` | Get DAG run details |
| `list_task_instances` | List task instances in a DAG run |
| `get_task_instance` | Get task instance details |
| `get_task_log` | Get task logs |
| `clear_task` | Clear a task to trigger re-execution (with downstream/upstream) |
| `list_variables` | List Airflow variables |
| `get_variable` | Get a variable value |

## Configuration

### Claude Code (`.mcp.json`)

```json
{
  "mcpServers": {
    "airflow": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/ssasuoirafen/airflow-mcp-server", "airflow-mcp"],
      "env": {
        "AIRFLOW_BASE_URL": "https://airflow.example.com/api/v1",
        "AIRFLOW_USERNAME": "your-username",
        "AIRFLOW_PASSWORD": "your-password"
      }
    }
  }
}
```

| Variable | Description |
|----------|-------------|
| `AIRFLOW_BASE_URL` | Airflow REST API base URL (e.g. `https://airflow.example.com/api/v1`) |
| `AIRFLOW_USERNAME` | Basic auth username |
| `AIRFLOW_PASSWORD` | Basic auth password |

## Development

```bash
git clone https://github.com/ssasuoirafen/airflow-mcp-server.git
cd airflow-mcp-server
uv sync --extra dev
uv run pytest tests/ -v
```

## License

MIT
