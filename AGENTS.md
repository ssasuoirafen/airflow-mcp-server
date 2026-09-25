# airflow-mcp-server

- MCP server exposing a curated set of Apache Airflow 2 tools over the stable REST API (`/api/v1`). Airflow 3 is deliberately out of scope; see `README.md` for the current tool list.
- Public repository under github.com/ssasuoirafen. Nothing from an employer may appear here: no internal hostnames, DAG names, data, screenshots or credentials, in code, tests, fixtures or commit messages.
- Everything is English: code, documentation, commits, PR descriptions, issues.
- It runs as a local stdio server and each user supplies their own Airflow credentials, which keeps Airflow RBAC intact. Keep that model: no shared service account, no bundled credentials, no hosted mode.
- Layout: one module per tool group under `src/airflow_mcp_server/tools/`, HTTP in `client.py`, settings in `config.py`, error mapping in `errors.py`. A new tool goes into the matching tools module, not into `server.py`.
- Write tools must stay refusable through `AIRFLOW_MCP_READ_ONLY=true`. A new write tool honors that flag and ships with a test asserting the refusal.
- `uv sync`, then `uv run pytest -q`. End-to-end tests hit a real Airflow and are deselected by the pytest addopts, so run them deliberately when they matter.
- Releases are consumed as `uvx --from git+https://...@vX.Y.Z`, so the git tag and the version in `pyproject.toml` must agree. CI enforces this on tags; bump the version in the same change that gets tagged.
- `.env` is ignored and points at a local Airflow. `.env.example` is the tracked template and carries no real values.
- `.python-version` pins 3.14 and is tracked on purpose, while `pyproject.toml` still supports 3.11 and up. Changing either floor means changing both deliberately.
