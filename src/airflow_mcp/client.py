import httpx


class AirflowApiError(Exception):
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"Airflow API error {status_code}: {detail}")


class AirflowClient:
    def __init__(self, base_url: str, username: str, password: str):
        self._http = httpx.AsyncClient(
            base_url=base_url,
            auth=httpx.BasicAuth(username, password),
            timeout=30.0,
            headers={"Accept": "application/json"},
        )

    def _raise_for_status(self, resp: httpx.Response) -> None:
        if resp.status_code >= 400:
            try:
                detail = resp.json().get("detail", resp.text)
            except Exception:
                detail = resp.text
            raise AirflowApiError(resp.status_code, detail)

    async def _get(self, path: str, params: dict | None = None) -> dict:
        resp = await self._http.get(path, params=params)
        self._raise_for_status(resp)
        return resp.json()

    async def _post(self, path: str, json: dict | None = None) -> dict:
        resp = await self._http.post(path, json=json)
        self._raise_for_status(resp)
        return resp.json()

    async def _patch(self, path: str, json: dict) -> dict:
        resp = await self._http.patch(path, json=json)
        self._raise_for_status(resp)
        return resp.json()

    async def _get_text(self, path: str, params: dict | None = None) -> str:
        resp = await self._http.get(
            path,
            params=params,
            headers={"Accept": "text/plain"},
        )
        self._raise_for_status(resp)
        return resp.text

    async def close(self) -> None:
        await self._http.aclose()

    # --- DAGs ---

    async def list_dags(
        self,
        limit: int = 25,
        offset: int = 0,
        dag_id_pattern: str | None = None,
        only_active: bool = True,
        paused: bool | None = None,
        tags: list[str] | None = None,
    ) -> dict:
        params: dict = {"limit": limit, "offset": offset, "only_active": only_active}
        if dag_id_pattern:
            params["dag_id_pattern"] = dag_id_pattern
        if paused is not None:
            params["paused"] = paused
        if tags:
            params["tags"] = tags
        return await self._get("/dags", params=params)

    async def get_dag(self, dag_id: str) -> dict:
        return await self._get(f"/dags/{dag_id}")

    async def set_dag_paused(self, dag_id: str, is_paused: bool) -> dict:
        return await self._patch(f"/dags/{dag_id}", json={"is_paused": is_paused})

    # --- DAG Runs ---

    async def trigger_dag(
        self,
        dag_id: str,
        conf: dict | None = None,
        logical_date: str | None = None,
        note: str | None = None,
    ) -> dict:
        body: dict = {}
        if conf:
            body["conf"] = conf
        if logical_date:
            body["logical_date"] = logical_date
        if note:
            body["note"] = note
        return await self._post(f"/dags/{dag_id}/dagRuns", json=body)

    async def list_dag_runs(
        self,
        dag_id: str,
        limit: int = 10,
        offset: int = 0,
        state: str | None = None,
        order_by: str = "-execution_date",
    ) -> dict:
        params: dict = {"limit": limit, "offset": offset, "order_by": order_by}
        if state:
            params["state"] = state
        return await self._get(f"/dags/{dag_id}/dagRuns", params=params)

    async def get_dag_run(self, dag_id: str, dag_run_id: str) -> dict:
        return await self._get(f"/dags/{dag_id}/dagRuns/{dag_run_id}")

    async def set_dag_run_state(self, dag_id: str, dag_run_id: str, state: str) -> dict:
        return await self._patch(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}", json={"state": state}
        )

    # --- Task Instances ---

    async def list_task_instances(
        self,
        dag_id: str,
        dag_run_id: str,
        limit: int = 100,
        state: str | None = None,
    ) -> dict:
        params: dict = {"limit": limit}
        if state:
            params["state"] = state
        return await self._get(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
            params=params,
        )

    async def get_task_instance(
        self, dag_id: str, dag_run_id: str, task_id: str
    ) -> dict:
        return await self._get(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"
        )

    async def get_task_log(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        try_number: int = 1,
        full_content: bool = True,
    ) -> str:
        params: dict = {"full_content": full_content}
        return await self._get_text(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}",
            params=params,
        )

    async def clear_task_instances(
        self,
        dag_id: str,
        task_ids: list[str],
        dag_run_id: str | None = None,
        downstream: bool = False,
        upstream: bool = False,
        only_failed: bool = False,
    ) -> dict:
        body: dict = {
            "task_ids": task_ids,
            "include_downstream": downstream,
            "include_upstream": upstream,
            "only_failed": only_failed,
        }
        if dag_run_id:
            body["dag_run_id"] = dag_run_id
        return await self._post(f"/dags/{dag_id}/clearTaskInstances", json=body)

    # --- Variables ---

    async def list_variables(self, limit: int = 100, offset: int = 0) -> dict:
        return await self._get(
            "/variables", params={"limit": limit, "offset": offset}
        )

    async def get_variable(self, key: str) -> dict:
        return await self._get(f"/variables/{key}")
