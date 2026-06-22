"""Airflow 2.x client - the stable REST API under ``/api/v1``.

A single concrete client. Shared HTTP/auth/error handling and the v1 endpoint
methods live together; there's no version abstraction because only Airflow 2 is
in scope. ``ensure_supported`` gives a clear error if pointed at something else.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

import httpx

from .config import Settings
from .errors import (
    AirflowAPIError,
    AirflowAuthError,
    AirflowError,
    AirflowNotFoundError,
    AirflowRequestError,
)
from .models import (
    DagList,
    DagRun,
    DagRunList,
    DagSummary,
    HealthStatus,
    ImportErrorList,
    PoolList,
    TaskInstance,
    TaskInstanceList,
    VersionInfo,
)

API_ROOT = "/api/v1"


def _build_auth(settings: Settings) -> tuple[httpx.Auth | None, dict[str, str]]:
    """Return the (httpx auth, extra headers) pair for the configured credentials.

    A bearer token wins over basic auth when both are present. ``Settings``
    guarantees at least one is set.
    """
    if settings.api_token:
        return None, {"Authorization": f"Bearer {settings.api_token}"}
    return httpx.BasicAuth(settings.username, settings.password), {}  # type: ignore[arg-type]


def _extract_detail(response: httpx.Response) -> str:
    """Pull a human-readable message out of an Airflow error response."""
    try:
        body = response.json()
    except ValueError:
        return response.text or f"HTTP {response.status_code}"
    if isinstance(body, dict):
        for key in ("detail", "title", "message"):
            value = body.get(key)
            if value:
                return str(value)
    return response.text or f"HTTP {response.status_code}"


def _seg(value: object) -> str:
    """URL-encode a single path segment (run ids contain ':' and '+')."""
    return quote(str(value), safe="")


def _params(**kwargs: Any) -> dict[str, Any]:
    """Drop ``None`` values so we only send the filters the caller set."""
    return {key: value for key, value in kwargs.items() if value is not None}


class AirflowClient:
    """Talks to Airflow 2.x via the stable v1 REST API."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        auth, extra_headers = _build_auth(settings)
        self._http = httpx.Client(
            base_url=settings.base_url,
            auth=auth,
            headers={"Accept": "application/json", **extra_headers},
            timeout=settings.timeout,
            verify=settings.verify_ssl,
        )

    # ---- lifecycle ----------------------------------------------------------

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "AirflowClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def read_only(self) -> bool:
        return self._settings.read_only

    # ---- transport ----------------------------------------------------------

    def _send(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Issue a request against ``/api/v1 + path``, raising on non-2xx."""
        try:
            response = self._http.request(method, f"{API_ROOT}{path}", **kwargs)
        except httpx.RequestError as exc:
            raise AirflowRequestError(
                f"Could not reach Airflow at {self._settings.base_url}: {exc}"
            ) from exc
        if not response.is_success:
            detail = _extract_detail(response)
            if response.status_code in (401, 403):
                raise AirflowAuthError(detail, status_code=response.status_code)
            if response.status_code == 404:
                raise AirflowNotFoundError(detail, status_code=response.status_code)
            raise AirflowAPIError(detail, status_code=response.status_code)
        return response

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        """Like ``_send`` but parse and return the JSON body (``None`` if empty)."""
        response = self._send(method, path, **kwargs)
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    # ---- diagnostics --------------------------------------------------------

    def get_version(self) -> VersionInfo:
        return VersionInfo.model_validate(self._request("GET", "/version"))

    def get_health(self) -> HealthStatus:
        return HealthStatus.model_validate(self._request("GET", "/health"))

    # ---- DAGs ---------------------------------------------------------------

    def list_dags(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        only_active: bool = True,
        paused: bool | None = None,
        tags: list[str] | None = None,
        dag_id_pattern: str | None = None,
    ) -> DagList:
        params = _params(
            limit=limit,
            offset=offset,
            only_active=only_active,
            paused=paused,
            tags=tags,
            dag_id_pattern=dag_id_pattern,
        )
        return DagList.model_validate(self._request("GET", "/dags", params=params))

    def get_dag(self, dag_id: str) -> DagSummary:
        return DagSummary.model_validate(self._request("GET", f"/dags/{_seg(dag_id)}"))

    # ---- DAG runs -----------------------------------------------------------

    def list_dag_runs(
        self,
        dag_id: str,
        *,
        limit: int = 50,
        offset: int = 0,
        state: list[str] | None = None,
        order_by: str = "-execution_date",
    ) -> DagRunList:
        params = _params(limit=limit, offset=offset, state=state, order_by=order_by)
        data = self._request("GET", f"/dags/{_seg(dag_id)}/dagRuns", params=params)
        return DagRunList.model_validate(data)

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> DagRun:
        data = self._request(
            "GET", f"/dags/{_seg(dag_id)}/dagRuns/{_seg(dag_run_id)}"
        )
        return DagRun.model_validate(data)

    # ---- task instances -----------------------------------------------------

    def list_task_instances(
        self,
        dag_id: str,
        dag_run_id: str,
        *,
        limit: int = 100,
        offset: int = 0,
        state: list[str] | None = None,
    ) -> TaskInstanceList:
        params = _params(limit=limit, offset=offset, state=state)
        data = self._request(
            "GET",
            f"/dags/{_seg(dag_id)}/dagRuns/{_seg(dag_run_id)}/taskInstances",
            params=params,
        )
        return TaskInstanceList.model_validate(data)

    def get_task_instance(
        self, dag_id: str, dag_run_id: str, task_id: str
    ) -> TaskInstance:
        data = self._request(
            "GET",
            f"/dags/{_seg(dag_id)}/dagRuns/{_seg(dag_run_id)}"
            f"/taskInstances/{_seg(task_id)}",
        )
        return TaskInstance.model_validate(data)

    # ---- logs ---------------------------------------------------------------

    def get_task_logs(
        self, dag_id: str, dag_run_id: str, task_id: str, try_number: int = 1
    ) -> str:
        """Return the raw log text for one task try."""
        response = self._send(
            "GET",
            f"/dags/{_seg(dag_id)}/dagRuns/{_seg(dag_run_id)}"
            f"/taskInstances/{_seg(task_id)}/logs/{int(try_number)}",
            headers={"Accept": "text/plain"},
        )
        return response.text

    # ---- import errors & pools ---------------------------------------------

    def list_import_errors(
        self, *, limit: int = 50, offset: int = 0
    ) -> ImportErrorList:
        params = _params(limit=limit, offset=offset)
        return ImportErrorList.model_validate(
            self._request("GET", "/importErrors", params=params)
        )

    def list_pools(self, *, limit: int = 50, offset: int = 0) -> PoolList:
        params = _params(limit=limit, offset=offset)
        return PoolList.model_validate(self._request("GET", "/pools", params=params))

    # ---- writes -------------------------------------------------------------

    def trigger_dag_run(
        self,
        dag_id: str,
        *,
        conf: dict[str, Any] | None = None,
        logical_date: str | None = None,
        dag_run_id: str | None = None,
        note: str | None = None,
    ) -> DagRun:
        body = _params(
            conf=conf, logical_date=logical_date, dag_run_id=dag_run_id, note=note
        )
        data = self._request("POST", f"/dags/{_seg(dag_id)}/dagRuns", json=body)
        return DagRun.model_validate(data)

    def set_dag_paused(self, dag_id: str, is_paused: bool) -> DagSummary:
        data = self._request(
            "PATCH", f"/dags/{_seg(dag_id)}", json={"is_paused": is_paused}
        )
        return DagSummary.model_validate(data)

    def clear_task_instances(
        self,
        dag_id: str,
        *,
        dag_run_id: str | None = None,
        task_ids: list[str] | None = None,
        include_downstream: bool = False,
        include_upstream: bool = False,
        only_failed: bool = False,
        dry_run: bool = False,
    ) -> TaskInstanceList:
        body: dict[str, Any] = {
            "dry_run": dry_run,
            "include_downstream": include_downstream,
            "include_upstream": include_upstream,
            "only_failed": only_failed,
        }
        if dag_run_id is not None:
            body["dag_run_id"] = dag_run_id
        if task_ids is not None:
            body["task_ids"] = task_ids
        data = self._request(
            "POST", f"/dags/{_seg(dag_id)}/clearTaskInstances", json=body
        )
        return TaskInstanceList.model_validate(data)

    # ---- guard --------------------------------------------------------------

    def ensure_supported(self) -> None:
        """Confirm the target is Airflow 2; raise a clear error otherwise.

        Called once when the client is first built. Catches the common
        mispointing cases (Airflow 3, or the v1 API disabled) early instead of
        letting later tool calls fail cryptically.
        """
        try:
            info = self.get_version()
        except AirflowNotFoundError as exc:
            raise AirflowError(
                "Could not read /api/v1/version. This server supports Airflow 2 "
                "(stable REST API v1) only - the target may be Airflow 3 or have "
                "the REST API disabled."
            ) from exc
        major = info.version.split(".", 1)[0]
        if major != "2":
            raise AirflowError(
                f"This server supports Airflow 2 only, but the target reports "
                f"Airflow {info.version}."
            )
