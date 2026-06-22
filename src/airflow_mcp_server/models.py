"""Response models.

Two flavors:
- Diagnostic models (version, health) use ``extra="allow"`` to tolerate Airflow's
  optional/changing fields.
- List/detail models are curated: only agent-useful fields are declared, and
  Pydantic's default (drop-extra) trims the rest to keep token cost down. A few
  field validators flatten Airflow's awkward shapes (tags, schedule).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator

# --------------------------------------------------------------------------- #
# Diagnostics
# --------------------------------------------------------------------------- #


class VersionInfo(BaseModel):
    model_config = ConfigDict(extra="allow")

    version: str
    git_version: str | None = None


class ComponentHealth(BaseModel):
    model_config = ConfigDict(extra="allow")

    status: str | None = None


class HealthStatus(BaseModel):
    model_config = ConfigDict(extra="allow")

    metadatabase: ComponentHealth | None = None
    scheduler: ComponentHealth | None = None
    triggerer: ComponentHealth | None = None
    dag_processor: ComponentHealth | None = None


# --------------------------------------------------------------------------- #
# DAGs
# --------------------------------------------------------------------------- #


class DagSummary(BaseModel):
    dag_id: str
    is_paused: bool | None = None
    is_active: bool | None = None
    description: str | None = None
    schedule_interval: str | None = None
    timetable_summary: str | None = None
    owners: list[str] = []
    tags: list[str] = []
    next_dagrun: str | None = None
    fileloc: str | None = None

    @field_validator("tags", mode="before")
    @classmethod
    def _flatten_tags(cls, value: Any) -> Any:
        # Airflow returns tags as [{"name": "x"}, ...]; we want ["x", ...].
        if isinstance(value, list):
            return [t.get("name") if isinstance(t, dict) else t for t in value]
        return value

    @field_validator("schedule_interval", mode="before")
    @classmethod
    def _flatten_schedule(cls, value: Any) -> Any:
        # schedule_interval is an object like {"__type": ..., "value": "0 0 * * *"}.
        if isinstance(value, dict):
            return value.get("value")
        return value


class DagList(BaseModel):
    dags: list[DagSummary] = []
    total_entries: int = 0


# --------------------------------------------------------------------------- #
# DAG runs
# --------------------------------------------------------------------------- #


class DagRun(BaseModel):
    dag_run_id: str
    dag_id: str | None = None
    state: str | None = None
    run_type: str | None = None
    logical_date: str | None = None
    execution_date: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    external_trigger: bool | None = None
    note: str | None = None


class DagRunList(BaseModel):
    dag_runs: list[DagRun] = []
    total_entries: int = 0


# --------------------------------------------------------------------------- #
# Task instances
# --------------------------------------------------------------------------- #


class TaskInstance(BaseModel):
    task_id: str
    dag_id: str | None = None
    dag_run_id: str | None = None
    state: str | None = None
    try_number: int | None = None
    map_index: int | None = None
    start_date: str | None = None
    end_date: str | None = None
    duration: float | None = None
    operator: str | None = None
    note: str | None = None


class TaskInstanceList(BaseModel):
    task_instances: list[TaskInstance] = []
    total_entries: int = 0


# --------------------------------------------------------------------------- #
# Import errors & pools
# --------------------------------------------------------------------------- #


class DagImportError(BaseModel):
    import_error_id: int | None = None
    filename: str | None = None
    stack_trace: str | None = None


class ImportErrorList(BaseModel):
    import_errors: list[DagImportError] = []
    total_entries: int = 0


class Pool(BaseModel):
    name: str | None = None
    slots: int | None = None
    occupied_slots: int | None = None
    running_slots: int | None = None
    queued_slots: int | None = None
    open_slots: int | None = None


class PoolList(BaseModel):
    pools: list[Pool] = []
    total_entries: int = 0


# --------------------------------------------------------------------------- #
# Variables
# --------------------------------------------------------------------------- #


class Variable(BaseModel):
    key: str
    value: str | None = None
    description: str | None = None


class VariableList(BaseModel):
    variables: list[Variable] = []
    total_entries: int = 0
