"""Runtime configuration, loaded from ``AIRFLOW_MCP_*`` environment variables."""

from __future__ import annotations

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Server settings.

    Every field maps to an ``AIRFLOW_MCP_`` env var (e.g. ``base_url`` ->
    ``AIRFLOW_MCP_BASE_URL``). The prefix avoids colliding with Airflow's own
    environment when this server runs next to an Airflow install.
    """

    model_config = SettingsConfigDict(
        env_prefix="AIRFLOW_MCP_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    base_url: str
    """Airflow base URL, e.g. ``http://localhost:8080`` (no ``/api/...`` suffix)."""

    username: str | None = None
    password: str | None = None
    api_token: str | None = None
    """Bearer token. Takes precedence over username/password when set."""

    read_only: bool = False
    """When true, write tools refuse to run regardless of which tools are registered."""

    verify_ssl: bool = True
    timeout: float = 30.0

    @model_validator(mode="after")
    def _normalize_and_validate(self) -> "Settings":
        self.base_url = self.base_url.rstrip("/")
        if not self.api_token and not (self.username and self.password):
            raise ValueError(
                "No credentials. Set AIRFLOW_MCP_API_TOKEN, or both "
                "AIRFLOW_MCP_USERNAME and AIRFLOW_MCP_PASSWORD."
            )
        return self
