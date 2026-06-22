"""Exceptions raised by the Airflow client, mapped from HTTP responses."""

from __future__ import annotations


class AirflowError(Exception):
    """Base class for all Airflow client errors."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class AirflowRequestError(AirflowError):
    """The request never got a response (DNS, connection, timeout, TLS)."""


class AirflowAuthError(AirflowError):
    """Authentication or authorization failed (HTTP 401/403)."""


class AirflowNotFoundError(AirflowError):
    """The requested resource does not exist (HTTP 404)."""


class AirflowAPIError(AirflowError):
    """The API returned an unexpected non-2xx status."""
