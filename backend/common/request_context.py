"""Request context helpers for API logs and downstream operations."""

from __future__ import annotations

import contextvars
import uuid


_request_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "request_id",
    default=None,
)


def new_request_id() -> str:
    return str(uuid.uuid4())


def set_request_id(value: str | None) -> contextvars.Token[str | None]:
    return _request_id.set(value)


def reset_request_id(token: contextvars.Token[str | None]) -> None:
    _request_id.reset(token)


def current_request_id() -> str | None:
    return _request_id.get()
