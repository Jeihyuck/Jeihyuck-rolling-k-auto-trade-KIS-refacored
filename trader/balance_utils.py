from __future__ import annotations

from typing import Any


def sanitize_balance_snapshot(snapshot: dict | None) -> dict:
    return _sanitize_value(snapshot or {})


def _sanitize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _sanitize_value(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_sanitize_value(item) for item in value]
    if value is None:
        return None
    return "****"
