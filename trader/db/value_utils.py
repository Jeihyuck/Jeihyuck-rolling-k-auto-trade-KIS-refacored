from __future__ import annotations

from typing import Any

from trader.db.json_safe import json_sanitize
from trader.indicators import safe_nullable_float


def merge_json_dict(base: Any, incoming: Any) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    if isinstance(base, dict):
        merged.update(base)
    if isinstance(incoming, dict):
        merged.update(incoming)
    return json_sanitize(merged)


def safe_float_or_none(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def restore_numeric_from_sources(*sources: Any, aliases: tuple[str, ...]) -> float | None:
    for source in sources:
        if not isinstance(source, dict):
            continue
        for alias in aliases:
            if alias not in source:
                continue
            numeric = safe_nullable_float(source.get(alias))
            if numeric is not None:
                return float(numeric)
    return None
