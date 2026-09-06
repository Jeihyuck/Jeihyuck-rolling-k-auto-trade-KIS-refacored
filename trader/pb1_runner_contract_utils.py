from __future__ import annotations

from typing import Any

from trader.constants import REQUIRED_FINAL30_SCORED_COLS


def missing_scored_cols(columns: list[str]) -> list[str]:
    cols = {str(c) for c in (columns or [])}
    missing = [c for c in REQUIRED_FINAL30_SCORED_COLS if c not in cols]
    for primary, alternative in (("close", "last_close"),):
        if primary in missing and alternative in cols:
            missing.remove(primary)
    return missing


def safe_flow_optional_missing(columns: list[str], *, flow_optional_cols: list[str] | None = None) -> list[str]:
    try:
        return [col for col in (flow_optional_cols or []) if col not in set(columns or [])]
    except Exception:
        return []
