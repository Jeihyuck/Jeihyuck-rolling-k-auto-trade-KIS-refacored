from __future__ import annotations

from typing import Any


def _to_float_safe(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def normalize_entry_style_value(value: Any) -> str:
    raw = str(value or "").strip().upper()
    aliases = {
        "BREAK": "BREAKOUT",
        "BO": "BREAKOUT",
        "ENTRY_BREAKOUT": "BREAKOUT",
        "PULL": "PULLBACK",
        "PB": "PULLBACK",
        "ENTRY_PULLBACK": "PULLBACK",
        "MOMO": "MOMENTUM",
        "MOM": "MOMENTUM",
        "ENTRY_MOMENTUM": "MOMENTUM",
        "MOMENTUM_CONTINUATION": "MOMENTUM",
    }
    return aliases.get(raw, raw)


def infer_entry_style_from_scores(row: dict[str, Any]) -> str:
    breakout = _to_float_safe(row.get("breakout_score"))
    pullback = _to_float_safe(row.get("pullback_score"))
    momentum = _to_float_safe(row.get("momentum_score"))
    scores = {"BREAKOUT": breakout, "PULLBACK": pullback, "MOMENTUM": momentum}
    if max(scores.values()) <= 0:
        return "MOMENTUM"
    priority = {"MOMENTUM": 3, "PULLBACK": 2, "BREAKOUT": 1}
    return sorted(scores.items(), key=lambda kv: (kv[1], priority[kv[0]]), reverse=True)[0][0]
