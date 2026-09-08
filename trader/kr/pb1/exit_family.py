from __future__ import annotations

from typing import Any


def normalize_entry_reason(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"ENTRY_BREAKOUT", "BREAKOUT", "ENTRY_BREAKOUT_CONFIRMED"}:
        return "ENTRY_BREAKOUT"
    if raw in {"ENTRY_PULLBACK", "PULLBACK", "ENTRY_PULLBACK_OVERRIDE"}:
        return "ENTRY_PULLBACK"
    if raw in {"ENTRY_MOMENTUM", "MOMENTUM", "ENTRY_MOMENTUM_CONTINUATION"}:
        return "ENTRY_MOMENTUM"
    return "ENTRY_GENERIC"


def resolve_exit_family(entry_reason: Any, entry_style_selected: Any) -> tuple[str, str]:
    normalized_reason = normalize_entry_reason(entry_reason or entry_style_selected)
    if normalized_reason == "ENTRY_BREAKOUT":
        return normalized_reason, "BREAKOUT_EXIT"
    if normalized_reason == "ENTRY_PULLBACK":
        return normalized_reason, "PULLBACK_EXIT"
    if normalized_reason == "ENTRY_MOMENTUM":
        return normalized_reason, "MOMENTUM_EXIT"
    return normalized_reason, "GENERIC_EXIT"
