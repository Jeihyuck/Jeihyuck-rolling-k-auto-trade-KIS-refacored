from __future__ import annotations

from typing import Any


def resolve_exit_family(entry_reason: Any, entry_style_selected: Any) -> tuple[str, str]:
    normalized_reason = str(entry_reason or entry_style_selected or "").strip().upper()
    if normalized_reason == "ENTRY_BREAKOUT":
        return normalized_reason, "BREAKOUT_EXIT"
    if normalized_reason == "ENTRY_PULLBACK":
        return normalized_reason, "PULLBACK_EXIT"
    if normalized_reason == "ENTRY_MOMENTUM":
        return normalized_reason, "MOMENTUM_EXIT"
    return normalized_reason, "GENERIC_EXIT"
