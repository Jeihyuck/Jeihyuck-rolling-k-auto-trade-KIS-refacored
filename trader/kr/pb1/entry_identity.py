from __future__ import annotations

from typing import Any

from trader.kr.pb1.exit_family import normalize_entry_reason, resolve_exit_family


def resolve_entry_identity_from_mapping(source: dict[str, Any] | None) -> dict[str, str]:
    payload = source if isinstance(source, dict) else {}
    entry_reason = normalize_entry_reason(
        payload.get("entry_reason")
        or payload.get("entry_style_selected")
        or payload.get("entry_signal")
    )
    raw_style = payload.get("entry_style_selected") or payload.get("entry_signal") or entry_reason
    entry_style_selected = normalize_entry_reason(raw_style)
    if entry_style_selected == "ENTRY_GENERIC":
        entry_style_selected = entry_reason
    entry_decision_family = str(payload.get("entry_decision_family") or entry_reason).strip().upper() or entry_reason
    _, exit_policy_family = resolve_exit_family(entry_reason, entry_style_selected)
    return {
        "entry_reason": entry_reason,
        "entry_style_selected": entry_style_selected,
        "entry_decision_family": entry_decision_family,
        "exit_policy_family": exit_policy_family,
    }
