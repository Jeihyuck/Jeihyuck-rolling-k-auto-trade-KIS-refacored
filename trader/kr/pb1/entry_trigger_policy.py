from __future__ import annotations


def resolve_entry_trigger_policy(
    *,
    trigger_ok: bool,
    entry_ok: bool,
    setup_filters_ok: bool,
    decision_family: str | None,
) -> str:
    if trigger_ok:
        return "BREAKOUT_CONFIRMED"
    if entry_ok and setup_filters_ok:
        family = str(decision_family or "").strip().upper()
        if family.endswith("PULLBACK_OVERRIDE"):
            return "PULLBACK_OVERRIDE"
        if family.endswith("MOMENTUM_CONTINUATION"):
            return "MOMENTUM_CONTINUATION"
        if family.endswith("SCORE_OVERRIDE"):
            return "SCORE_OVERRIDE"
        return "SETUP_OVERRIDE"
    return "NONE"
