from __future__ import annotations

from trader.kr.pb1.exit_family import normalize_entry_reason


def resolve_entry_setup_family(*, entry_style_selected, entry_signal, breakout_score: float, pullback_score: float, momentum_score: float) -> str:
    selected_family = normalize_entry_reason(entry_style_selected or entry_signal)
    score_lookup = {
        "ENTRY_BREAKOUT": float(breakout_score or 0.0),
        "ENTRY_PULLBACK": float(pullback_score or 0.0),
        "ENTRY_MOMENTUM": float(momentum_score or 0.0),
    }
    if selected_family in score_lookup and score_lookup[selected_family] > 0:
        return selected_family
    strongest_family = max(score_lookup.items(), key=lambda item: item[1])[0]
    if score_lookup[strongest_family] > 0:
        return strongest_family
    return "ENTRY_GENERIC"


def resolve_entry_decision_family(
    *,
    entry_reason: str,
    setup_filters_ok: bool,
    breakout_trigger_ok: bool,
    trigger_reason=None,
) -> str:
    normalized_reason = normalize_entry_reason(entry_reason)
    trigger_reason_s = str(trigger_reason or "").strip().lower()
    if breakout_trigger_ok:
        return "ENTRY_BREAKOUT_CONFIRMED"
    if normalized_reason == "ENTRY_PULLBACK" and setup_filters_ok:
        return "ENTRY_PULLBACK_OVERRIDE"
    if normalized_reason == "ENTRY_MOMENTUM" and setup_filters_ok:
        return "ENTRY_MOMENTUM_CONTINUATION"
    if setup_filters_ok:
        if "score" in trigger_reason_s:
            return "ENTRY_SCORE_OVERRIDE"
        return "ENTRY_SETUP_OVERRIDE"
    if "score" in trigger_reason_s:
        return "ENTRY_SCORE_OVERRIDE"
    return "ENTRY_SETUP_OVERRIDE"
