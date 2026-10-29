"""US PB1 entry identity vs observed trigger evidence, without policy changes.

The selected style is authoritative for execution. Score maxima are diagnostic
only; a lower score for the selected style does NOT mean a strategy bug.
"""
from __future__ import annotations

import math
from typing import Any

_STYLE_SCORES = {
    "ENTRY_PULLBACK": "pullback_score",
    "ENTRY_MOMENTUM": "momentum_score",
    "ENTRY_BREAKOUT": "breakout_score",
    "ENTRY_VCP": "vcp_score",
}


def _score(value: Any) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def build_entry_identity_proof(
    entry_meta: dict | None, *, intent: dict, observed_price: Any = None,
) -> dict:
    """Preserve who chose entry and what *actual* evidence was available.

    No imagined trade-volume, RVOL or trigger timestamp is synthesized.
    Source data may be genuinely unknown; this function never authorizes BUY.
    """
    from trader.us.pb1.us_explain import normalize_us_entry_style

    meta = entry_meta if isinstance(entry_meta, dict) else {}
    raw = str(meta.get("entry_style_raw") or meta.get("entry_style_selected")
              or meta.get("entry_style") or intent.get("entry_style_raw") or "")
    selected = normalize_us_entry_style(
        meta.get("entry_style_selected") or meta.get("entry_style")
        or intent.get("entry_style_selected") or intent.get("entry_style")
    )
    scores = {key: _score(meta.get(key)) for key in _STYLE_SCORES.values()}
    present = {key: value for key, value in scores.items() if value is not None}
    max_score = max(present.values()) if present else None
    selected_key = _STYLE_SCORES.get(selected)
    chosen_score = scores.get(selected_key) if selected_key else None
    return {
        "schema_version": 1,
        "strategy_owner": str(intent.get("strategy_owner") or "UNVERIFIED"),
        "entry_strategy": str(intent.get("entry_strategy") or "UNVERIFIED"),
        "selected_style": selected,
        "raw_style": raw,
        "entry_signal_type": str(intent.get("entry_signal_type") or ""),
        "observed_trigger": meta.get("observed_trigger") or meta.get("trigger_event"),
        "trigger_timestamp": meta.get("trigger_timestamp") or meta.get("signal_timestamp"),
        "trigger_source": meta.get("trigger_source"),
        "entry_signal_proof_source": meta.get("entry_signal_proof_source"),
        "independent_entry_contract_v1": meta.get("independent_entry_contract_v1"),
        "evidence_status": meta.get("independent_proof_status") or "NOT_ASSERTED",
        "selected_score": chosen_score,
        "max_component_score": max_score,
        "selected_below_score_max": bool(chosen_score is not None
                                         and max_score is not None
                                         and chosen_score < max_score),
        "component_scores": scores,
        "observed_price": _score(observed_price),
        "quote_source": meta.get("quote_source"),
        "quote_timestamp": meta.get("quote_timestamp"),
        "volume_proof_source": meta.get("volume_proof_source"),
        "volume_proof_status": meta.get("volume_proof_status") or "UNVERIFIED",
        "trade_date": intent.get("trade_date"),
        "position_action": intent.get("position_action"),
    }
