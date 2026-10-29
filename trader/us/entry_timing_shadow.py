"""Read-only US_STANDARD entry timing observations. No BUY authority.

PREP eligibility and live entry timing are deliberately separate. Unknown
intraday evidence means WAIT/UNKNOWN, never a fabricated successful trigger.
"""
from __future__ import annotations

from typing import Any

FAMILIES = {"ENTRY_PULLBACK", "ENTRY_MOMENTUM", "ENTRY_BREAKOUT", "ENTRY_VCP"}
STATES = {"BUY_READY", "WAIT", "INVALIDATED", "NOT_EVALUATED"}

def evaluate_entry_timing_shadow(
    *, owner: str, style: str, prep_eligible: bool,
    live_facts: dict[str, Any], enabled: bool = True,
) -> dict[str, Any]:
    family = str(style or "").upper()
    result: dict[str, Any] = {
        "mode": "SHADOW_ONLY", "owner": owner, "entry_family": family,
        "timing_state": "NOT_EVALUATED", "reason": "not_evaluated",
        "observed_facts": dict(live_facts), "may_authorize_order": False,
    }
    if not enabled:
        result["reason"] = "shadow_disabled"
        return result
    if owner != "US_STANDARD" or family not in FAMILIES:
        result["reason"] = "owner_or_family_not_supported"
        return result
    if not prep_eligible:
        result.update(timing_state="INVALIDATED", reason="prep_not_eligible")
        return result
    if live_facts.get("quote_fresh") is not True:
        result.update(timing_state="WAIT", reason="fresh_quote_not_proven")
        return result

    # These are observation candidates, not live strategy policy gates.
    # VWAP is not mandatory for Pullback; VCP honors its pre-existing live proof.
    evidence_keys = {
        "ENTRY_PULLBACK": ("support_reaction_confirmed",),
        "ENTRY_MOMENTUM": ("intraday_continuation_confirmed", "volume_confirmation"),
        "ENTRY_BREAKOUT": ("pivot_breakout_confirmed",),
        "ENTRY_VCP": ("vcp_live_breakout_verified",),
    }[family]
    observed = [live_facts.get(k) for k in evidence_keys]
    if any(v is False for v in observed):
        result.update(timing_state="WAIT", reason="intraday_trigger_not_confirmed")
    elif any(v is not True for v in observed):
        result.update(timing_state="WAIT", reason="intraday_evidence_unknown")
    else:
        result.update(timing_state="BUY_READY", reason="shadow_timing_confirmed")
    return result

def derive_quote_freshness(quote: dict | None, *, now_epoch: float, max_age_sec: float = 15.0) -> dict:
    """Require an actual timestamp and trusted broker quote provenance.

    An old PREP cached_price cannot masquerade as a live quote because its
    score, existence, or age_sec string alone is never a freshness proof.
    """
    if not isinstance(quote, dict):
        return {"quote_fresh": False, "quote_reason": "missing_live_quote"}
    if quote.get("stale") or quote.get("suspect") or quote.get("_stale_date"):
        return {"quote_fresh": False, "quote_reason": "quote_stale_or_suspect"}
    if str(quote.get("quality") or "").lower() in {"stale", "suspect", "degraded"}:
        return {"quote_fresh": False, "quote_reason": "quote_quality_invalid"}
    source = str(quote.get("source") or "").upper()
    if source not in {
        "KIS_LIVE", "KIS_CACHE", "KIS_WEBSOCKET", "KIS_WEBSOCKET_COLLECTOR",
    }:
        return {"quote_fresh": False, "quote_reason": "unverified_quote_source"}
    if source == "KIS_WEBSOCKET_COLLECTOR" and not quote.get("collector_generation"):
        # The collector generation is required to reject stale fork/cache
        # snapshots across WS reconnects; quote age alone is not enough.
        return {"quote_fresh": False, "quote_reason": "collector_generation_missing"}
    try:
        ts = float(quote.get("asof_epoch"))
        last = float(quote.get("last"))
        age = float(now_epoch) - ts
    except (TypeError, ValueError, OverflowError):
        return {"quote_fresh": False, "quote_reason": "quote_timestamp_or_price_missing"}
    import math
    if not all(math.isfinite(x) for x in (ts, last, age)) or last <= 0:
        return {"quote_fresh": False, "quote_reason": "quote_timestamp_or_price_invalid"}
    if age < -2.0 or age > max_age_sec:
        return {"quote_fresh": False, "quote_reason": "quote_age_out_of_bounds"}
    return {"quote_fresh": True, "quote_reason": "broker_timestamp_verified", "age_sec": round(age, 3)}
