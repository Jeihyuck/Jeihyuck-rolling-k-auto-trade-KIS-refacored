from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RiskDecision:
    allow_buy: bool
    reason: str
    market_crash: bool = False
    verified_rebound: bool = False
    market_state: str = ""
    market_regime: str = ""
    buy_multiplier: float = 1.0
    regime_reserve_permission: bool = False


CANONICAL_MARKET_STATES = {
    "STRONG_RISK_ON", "RISK_ON", "NORMAL", "DEFENSE_CAUTION",
    "DEFENSE_RISK_OFF", "DEFENSE_CRASH_PENDING", "DEFENSE_CRASH_CONFIRMED",
    "DEFENSE_CRASH_REBOUND",
}

TQQQ_PB1_ONLY_ENTRY_BLOCK_REASONS = frozenset({
    "sector_cap_violation_block",
    "cluster_cap_contract_failed",
    "risk_off_entry_block",
    "allow_new_buy_false",
    "final30_empty",
    "final30_below_absolute_min",
    "final30_underfilled_regime_block",
    "score_contract_failed",
    "validation_failed",
    "volume_missing_provider_entry_block",
})


def apply_tqqq_runtime_entry_override(overlay: dict | None) -> tuple[bool, str]:
    """Release only PB1-owned entry blocks for a healthy TQQQ sleeve context."""
    if not isinstance(overlay, dict):
        return False, "overlay_missing"
    if bool(overlay.get("entry_can_proceed", True)):
        return False, "entry_already_allowed"

    runtime_gate = overlay.get("tqqq_runtime_gate")
    if not isinstance(runtime_gate, dict):
        return False, "runtime_entry_block_provenance_missing"
    if (
        runtime_gate.get("entry_block_source") != "authoritative_session_prep_guard"
        or not str(runtime_gate.get("prep_run_id") or "").strip()
        or runtime_gate.get("prep_recovery_verified") is not True
    ):
        return False, "authoritative_prep_recovery_not_proven"

    block_reason = str(runtime_gate.get("entry_block_reason") or "").strip().lower()
    if block_reason not in TQQQ_PB1_ONLY_ENTRY_BLOCK_REASONS:
        return False, block_reason or "unknown_runtime_entry_block"
    if (
        not bool(overlay.get("tqqq_policy_override_allowed", False))
        or runtime_gate.get("override_authorized") is not True
    ):
        return False, "tqqq_pb1_policy_override_not_authorized"

    context_quality = str(runtime_gate.get("context_quality") or "").strip().lower()
    quote_stale = bool(runtime_gate.get("quote_stale", True) or overlay.get("tqqq_quote_stale", True))
    exit_can_proceed = bool(runtime_gate.get("exit_can_proceed", False)) and bool(
        overlay.get("exit_can_proceed", False)
    )
    hard_failure = bool(runtime_gate.get("hard_system_failure", True) or overlay.get("hard_system_failure", True))
    reconcile_block = bool(
        runtime_gate.get("reconcile_entry_block", True) or overlay.get("reconcile_entry_block", True)
    )
    if context_quality != "ok" or quote_stale or not exit_can_proceed or hard_failure or reconcile_block:
        return False, "tqqq_operational_safety_not_proven"

    overlay["entry_can_proceed"] = True
    overlay["tqqq_runtime_entry_override"] = True
    overlay["tqqq_runtime_entry_override_reason"] = block_reason
    return True, block_reason


def effective_regime(overlay: dict | None) -> tuple[str, float, bool, bool, str]:
    """Resolve labels into (regime, multiplier, reserve permission, entry, reason).

    The third value is a per-tick permission, never persistent unlock state.
    Only the policy-state machine may mutate ``InfiniteState.reserve_unlocked``.
    """
    o = overlay or {}
    apply_tqqq_runtime_entry_override(o)
    raw_state = str(o.get("market_state") or "").upper()
    raw_regime = str(o.get("market_regime") or "").upper()
    combined = {raw_state, raw_regime}
    crash = next((value for value in combined if "CRASH" in value and "REBOUND" not in value), "")
    if crash:
        name = "DEFENSE_CRASH_CONFIRMED" if "CONFIRMED" in crash else "DEFENSE_CRASH_PENDING"
        return name, 0.0, False, False, "crash_policy"
    if any("CAPITAL_PRESERVATION" in value for value in combined):
        return "CAPITAL_PRESERVATION", 0.5, False, True, "capital_preservation_sparse_evaluation"
    if any(value in {"RISK_OFF", "DEFENSIVE", "DEFENSE_RISK_OFF"} for value in combined):
        return "RISK_OFF" if any("RISK_OFF" in value for value in combined) else "DEFENSIVE", 0.5, False, True, "defensive_sparse_evaluation"
    if "CHOP_HIGH_VOL" in combined:
        return "CHOP_HIGH_VOL", 0.5, False, True, "high_volatility_chop_sparse_evaluation"
    if "DEFENSE_CRASH_REBOUND" in combined:
        return "DEFENSE_CRASH_REBOUND", 0.5, True, True, "verified_crash_rebound"
    if "DEFENSE_CAUTION" in combined:
        return "DEFENSE_CAUTION", 0.5, False, True, "defense_caution"
    if "NEUTRAL" in combined or "NORMAL" in combined:
        return "NEUTRAL", 0.75, False, True, "neutral_policy"
    if "STRONG_RISK_ON" in combined:
        return "STRONG_RISK_ON", 1.0, True, True, "strong_risk_on"
    if "RISK_ON" in combined:
        return "RISK_ON", 1.0, True, True, "risk_on"
    return "UNKNOWN", 0.0, False, False, "unknown_regime"


def assess_market_risk(overlay: dict | None) -> RiskDecision:
    """Interpret the existing overlay conservatively; unknowns fail closed."""
    overlay = overlay or {}
    if (str(overlay.get("strategy_owner") or overlay.get("owner_strategy") or overlay.get("sleeve_id") or "").upper() == "TQQQ_INFINITE"
            and (overlay.get("intraday_market_overlay") or overlay.get("intraday_rotation_overlay"))):
        return RiskDecision(True, "infinite_overlay_bypass", market_state=str(overlay.get("market_state") or ""), market_regime=str(overlay.get("market_regime") or ""))
    state = str(overlay.get("market_state") or "").upper()
    regime = str(overlay.get("market_regime") or "").upper()
    reason = " ".join(str(x) for x in (
        overlay.get("reason"), overlay.get("market_reason"), overlay.get("trade_block_reason"),
        " ".join(str(v) for v in (overlay.get("market_state_reasons") or [])),
    ) if x).upper()
    rebound = state == "DEFENSE_CRASH_REBOUND" or bool(overlay.get("verified_rebound"))
    combined = f"{state} {reason}"
    if any(token in combined for token in ("ACCOUNT", "INTRADAY_LOSS", "ROLLING_LOSS")):
        return RiskDecision(False, "account_crash", verified_rebound=rebound, market_state=state, market_regime=regime)
    if any(token in combined for token in ("DATA", "SYSTEM", "SUSPECT", "DEGRADED", "MISSING", "UNCERTAIN")):
        return RiskDecision(False, "data_or_system_risk", verified_rebound=rebound, market_state=state, market_regime=regime)
    if state not in CANONICAL_MARKET_STATES:
        return RiskDecision(False, "unknown_market_risk", market_state=state, market_regime=regime)
    crash = state in {"DEFENSE_CRASH_PENDING", "DEFENSE_CRASH_CONFIRMED"}
    if crash:
        return RiskDecision(False, "crash_pending_buy_block" if state.endswith("PENDING") else "crash_confirmed_buy_block",
                            market_crash=True, market_state=state, market_regime=regime)
    if rebound:
        return RiskDecision(True, "verified_rebound", verified_rebound=True, market_state=state, market_regime=regime)
    return RiskDecision(True, state.lower(), market_state=state, market_regime=regime)
