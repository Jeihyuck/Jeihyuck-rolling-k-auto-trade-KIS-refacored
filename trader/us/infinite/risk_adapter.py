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


def effective_regime(overlay: dict | None) -> tuple[str, float, bool, bool, str]:
    """Resolve labels into (regime, multiplier, reserve permission, entry, reason).

    The third value is a per-tick permission, never persistent unlock state.
    Only the policy-state machine may mutate ``InfiniteState.reserve_unlocked``.
    """
    o = overlay or {}
    raw_state = str(o.get("market_state") or "").upper()
    raw_regime = str(o.get("market_regime") or "").upper()
    combined = {raw_state, raw_regime}
    if any("CAPITAL_PRESERVATION" in value for value in combined):
        return "CAPITAL_PRESERVATION", 0.0, False, False, "capital_preservation"
    if "DEFENSE_CRASH_REBOUND" in combined:
        return "RISK_ON", 0.5, True, True, "verified_crash_rebound"
    if any("CRASH" in value for value in combined):
        name = "DEFENSE_CRASH" if any("DEFENSE" in value for value in combined) else "CRASH"
        return name, 0.0, False, False, "crash_policy"
    if "CHOP_HIGH_VOL" in combined:
        return "CHOP_HIGH_VOL", 0.0, False, False, "high_volatility_chop"
    if any(value in {"RISK_OFF", "DEFENSIVE", "DEFENSE_RISK_OFF"} for value in combined):
        return "RISK_OFF" if "RISK_OFF" in combined else "DEFENSIVE", 0.0, False, False, "defensive_policy"
    if "NEUTRAL" in combined or "NORMAL" in combined:
        return "NEUTRAL", 0.75, False, True, "neutral_policy"
    if "STRONG_RISK_ON" in combined:
        return "STRONG_RISK_ON", 1.25, True, True, "strong_risk_on"
    if "RISK_ON" in combined:
        return "RISK_ON", 1.0, True, True, "risk_on"
    return "UNKNOWN", 0.0, False, False, "unknown_regime"


def assess_market_risk(overlay: dict | None) -> RiskDecision:
    """Interpret the existing overlay conservatively; unknowns fail closed."""
    overlay = overlay or {}
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
