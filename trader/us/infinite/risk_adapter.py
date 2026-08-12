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


CANONICAL_MARKET_STATES = {
    "STRONG_RISK_ON", "RISK_ON", "NORMAL", "DEFENSE_CAUTION",
    "DEFENSE_RISK_OFF", "DEFENSE_CRASH_PENDING", "DEFENSE_CRASH_CONFIRMED",
    "DEFENSE_CRASH_REBOUND",
}


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
