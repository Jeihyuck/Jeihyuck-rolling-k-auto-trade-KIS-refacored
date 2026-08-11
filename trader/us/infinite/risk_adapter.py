from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RiskDecision:
    allow_buy: bool
    reason: str
    market_crash: bool = False
    verified_rebound: bool = False


def assess_market_risk(overlay: dict | None) -> RiskDecision:
    """Interpret the existing overlay conservatively; unknowns fail closed."""
    overlay = overlay or {}
    state = str(overlay.get("market_state") or "").upper()
    reason = str(overlay.get("reason") or overlay.get("market_reason") or overlay.get("trade_block_reason") or "").upper()
    rebound = state == "DEFENSE_CRASH_REBOUND" or bool(overlay.get("verified_rebound"))
    combined = f"{state} {reason}"
    if any(token in combined for token in ("ACCOUNT", "INTRADAY_LOSS", "ROLLING_LOSS")):
        return RiskDecision(False, "account_crash", verified_rebound=rebound)
    if any(token in combined for token in ("DATA", "SYSTEM", "SUSPECT", "DEGRADED", "MISSING", "UNCERTAIN")):
        return RiskDecision(False, "data_or_system_risk", verified_rebound=rebound)
    crash = state in {"DEFENSE_CRASH", "CRASH", "MARKET_CRASH"} or "MARKET_CRASH" in combined
    if crash:
        return RiskDecision(True, "market_crash_limited", market_crash=True)
    if rebound:
        return RiskDecision(True, "verified_rebound", verified_rebound=True)
    if state in {"NORMAL", "RISK_ON", "GROWTH_LEADERSHIP", "NEUTRAL"} and not overlay.get("force_entry_block"):
        return RiskDecision(True, "normal")
    return RiskDecision(False, "unknown_market_risk")
