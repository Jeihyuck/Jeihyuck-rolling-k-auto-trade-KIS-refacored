KNOWN_STATES = frozenset({
    "KR_STRONG_RISK_ON", "KR_RISK_ON", "KR_NORMAL", "KR_DEFENSE_CAUTION",
    "KR_DEFENSE_RISK_OFF", "KR_DEFENSE_CRASH", "KR_SHOCK_REBOUND_PENDING",
    "KR_SHOCK_REBOUND_CONFIRMED",
})


def buy_pause_reason(state: str | None, data_quality: str = "OK") -> str | None:
    if not state or str(data_quality).upper() == "BLOCKED":
        return "MISSING_KR_MARKET_REGIME_BUY_PAUSED"
    if state not in KNOWN_STATES:
        return "KR_INF_UNKNOWN_REGIME_BUY_PAUSED"
    return None


def allows_new_cycle(state: str | None) -> bool:
    return state in {"KR_STRONG_RISK_ON", "KR_RISK_ON", "KR_NORMAL", "KR_SHOCK_REBOUND_CONFIRMED"}


def permits_dedicated_new_cycle(state: str | None, *, symbol: str = "122630") -> bool:
    """Align COMPLETE->new-cycle scope with the KR_INFINITE owner BUY policy.

    The 122630 strategy already allows a first BUY in CAUTION/RISK_OFF;
    an old COMPLETE cycle must NEVER be reused for that first BUY.
    CRASH and pending shock-rebound states remain entry-blocked.
    """
    if symbol == "122630":
        return state in {
            "KR_DEFENSE_CAUTION", "KR_DEFENSE_RISK_OFF", "KR_NORMAL",
            "KR_RISK_ON", "KR_STRONG_RISK_ON", "KR_SHOCK_REBOUND_CONFIRMED",
        }
    return allows_new_cycle(state)
