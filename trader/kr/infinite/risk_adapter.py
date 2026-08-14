VALID_STATES=frozenset({"KR_STRONG_RISK_ON","KR_RISK_ON","KR_NORMAL","KR_DEFENSE_CAUTION","KR_DEFENSE_RISK_OFF","KR_DEFENSE_CRASH","KR_SHOCK_REBOUND_PENDING","KR_SHOCK_REBOUND_CONFIRMED"})
def validate_regime(state: str, data_quality: str="OK") -> str:
    if state not in VALID_STATES or data_quality == "BLOCKED": raise ValueError("KR_INF_REGIME_DATA_BLOCKED")
    return state
def allows_new_cycle(state: str) -> bool:
    return state in {"KR_STRONG_RISK_ON","KR_RISK_ON","KR_NORMAL","KR_SHOCK_REBOUND_CONFIRMED"}
