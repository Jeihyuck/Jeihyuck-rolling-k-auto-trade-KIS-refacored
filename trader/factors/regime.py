from __future__ import annotations

import pandas as pd


def get_regime(index_prices: pd.Series, ma_fast: int, ma_slow: int, breadth_series: pd.Series | None = None) -> dict:
    if index_prices is None or len(index_prices) < ma_slow:
        return {"regime": "UNKNOWN", "ma_fast": None, "ma_slow": None}
    ma_fast_val = float(index_prices.rolling(ma_fast).mean().iloc[-1])
    ma_slow_val = float(index_prices.rolling(ma_slow).mean().iloc[-1])
    close = float(index_prices.iloc[-1])
    if close > ma_fast_val and ma_fast_val > ma_slow_val:
        regime = "GOOD"
    elif close < ma_slow_val:
        regime = "RISK_OFF"
    else:
        regime = "NEUTRAL"
    return {
        "regime": regime,
        "ma_fast": ma_fast_val,
        "ma_slow": ma_slow_val,
        "close": close,
    }


def risk_multiplier(regime_dict: dict, mode: str, *, max_risk: float, mid_risk: float, min_risk: float) -> float:
    regime = (regime_dict.get("regime") or "UNKNOWN").upper()
    mode = (mode or "STRICT").upper()
    if regime == "GOOD":
        return float(max_risk)
    if regime == "NEUTRAL":
        return float(mid_risk)
    if regime == "RISK_OFF":
        return 0.0 if mode == "STRICT" else float(min_risk)
    return float(min_risk)
