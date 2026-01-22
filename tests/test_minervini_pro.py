from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

import numpy as np

from trader.factors.liquidity_risk import liquidity_filter
from trader.factors.regime import get_regime, risk_multiplier
from trader.factors.rs_rank import compute_rs_composite, rank_rs
from trader.positioning.minervini_risk import calc_initial_stop, calc_position_size, update_exits
from trader.setups.vcp_pro import PriceTightRules, VolContractRules, score_vcp


def _make_series(start: float, days: int, step: float = 1.0) -> pd.Series:
    return pd.Series([start + i * step for i in range(days)])


def _make_df(days: int) -> pd.DataFrame:
    start = datetime(2024, 1, 1)
    dates = [start + timedelta(days=i) for i in range(days)]
    close = [100 + i * 0.5 for i in range(days)]
    high = [c + 1 for c in close]
    low = [c - 1 for c in close]
    volume = [1000] * days
    return pd.DataFrame({"date": dates, "open": close, "high": high, "low": low, "close": close, "volume": volume})


def test_rs_composite_weights() -> None:
    score = compute_rs_composite(0.1, 0.2, 0.6, 0.4)
    assert abs(score - 0.14) < 1e-6


def test_rs_rank_pctile() -> None:
    bench = _make_series(100, 200, 1.0)
    universe = {"AAA": _make_series(100, 200, 1.2), "BBB": _make_series(100, 200, 0.8)}
    ranked = rank_rs(universe, bench, lookback_days=63, lookback2_days=126)
    assert ranked.iloc[0]["ticker"] == "AAA"


def test_regime_risk_multiplier() -> None:
    series = _make_series(100, 220, 1.0)
    regime = get_regime(series, 50, 200)
    risk = risk_multiplier(regime, "STRICT", max_risk=1.0, mid_risk=0.6, min_risk=0.0)
    assert risk == 1.0


def test_vcp_score_positive() -> None:
    df = _make_df(120)
    score = score_vcp(df, 120, VolContractRules(), PriceTightRules())
    assert score >= 0


def test_calc_initial_stop_uses_tightlow() -> None:
    stop = calc_initial_stop(100.0, 95.0, 2.0, "TIGHTLOW", entry=100.0, atr_mult=2.5)
    assert stop <= 95.0


def test_calc_position_size_risk() -> None:
    qty = calc_position_size(1000000.0, 0.5, 100.0, 95.0, 1.0)
    assert qty == 1000


def test_rs_rank_handles_missing_bench() -> None:
    bench = pd.Series([], dtype=float)
    universe = {"AAA": _make_series(100, 200, 1.2)}
    ranked = rank_rs(universe, bench, lookback_days=63, lookback2_days=126)
    assert ranked.iloc[0]["pctile"] == 0.0


def test_liquidity_filter_tolerates_nan_volume() -> None:
    df = _make_df(20)
    df.loc[df.index[-1], "volume"] = np.nan
    assert liquidity_filter(df, min_avg_value_krw=1e7)


def test_update_exits_triggers_tp1() -> None:
    state = {"entry_price": 100.0, "stop_price": 95.0, "r_value": 5.0, "qty": 100, "tp1_done": False}
    orders = update_exits(
        state,
        last_price=110.0,
        ma20=100.0,
        atr=2.0,
        take_profit_r1=2.0,
        take_profit_r2=3.0,
        tp1_pct=0.33,
        tp2_pct=0.33,
        trail_mode="MA20",
        trail_step_after_r=1.5,
        failed_breakout_days=2,
        failed_breakout=False,
    )
    assert any(order.reason == "TP1" for order in orders)
