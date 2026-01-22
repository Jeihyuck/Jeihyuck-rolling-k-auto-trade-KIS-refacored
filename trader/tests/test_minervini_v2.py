from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from trader.strategies.pb1_minervini_v2 import (
    MinerviniConfig,
    compute_pivot,
    detect_vcp,
    entry_trigger,
    initial_stop,
    risk_position_size,
    update_trailing_stop,
)


def _make_df(
    *,
    days: int,
    base_price: float = 100.0,
    ranges: list[float] | None = None,
    volumes: list[float] | None = None,
    tight_close: bool = False,
) -> pd.DataFrame:
    start = datetime(2024, 1, 1)
    dates = [start + timedelta(days=i) for i in range(days)]
    if ranges is None:
        ranges = [2.0] * days
    if volumes is None:
        volumes = [1000.0] * days
    closes = []
    for i in range(days):
        if tight_close and i >= days - 5:
            closes.append(base_price * (1 + 0.002 * ((i - days) % 2)))
        else:
            closes.append(base_price + i * 0.1)
    highs = [c + r for c, r in zip(closes, ranges)]
    lows = [c - r for c, r in zip(closes, ranges)]
    return pd.DataFrame(
        {
            "date": dates,
            "open": closes,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
        }
    )


def test_detect_vcp_true_with_contractions_and_dryup() -> None:
    cfg = MinerviniConfig(base_lookback_max=40)
    ranges = [4.0] * 10 + [3.0] * 10 + [2.5] * 10 + [2.0] * 10
    volumes = [1200.0] * 30 + [600.0] * 10
    df = _make_df(days=40, ranges=ranges, volumes=volumes, tight_close=True)
    info = detect_vcp(df, cfg)
    assert info["vcp_ok"] is True


def test_compute_pivot_excludes_recent_window() -> None:
    cfg = MinerviniConfig(base_lookback_max=40, base_exclude_recent=10)
    df = _make_df(days=40)
    df.loc[df.index[35:], "high"] = 999.0
    df.loc[df.index[20], "high"] = 200.0
    pivot, meta = compute_pivot(df, cfg)
    assert pivot == 200.0
    assert meta["pivot_age"] >= 0


def test_entry_trigger_respects_volume_and_chase_rules() -> None:
    cfg = MinerviniConfig()
    feats = {"pivot": 100.0, "vol20": 1000.0, "vcp_ok": True}
    ok, _info = entry_trigger(feats, last_price=101.0, last_volume=1600.0, cfg=cfg)
    assert ok is True
    ok, _info = entry_trigger(feats, last_price=110.0, last_volume=1600.0, cfg=cfg)
    assert ok is False


def test_initial_stop_below_entry() -> None:
    cfg = MinerviniConfig()
    df = _make_df(days=20, ranges=[2.0] * 20)
    feats = {"low_10": 90.0, "atr14": 2.0}
    stop = initial_stop(100.0, df, feats, cfg)
    assert stop < 100.0


def test_update_trailing_stop_moves_to_breakeven() -> None:
    cfg = MinerviniConfig()
    df = _make_df(days=60)
    feats = {"initial_stop": 92.0, "stop_price": 92.0, "ma50": 98.0, "low_10": 97.0}
    stop, reasons = update_trailing_stop(100.0, 108.0, df, feats, cfg)
    assert stop >= 99.5
    assert "breakeven" in reasons


def test_risk_position_size_caps_loss() -> None:
    qty = risk_position_size(
        entry_price=100.0,
        stop_price=92.0,
        risk_krw=800.0,
        max_capital_krw=20000.0,
        min_order_krw=0.0,
    )
    assert qty == 100
