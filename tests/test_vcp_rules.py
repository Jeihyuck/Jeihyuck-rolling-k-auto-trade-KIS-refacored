from __future__ import annotations

import pandas as pd

from trader.minervini_v2_runner import _vcp_eval


def _base_df() -> pd.DataFrame:
    n = 80
    highs = [100.0] * n
    lows = [90.0] * n
    closes = [95.0] * n
    volumes = [1000.0] * n

    lows[-55] = 60.0
    lows[-25] = 75.0
    lows[-10] = 88.0

    for i in range(10):
        closes[-10 + i] = 95.0 + (0.2 if i % 2 else -0.2)

    for i in range(10):
        volumes[-10 + i] = 600.0

    return pd.DataFrame({"high": highs, "low": lows, "close": closes, "volume": volumes})


def test_vcp_pass_case() -> None:
    df = _base_df()
    close = float(df["close"].iloc[-1])
    _vals, reasons, vcp_pass, checks = _vcp_eval(df, close, atr_pct=0.05)
    assert vcp_pass is True
    assert reasons == []
    assert all(checks.values())


def test_vcp_fail_contraction_reason() -> None:
    df = _base_df()
    df.loc[:, "low"] = 90.0
    df.iloc[-1, df.columns.get_loc("close")] = 80.0
    close = float(df["close"].iloc[-1])
    _vals, reasons, vcp_pass, checks = _vcp_eval(df, close, atr_pct=0.05)
    assert checks["contraction_ok"] is False
    assert vcp_pass is False
    assert "vcp_no_contraction" in reasons


def test_vcp_fail_tight_reason() -> None:
    df = _base_df()
    for i in range(10):
        df.iloc[-10 + i, df.columns.get_loc("close")] = 70.0 + i * 3.0
    df.iloc[-1, df.columns.get_loc("close")] = 80.0
    close = float(df["close"].iloc[-1])
    _vals, reasons, vcp_pass, checks = _vcp_eval(df, close, atr_pct=0.09)
    assert checks["tight_ok"] is False
    assert vcp_pass is False
    assert "vcp_not_tight" in reasons


def test_vcp_fail_volume_reason() -> None:
    df = _base_df()
    for i in range(10):
        df.iloc[-10 + i, df.columns.get_loc("volume")] = 1200.0
    df.iloc[-1, df.columns.get_loc("close")] = 80.0
    close = float(df["close"].iloc[-1])
    _vals, reasons, vcp_pass, checks = _vcp_eval(df, close, atr_pct=0.05)
    assert checks["vol_ok"] is False
    assert vcp_pass is False
    assert "vcp_volume_not_shrinking" in reasons


def test_vcp_fail_near_reason() -> None:
    df = _base_df()
    df.iloc[-1, df.columns.get_loc("close")] = 80.0
    for i in range(10):
        df.iloc[-10 + i, df.columns.get_loc("volume")] = 1200.0
    close = float(df["close"].iloc[-1])
    _vals, reasons, vcp_pass, checks = _vcp_eval(df, close, atr_pct=0.05)
    assert checks["near_ok"] is False
    assert vcp_pass is False
    assert "vcp_not_near_pivot" in reasons


def test_vcp_fail_guard_reason() -> None:
    df = _base_df()
    df.iloc[-5, df.columns.get_loc("low")] = 60.0
    df.iloc[-1, df.columns.get_loc("close")] = 80.0
    close = float(df["close"].iloc[-1])
    _vals, reasons, vcp_pass, checks = _vcp_eval(df, close, atr_pct=0.05)
    assert checks["guard_ok"] is False
    assert vcp_pass is False
    assert "vcp_recent_range_too_wide" in reasons
