from __future__ import annotations

import numpy as np
import pandas as pd


def liquidity_filter(ohlcv_df: pd.DataFrame, min_avg_value_krw: float) -> bool:
    if ohlcv_df is None or ohlcv_df.empty:
        return False
    close = ohlcv_df["close"].fillna(0)
    volume = ohlcv_df["volume"].fillna(0)
    value = (close * volume).rolling(20).mean().iloc[-1]
    return bool(np.isfinite(value) and value >= min_avg_value_krw)


def gap_filter(ohlcv_df: pd.DataFrame, max_gap_up_pct: float) -> bool:
    if ohlcv_df is None or len(ohlcv_df) < 2:
        return False
    prev_close = float(ohlcv_df["close"].iloc[-2])
    open_price = float(ohlcv_df["open"].iloc[-1])
    if prev_close <= 0:
        return False
    gap_pct = (open_price - prev_close) / prev_close * 100.0
    return gap_pct <= max_gap_up_pct


def spread_proxy_filter(ohlcv_df: pd.DataFrame, max_bps: float) -> bool:
    if ohlcv_df is None or ohlcv_df.empty:
        return False
    close = ohlcv_df["close"]
    spread_proxy = (ohlcv_df["high"] - ohlcv_df["low"]) / close.replace(0, np.nan) * 10000.0
    avg_bps = float(spread_proxy.tail(20).mean())
    return bool(np.isfinite(avg_bps) and avg_bps <= max_bps)


def range_filter(ohlcv_df: pd.DataFrame, max_intraday_range_pct: float) -> bool:
    if ohlcv_df is None or ohlcv_df.empty:
        return False
    range_pct = (ohlcv_df["high"] - ohlcv_df["low"]) / ohlcv_df["close"] * 100.0
    avg_range = float(range_pct.tail(20).mean())
    return bool(np.isfinite(avg_range) and avg_range <= max_intraday_range_pct)
