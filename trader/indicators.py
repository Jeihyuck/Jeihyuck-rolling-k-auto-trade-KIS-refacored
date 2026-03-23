from __future__ import annotations

from typing import Any

import pandas as pd


def safe_nullable_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        if isinstance(value, str):
            value = value.strip()
            if value == "":
                return None
        out = float(value)
        if pd.isna(out) or out in (float("inf"), float("-inf")):
            return None
        return out
    except Exception:
        return None


def compute_ma20_from_ohlcv(df: pd.DataFrame) -> float | None:
    if df is None or df.empty or "close" not in df.columns or len(df) < 20:
        return None
    series = pd.to_numeric(df["close"], errors="coerce")
    value = safe_nullable_float(series.rolling(20).mean().iloc[-1])
    if value is None or value <= 0:
        return None
    return value


def compute_atr_pct_from_ohlcv(df: pd.DataFrame, period: int = 14) -> float | None:
    if df is None or len(df) < period + 1:
        return None
    required = {"high", "low", "close"}
    if not required.issubset(df.columns):
        return None

    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")
    prev_close = close.shift(1)

    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = safe_nullable_float(tr.rolling(period).mean().iloc[-1])
    last_close = safe_nullable_float(close.iloc[-1])
    if atr is None or last_close is None or last_close <= 0:
        return None
    atr_pct = atr / last_close
    if atr_pct <= 0:
        return None
    return atr_pct