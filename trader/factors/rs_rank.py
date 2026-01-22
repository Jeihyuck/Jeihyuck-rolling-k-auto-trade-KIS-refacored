from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def compute_rs(prices_stock: pd.Series, prices_bench: pd.Series, lookback_days: int) -> float:
    if prices_stock is None or prices_bench is None:
        return float("nan")
    if len(prices_stock) < lookback_days + 1 or len(prices_bench) < lookback_days + 1:
        return float("nan")
    stock_ret = float(prices_stock.iloc[-1] / prices_stock.iloc[-(lookback_days + 1)] - 1.0)
    bench_ret = float(prices_bench.iloc[-1] / prices_bench.iloc[-(lookback_days + 1)] - 1.0)
    return stock_ret - bench_ret


def compute_rs_composite(rs_3m: float, rs_6m: float, w1: float, w2: float) -> float:
    if not np.isfinite(rs_3m) and not np.isfinite(rs_6m):
        return float("nan")
    rs_3m = rs_3m if np.isfinite(rs_3m) else 0.0
    rs_6m = rs_6m if np.isfinite(rs_6m) else 0.0
    return float(w1 * rs_3m + w2 * rs_6m)


def rank_rs(
    universe_prices: dict[str, pd.Series],
    bench_prices: pd.Series,
    lookback_days: int = 63,
    lookback2_days: int = 126,
    w1: float = 0.6,
    w2: float = 0.4,
) -> pd.DataFrame:
    min_len = max(lookback_days, lookback2_days) + 1
    if bench_prices is None or len(bench_prices) < min_len:
        logger.warning(
            "[RS][RANK][SKIP] bench_missing=1 len=%s required=%s",
            len(bench_prices) if bench_prices is not None else 0,
            min_len,
        )
        rows = [
            {"ticker": ticker, "rs": float("nan"), "rs6m": float("nan"), "composite": 0.0, "pctile": 0.0}
            for ticker in universe_prices
        ]
        return pd.DataFrame(rows)
    rows: list[dict] = []
    for ticker, series in universe_prices.items():
        rs_3m = compute_rs(series, bench_prices, lookback_days)
        rs_6m = compute_rs(series, bench_prices, lookback2_days)
        composite = compute_rs_composite(rs_3m, rs_6m, w1, w2)
        rows.append({"ticker": ticker, "rs": rs_3m, "rs6m": rs_6m, "composite": composite})

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["pctile"] = df["composite"].rank(pct=True, method="min")
    return df.sort_values("pctile", ascending=False).reset_index(drop=True)
