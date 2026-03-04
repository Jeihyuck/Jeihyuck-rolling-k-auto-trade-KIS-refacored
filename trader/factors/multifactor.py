from __future__ import annotations

from typing import Any
import importlib

import numpy as np
import pandas as pd


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, str):
            s = v.replace(",", "").strip()
            if not s:
                return default
            return float(s)
        return float(v)
    except Exception:
        return default


def _pct_rank(values: list[float]) -> list[float]:
    if not values:
        return []
    arr = np.array(values, dtype=float)
    order = np.argsort(arr)
    ranks = np.empty(len(arr), dtype=float)
    denom = max(1, len(arr) - 1)
    for i, idx in enumerate(order):
        ranks[idx] = (i / denom) * 100.0
    return ranks.tolist()


def compute_rs_features(stock_close: pd.Series, bench_close: pd.Series | None) -> dict[str, float]:
    if stock_close is None or len(stock_close) < 253:
        return {"rs63": 0.0, "rs126": 0.0, "rs252": 0.0, "rs_score": 0.0}

    def _rs(lb: int) -> float:
        if len(stock_close) <= lb:
            return 0.0
        stock_ret = float(stock_close.iloc[-1] / stock_close.iloc[-1 - lb] - 1.0)
        if bench_close is None or len(bench_close) <= lb:
            return stock_ret
        bench_ret = float(bench_close.iloc[-1] / bench_close.iloc[-1 - lb] - 1.0)
        return stock_ret - bench_ret

    rs63 = _rs(63)
    rs126 = _rs(126)
    rs252 = _rs(252)
    rs_score = (rs63 * 0.4 + rs126 * 0.4 + rs252 * 0.2) * 100.0
    return {
        "rs63": rs63,
        "rs126": rs126,
        "rs252": rs252,
        "rs_score": rs_score,
    }


def compute_ai_rs_scores(rows: list[dict[str, Any]]) -> dict[str, float]:
    if not rows:
        return {}

    matrix = []
    codes = []
    target = []
    for row in rows:
        code = str(row.get("code") or "").zfill(6)
        if not code:
            continue
        rs63 = _safe_float(row.get("rs63"), 0.0)
        rs126 = _safe_float(row.get("rs126"), 0.0)
        rs252 = _safe_float(row.get("rs252"), 0.0)
        volume_trend = _safe_float(row.get("volume_trend"), 0.0)
        volatility = _safe_float(row.get("volatility"), 0.0)
        momentum = _safe_float(row.get("momentum"), 0.0)
        matrix.append([rs63, rs126, rs252, volume_trend, volatility, momentum])
        codes.append(code)
        target.append(_safe_float(row.get("rs_score"), 0.0))

    if not matrix:
        return {}

    X = np.asarray(matrix, dtype=float)
    y = np.asarray(target, dtype=float)

    preds = y.copy()
    try:
        sklearn_ensemble = importlib.import_module("sklearn.ensemble")
        GradientBoostingRegressor = getattr(sklearn_ensemble, "GradientBoostingRegressor")

        model = GradientBoostingRegressor(
            random_state=42,
            n_estimators=120,
            learning_rate=0.05,
            max_depth=2,
        )
        model.fit(X, y)
        preds = model.predict(X)
    except Exception:
        preds = y

    pct = _pct_rank(preds.tolist())
    return {code: float(score) for code, score in zip(codes, pct)}


def optimize_meta_k(df: pd.DataFrame, lookback: int = 20) -> tuple[float, float, float]:
    if df is None or df.empty:
        return 0.5, 0.0, 0.0
    required = {"open", "high", "low", "close"}
    if not required.issubset(df.columns):
        return 0.5, 0.0, 0.0

    work = df.tail(max(lookback + 1, 30)).copy()
    if len(work) < lookback + 1:
        return 0.5, 0.0, 0.0

    best_k = 0.5
    best_score = -1.0
    for k in np.arange(0.1, 1.01, 0.05):
        sub = work.iloc[-(lookback + 1):-1]
        rng = (sub["high"] - sub["low"]).astype(float)
        target = sub["open"].astype(float) + float(k) * rng
        success = (sub["close"].astype(float) > target).astype(float)
        score = float(success.mean())
        if score > best_score:
            best_score = score
            best_k = float(k)

    last = work.iloc[-1]
    target_today = float(last["open"]) + best_k * float(last["high"] - last["low"])
    breakout_score = 100.0 if float(last["close"]) > target_today else max(0.0, 60.0 * best_score)
    return best_k, target_today, breakout_score


def compute_liquidity_score(avg_value20: float, turnover_pct: float) -> float:
    value_score = max(0.0, min(100.0, (avg_value20 / 1_000_000_000.0) * 100.0))
    turnover_score = max(0.0, min(100.0, (turnover_pct / 1.0) * 100.0))
    return float(value_score * 0.7 + turnover_score * 0.3)


def compute_volatility_score(atr_pct: float) -> float:
    return max(0.0, min(100.0, 100.0 - abs(float(atr_pct) - 0.03) * 1500.0))
