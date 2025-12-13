from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import FinanceDataReader as fdr
import numpy as np
import pandas as pd

from rolling_k_auto_trade_api.adjust_price_to_tick import adjust_price_to_tick
from rolling_k_auto_trade_api.kis_api import get_price_quote
from .universe import kospi_universe

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))
INDEX_CODE = "KS11"
DEFAULT_TOP_N = 100
MAX_POSITIONS = 10


def _latest_index_history(days: int = 400) -> pd.DataFrame:
    today = datetime.now(tz=KST).date()
    start = today - timedelta(days=days)
    df = fdr.DataReader(INDEX_CODE, start=start, end=today)
    if df is None or df.empty:
        raise ValueError("no index history")
    df = df.dropna(subset=["Close"]).reset_index()
    if "Date" not in df.columns:
        df = df.rename_axis("Date").reset_index()
    df["date"] = pd.to_datetime(df["Date"]).dt.date
    df = df[["date", "Close"]].rename(columns={"Close": "close"}).sort_values("date")
    return df


def evaluate_regime() -> Dict[str, float | bool]:
    history = _latest_index_history()
    close = pd.Series(history["close"].astype(float))
    if len(close) < 200:
        raise ValueError("insufficient index history for regime")
    ma_200 = close.rolling(200).mean().iloc[-1]
    trend_ok = bool(close.iloc[-1] > ma_200)
    momentum_ok = False
    if len(close) > 63:
        momentum_ok = bool((close.iloc[-1] / close.shift(63).iloc[-1] - 1) * 100 > 0)
    log_ret = np.log(close / close.shift(1)).dropna()
    vol_ok = False
    if len(log_ret) >= 60:
        vol_20 = log_ret.tail(20).std()
        vol_60 = log_ret.tail(60).std()
        vol_ok = bool(vol_20 < vol_60 * 1.2)
    regime_on = sum([trend_ok, momentum_ok, vol_ok]) >= 2
    daily_change = 0.0
    if len(close) >= 2:
        daily_change = float((close.iloc[-1] / close.iloc[-2] - 1) * 100)
    logger.info(
        "[KOSPI_CORE][REGIME] %s | trend=%s | momentum=%s | vol=%s",
        "ON" if regime_on else "OFF",
        trend_ok,
        momentum_ok,
        vol_ok,
    )
    return {
        "regime_on": regime_on,
        "trend_ok": trend_ok,
        "momentum_ok": momentum_ok,
        "vol_ok": vol_ok,
        "daily_change_pct": daily_change,
    }


def _stock_history(code: str, days: int = 220) -> pd.DataFrame:
    today = datetime.now(tz=KST).date()
    start = today - timedelta(days=days)
    df = fdr.DataReader(code, start=start, end=today)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.dropna(subset=["Close"]).reset_index()
    if "Date" not in df.columns:
        df = df.rename_axis("Date").reset_index()
    df["date"] = pd.to_datetime(df["Date"]).dt.date
    df = df[["date", "Close"]].rename(columns={"Close": "close"}).sort_values("date")
    return df


def _compute_metrics(code: str) -> Dict[str, float] | None:
    df = _stock_history(code)
    if df.empty or len(df) < 126:
        return None
    close = pd.Series(df["close"].astype(float))
    ret_6m = float((close.iloc[-1] / close.iloc[-126] - 1) * 100)
    vol_6m = float(close.pct_change().dropna().tail(126).std())
    return {"code": code, "ret_6m": ret_6m, "vol_6m": vol_6m}


def _select_candidates(universe: List[Dict[str, str]]) -> List[str]:
    metrics: List[Dict[str, float]] = []
    for item in universe:
        code = item.get("code")
        m = _compute_metrics(code)
        if m:
            metrics.append(m)
    if not metrics:
        logger.warning("[KOSPI_CORE] no metrics available for selection")
        return []
    metrics = sorted(metrics, key=lambda x: x["ret_6m"], reverse=True)
    top_cut = max(1, int(len(metrics) * 0.4))
    top_momentum = {m["code"] for m in metrics[:top_cut]}

    vol_sorted = sorted(metrics, key=lambda x: x["vol_6m"])
    vol_cut = max(1, int(len(vol_sorted) * 0.3))
    low_vol = {m["code"] for m in vol_sorted[:vol_cut]}

    intersection = [m for m in metrics if m["code"] in top_momentum and m["code"] in low_vol]
    intersection = sorted(intersection, key=lambda x: (-x["ret_6m"], x["vol_6m"]))
    final_n = min(MAX_POSITIONS, len(intersection))
    return [m["code"] for m in intersection[:final_n]]


def build_target_allocations(
    total_capital: float, top_n: int = DEFAULT_TOP_N, max_positions: int = MAX_POSITIONS
) -> Tuple[List[Dict[str, float]], Dict[str, List[str]]]:
    universe = kospi_universe(top_n)
    if not universe:
        logger.warning("[KOSPI_CORE] universe empty")
        return [], {"selected": []}
    selected_codes = _select_candidates(universe)[:max_positions]
    if not selected_codes:
        return [], {"selected": []}

    weight = min(1.0 / len(selected_codes), 0.15)
    targets: List[Dict[str, float]] = []
    for item in universe:
        code = item.get("code")
        if code not in selected_codes:
            continue
        try:
            quote = get_price_quote(code)
            price = float(quote.get("askp1") or quote.get("stck_prpr") or 0)
        except Exception:
            logger.exception("[KOSPI_CORE] quote fail for %s", code)
            price = 0.0
        price = adjust_price_to_tick(price) if price else 0.0
        if price <= 0:
            logger.warning("[KOSPI_CORE] skip %s (no price)", code)
            continue
        target_val = total_capital * weight
        target_qty = int(target_val // price) if price > 0 else 0
        targets.append(
            {
                "code": code,
                "name": item.get("name") or "",
                "weight": weight,
                "target_value": target_val,
                "last_price": price,
                "target_qty": target_qty,
            }
        )

    logger.info("[KOSPI_CORE][SELECTION] selected=%s", selected_codes)
    return targets, {"selected": selected_codes}
