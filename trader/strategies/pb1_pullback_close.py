from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, time
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from trader.config import (
    PB1_DAY_SL_R,
    PB1_DAY_TP_R,
    PB1_PULLBACK_BAND_KOSDAQ,
    PB1_PULLBACK_BAND_KOSPI,
    PB1_R_FLOOR_PCT,
    PB1_SWING_TREND_MIN,
    PB1_SWING_VOL_CONTRACTION_MAX,
    PB1_SWING_VOLU_CONTRACTION_MAX,
    PB1_VOL_CONTRACTION_MAX,
    PB1_VOLU_CONTRACTION_MAX,
    PB1_TIME_STOP_DAYS,
    KOSDAQ_HARD_STOP_PCT,
    KOSPI_HARD_STOP_PCT,
)


def _pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a / b) * 100.0


def compute_features(daily_df: pd.DataFrame) -> Dict[str, float]:
    df = daily_df.copy()
    df = df.sort_values("date")
    if len(df) < 60:
        return {"setup_ok": False, "reasons": ["insufficient_candles"], "count": len(df)}
    volume_missing = df["volume"].isna().all()
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma50"] = df["close"].rolling(50).mean()
    df["ma10"] = df["close"].rolling(10).mean()
    df["tr"] = np.maximum(df["high"], df["close"].shift(1)) - np.minimum(df["low"], df["close"].shift(1))
    df["atr14"] = df["tr"].rolling(14).mean()
    df["tr_range_pct"] = (df["high"] - df["low"]) / df["close"] * 100
    df["vol_contraction"] = df["tr_range_pct"].rolling(5).mean() / df["tr_range_pct"].rolling(20).mean()
    volu_contraction = df["volume"].rolling(5).mean() / df["volume"].rolling(20).mean()
    df["volu_contraction"] = volu_contraction if not volume_missing else np.nan

    ma20_tail = df["ma20"].tail(5)
    slope = None
    if len(ma20_tail.dropna()) >= 5:
        x = np.arange(len(ma20_tail))
        try:
            slope = float(np.polyfit(x, ma20_tail.values, 1)[0])
        except Exception:
            slope = None

    last = df.iloc[-1]
    high20 = df["high"].tail(20).max()
    features = {
        "close": float(last["close"]),
        "ma20": float(last["ma20"]),
        "ma50": float(last["ma50"]),
        "ma10": float(last["ma10"]),
        "atr14": float(last["atr14"]),
        "vol_contraction": float(last["vol_contraction"]),
        "volu_contraction": float(last["volu_contraction"]),
        "ma20_slope": slope,
        "high20": float(high20),
        "pullback_pct": _pct(high20 - last["close"], high20),
        "tr_range_pct": float(last["tr_range_pct"]),
        "trend_strength": float(last["close"] / last["ma50"] if last["ma50"] else math.inf),
        "volume_missing": volume_missing,
    }
    return features


def _is_missing(value: float | None) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def evaluate_setup(features: Dict[str, float], market: str, require_volume: bool = True) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    close = features.get("close")
    ma20 = features.get("ma20")
    ma50 = features.get("ma50")
    pullback = features.get("pullback_pct")
    vol_c = features.get("vol_contraction")
    volu_c = features.get("volu_contraction")
    slope = features.get("ma20_slope")
    volume_missing = bool(features.get("volume_missing"))

    if volume_missing and require_volume:
        reasons.append("volume_missing")
    if close is None or ma20 is None or ma50 is None:
        reasons.append("missing_ma")
    else:
        if not (close > ma20 and close > ma50):
            reasons.append("close_below_ma")
    if slope is None or slope <= 0:
        reasons.append("ma20_slope_nonpos")

    if pullback is None:
        reasons.append("pullback_missing")
    else:
        low, high = (PB1_PULLBACK_BAND_KOSPI if market == "KOSPI" else PB1_PULLBACK_BAND_KOSDAQ)
        if not (low <= pullback <= high):
            reasons.append("pullback_out_of_band")

    if _is_missing(vol_c) or vol_c > PB1_VOL_CONTRACTION_MAX:
        reasons.append("vol_contraction_fail")
    if not volume_missing and (_is_missing(volu_c) or volu_c > PB1_VOLU_CONTRACTION_MAX):
        reasons.append("volu_contraction_fail")

    return (len(reasons) == 0, reasons)


def choose_mode(features: Dict[str, float]) -> Tuple[int, List[str]]:
    reasons: List[str] = []
    trend = features.get("trend_strength") or 0
    vol_c = features.get("vol_contraction") or 0
    volu_c = features.get("volu_contraction") or 0
    if trend >= PB1_SWING_TREND_MIN and vol_c <= PB1_SWING_VOL_CONTRACTION_MAX and volu_c <= PB1_SWING_VOLU_CONTRACTION_MAX:
        reasons.append("swing_conditions_met")
        return 2, reasons
    reasons.append("default_day_mode")
    return 1, reasons


@dataclass
class OrderIntent:
    code: str
    market: str
    sid: int
    mode: int
    qty: int
    price: float
    window: str
    client_order_key: str
    stage: str


def plan_entry(code: str, features: Dict[str, float], mode: int, now: datetime, close_window: Tuple[time, time]) -> OrderIntent | None:
    start, end = close_window
    if not (start <= now.time() < end):
        return None
    price = features.get("close")
    if price is None:
        return None
    client_key = f"{now.date()}|{code}|sid=1|mode={mode}|BUY|close"
    return OrderIntent(
        code=code,
        market=features.get("market", ""),
        sid=1,
        mode=mode,
        qty=features.get("planned_qty", 0) or 0,
        price=float(price),
        window="close",
        client_order_key=client_key,
        stage="PB1-CLOSE",
    )


@dataclass
class ExitIntent:
    code: str
    market: str
    sid: int
    mode: int
    reason: str
    qty: int
    price: float | None
    stage: str


def plan_exit(position: Dict, features: Dict[str, float], now: datetime, windows: Dict[str, Tuple[time, time]]) -> ExitIntent | None:
    mode = position.get("mode")
    qty = position.get("total_qty") or 0
    if qty <= 0:
        return None
    avg = position.get("avg_buy_price")
    if avg is None:
        return None
    code = position.get("code")
    market = position.get("market")
    if mode == 1:
        window = windows.get("morning_exit")
        if not window:
            return None
        start, end = window
        if not (start <= now.time() <= end):
            return None
        atr_pct = _pct(features.get("atr14", 0.0), avg)
        r_pct = max(atr_pct, PB1_R_FLOOR_PCT)
        take_profit_pct = PB1_DAY_TP_R * r_pct
        stop_loss_pct = PB1_DAY_SL_R * r_pct
        mark = features.get("mark_price", features.get("close"))
        if mark is None:
            return None
        ret_pct = _pct(mark - avg, avg)
        if ret_pct >= take_profit_pct or ret_pct <= -stop_loss_pct or now.time() >= end:
            return ExitIntent(
                code=code,
                market=market,
                sid=1,
                mode=mode,
                reason="mode1_exit",
                qty=qty,
                price=mark,
                stage="DAY-EXIT",
            )
    elif mode == 2:
        start, end = windows.get("close", (None, None))
        mark = features.get("mark_price", features.get("close"))
        if mark is None:
            return None
        ret_pct = _pct(mark - avg, avg)
        hard_stop = KOSDAQ_HARD_STOP_PCT if market == "KOSDAQ" else KOSPI_HARD_STOP_PCT
        if ret_pct <= -hard_stop:
            return ExitIntent(
                code=code,
                market=market,
                sid=1,
                mode=mode,
                reason="hard_stop",
                qty=qty,
                price=mark,
                stage="HARD-STOP",
            )
        close_px = features.get("close")
        ma20 = features.get("ma20")
        holding_days = position.get("holding_days") or 0
        if holding_days >= PB1_TIME_STOP_DAYS:
            if start and end and (start <= now.time() < end or now.time() >= end):
                return ExitIntent(
                    code=code,
                    market=market,
                    sid=1,
                    mode=mode,
                    reason="time_stop",
                    qty=qty,
                    price=mark,
                    stage="TIME-STOP",
                )
        if close_px is not None and ma20 is not None and close_px < ma20:
            if start and end and (start <= now.time() < end or now.time() >= end):
                return ExitIntent(
                    code=code,
                    market=market,
                    sid=1,
                    mode=mode,
                    reason="ma20_trail",
                    qty=qty,
                    price=close_px,
                    stage="MA20-TRAIL",
                )
    return None
