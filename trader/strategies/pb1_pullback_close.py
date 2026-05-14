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
    PB1_MA20_SLOPE_HARD_FAIL_MIN,
    PB1_PULLBACK_BAND_KOSDAQ_STRICT,
    PB1_PULLBACK_BAND_KOSDAQ,
    PB1_PULLBACK_BAND_KOSPI_STRICT,
    PB1_PULLBACK_BAND_KOSPI,
    PB1_PULLBACK_BAND_RELAXED,
    PB1_RELAX_MA20_SLOPE,
    PB1_RELAX_MA_FILTER,
    PB1_R_FLOOR_PCT,
    PB1_SWING_TREND_MIN,
    PB1_SWING_VOL_CONTRACTION_MAX,
    PB1_SWING_VOLU_CONTRACTION_MAX,
    PB1_VOL_CONTRACTION_MAX_STRICT,
    PB1_VOL_CONTRACTION_MAX,
    PB1_VOLU_CONTRACTION_MAX_STRICT,
    PB1_VOLU_CONTRACTION_MAX,
    PB1_TIME_STOP_DAYS,
    KOSDAQ_HARD_STOP_PCT,
    KOSPI_HARD_STOP_PCT,
    PB1_MIN_CANDLES,
)


def _pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a / b) * 100.0


def compute_features(daily_df: pd.DataFrame, *, min_candles: int = PB1_MIN_CANDLES) -> Dict[str, float]:
    df = daily_df.copy()
    df = df.sort_values("date")
    if len(df) < min_candles:
        raise ValueError(f"insufficient_candles:{len(df)}<{min_candles}")
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

    # 유동성(거래대금) 지표: 20일 평균 거래대금
    if not volume_missing:
        df["value"] = df["close"] * df["volume"]
        df["value20"] = df["value"].rolling(20).mean()
    else:
        df["value20"] = np.nan

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

    value20 = float(last["value20"])
    if math.isnan(value20):
        value20 = None

    features = {
        "close": float(last["close"]),
        "ma20": float(last["ma20"]),
        "ma50": float(last["ma50"]),
        "ma10": float(last["ma10"]),
        "atr14": float(last["atr14"]),
        "atr_pct": float(last["atr14"]) / float(last["close"]) if float(last["close"]) > 0 else 0.0,  # ratio (0~1)
        "vol_contraction": float(last["vol_contraction"]),
        "volu_contraction": float(last["volu_contraction"]),
        "ma20_slope": slope,
        "high20": float(high20),
        "pullback_pct": _pct(high20 - last["close"], high20),
        "tr_range_pct": float(last["tr_range_pct"]),
        "trend_strength": float(last["close"] / last["ma50"] if last["ma50"] else math.inf),
        "value20": value20,
        "volume_missing": volume_missing,
    }
    return features


def _is_missing(value: float | None) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def evaluate_setup(
    features: Dict[str, float],
    market: str,
    require_volume: bool = True,
    *,
    mode: str = "relaxed",
    relax_ma_filter: bool | None = None,
    relax_ma20_slope: bool | None = None,
) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    soft_reasons: List[str] = []
    close = features.get("close")
    ma20 = features.get("ma20")
    ma50 = features.get("ma50")
    pullback = features.get("pullback_pct")
    vol_c = features.get("vol_contraction")
    volu_c = features.get("volu_contraction")
    slope = features.get("ma20_slope")
    volume_missing = bool(features.get("volume_missing"))
    use_relaxed_mode = str(mode or "").strip().lower() != "strict"
    if use_relaxed_mode and not PB1_PULLBACK_BAND_RELAXED:
        use_relaxed_mode = False
    if relax_ma_filter is None:
        relax_ma_filter = PB1_RELAX_MA_FILTER
    if relax_ma20_slope is None:
        relax_ma20_slope = PB1_RELAX_MA20_SLOPE
    if not use_relaxed_mode:
        relax_ma_filter = False
        relax_ma20_slope = False

    if volume_missing and require_volume:
        reasons.append("volume_missing")
    if close is None or ma20 is None or ma50 is None:
        reasons.append("missing_ma")
    else:
        if relax_ma_filter:
            if not (close > ma20):
                reasons.append("close_below_ma20")
            if not (close > ma50):
                soft_reasons.append("close_below_ma50")
        else:
            if not (close > ma20 and close > ma50):
                reasons.append("close_below_ma")
    if slope is None:
        reasons.append("ma20_slope_missing")
    elif relax_ma20_slope:
        if slope <= PB1_MA20_SLOPE_HARD_FAIL_MIN:
            reasons.append("ma20_slope_hard_fail")
        elif slope <= 0:
            soft_reasons.append("ma20_slope_nonpos")
    elif slope <= 0:
        reasons.append("ma20_slope_nonpos")

    if pullback is None:
        reasons.append("pullback_missing")
    else:
        if use_relaxed_mode:
            low, high = (PB1_PULLBACK_BAND_KOSPI if market == "KOSPI" else PB1_PULLBACK_BAND_KOSDAQ)
        else:
            low, high = (PB1_PULLBACK_BAND_KOSPI_STRICT if market == "KOSPI" else PB1_PULLBACK_BAND_KOSDAQ_STRICT)
        if high <= 1.0:
            # 호환성을 위해 0~1 구간으로 들어온 설정값은 %로 확장
            low *= 100.0
            high *= 100.0
        if not (low <= pullback <= high):
            reasons.append("pullback_out_of_band")

    vol_max = PB1_VOL_CONTRACTION_MAX if use_relaxed_mode else PB1_VOL_CONTRACTION_MAX_STRICT
    volu_max = PB1_VOLU_CONTRACTION_MAX if use_relaxed_mode else PB1_VOLU_CONTRACTION_MAX_STRICT
    if _is_missing(vol_c) or vol_c > vol_max:
        reasons.append("vol_contraction_fail")
    if not volume_missing and (_is_missing(volu_c) or volu_c > volu_max):
        reasons.append("volu_contraction_fail")

    merged_reasons = reasons + [f"soft:{r}" for r in soft_reasons]
    return (len(reasons) == 0, merged_reasons)


def classify_pb1_near_miss(
    features: dict,
    reasons: list[str],
    *,
    market: str,
    rs_percentile: float | None = None,
    trend_score: float | None = None,
    atr_max_pct: float = 0.10,
) -> tuple[bool, list[str]]:
    """
    한국장 PB1 setup near-miss 판정.

    목적:
    - vol_contraction_fail 단독 또는 contraction 계열 단독 실패로 전 종목이 탈락하는 문제 방지
    - 단, 추세/MA/ATR 핵심 리스크 조건은 유지

    반환:
    - near_miss_ok: True이면 near-miss 복구 가능
    - near_miss_reasons: 복구 허용/거부 사유
    """
    hard_reasons = set(reasons or [])

    # 절대 차단: 핵심 리스크 조건
    hard_blockers = {
        "close_below_ma20",
        "close_below_ma",
        "ma20_slope_hard_fail",
        "ma20_slope_missing",
        "missing_ma",
        "pullback_missing",
        "volume_missing",
    }

    if hard_reasons & hard_blockers:
        return False, ["hard_blocker_present"]

    # contraction 계열만 허용
    allowed_contraction_only = {
        "vol_contraction_fail",
        "volu_contraction_fail",
    }

    # contraction 이외의 hard reason이 있으면 차단
    non_contraction_reasons = [
        r for r in hard_reasons
        if r not in allowed_contraction_only and not str(r).startswith("soft:")
    ]

    if non_contraction_reasons:
        return False, ["non_contraction_hard_reason"]

    # 핵심 가격/MA 검증
    close = features.get("close")
    ma20 = features.get("ma20")
    ma50 = features.get("ma50")
    slope = features.get("ma20_slope")
    atr_pct = features.get("atr_pct")

    try:
        close = float(close)
        ma20 = float(ma20)
        ma50 = float(ma50) if ma50 is not None else None
    except Exception:
        return False, ["missing_price_or_ma"]

    # close > ma20 절대 조건
    if not (close > ma20):
        return False, ["close_not_above_ma20"]

    # ma50이 있으면 97% 이상 유지
    if ma50 is not None and not (close > ma50 * 0.97):
        return False, ["too_far_below_ma50"]

    # ma20_slope 절대 조건
    try:
        slope_value = float(slope)
    except Exception:
        return False, ["slope_missing"]

    if slope_value <= PB1_MA20_SLOPE_HARD_FAIL_MIN:
        return False, ["ma20_slope_hard_fail"]

    # ATR 초과 차단
    try:
        atr_value = float(atr_pct or 0)
    except Exception:
        atr_value = 999

    if atr_value > atr_max_pct:
        return False, ["atr_too_high"]

    # RS percentile 최소 조건
    if rs_percentile is not None:
        try:
            if float(rs_percentile) < 75:
                return False, ["rs_too_low_for_near_miss"]
        except Exception:
            pass

    # 모든 조건 통과: contraction 단독 실패만 있음
    return True, ["contraction_only_near_miss"]


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


def score_setup(features: Dict[str, float], market: str) -> float:
    """
    0~100 점수. 높을수록 '최고 눌림목'에 가깝다.
    - 추세 강함(trend_strength)
    - 눌림이 밴드 중앙에 가까움(pullback_pct)
    - 변동성/거래량 수축이 강함(vol_contraction, volu_contraction)
    - ATR%가 과도하게 크지 않음(atr_pct)
    """
    trend = float(features.get("trend_strength") or 0.0)   # close/ma50
    pullback = float(features.get("pullback_pct") or 999.0)
    vol_c = float(features.get("vol_contraction") or 9.0)
    volu_c = float(features.get("volu_contraction") or 9.0)
    atr_pct = float(features.get("atr_pct") or 999.0)

    if market.upper() == "KOSPI":
        low, high = PB1_PULLBACK_BAND_KOSPI
    else:
        low, high = PB1_PULLBACK_BAND_KOSDAQ

    # 1) 추세: 1.00~1.20 구간을 0~1로 정규화
    s_trend = max(0.0, min(1.0, (trend - 1.00) / 0.20))

    # 2) 눌림: 밴드 중앙에 가까울수록 점수 ↑
    mid = (low + high) / 2.0
    half = max(1e-6, (high - low) / 2.0)
    s_pull = max(0.0, 1.0 - abs(pullback - mid) / half)

    # 3) 수축: 낮을수록 좋음 (0.5가 매우 좋다고 가정)
    s_vol = max(0.0, min(1.0, (0.9 - vol_c) / 0.4))        # vol_c 0.5~0.9
    s_volu = max(0.0, min(1.0, (0.9 - volu_c) / 0.4))      # volu_c 0.5~0.9

    # 4) ATR%: 너무 크면 감점 (2~6%가 이상적이라 가정 -> 0.02~0.06 ratio)
    # atr_pct가 0.06 이하면 최고 점수, 0.10이면 0점
    s_atr = max(0.0, min(1.0, (0.06 - atr_pct) / 0.04))

    score = 100.0 * (
        0.35 * s_trend +
        0.25 * s_pull +
        0.20 * s_vol +
        0.10 * s_volu +
        0.10 * s_atr
    )
    return float(max(0.0, min(100.0, score)))


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
