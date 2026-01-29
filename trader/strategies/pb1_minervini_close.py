from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

import numpy as np
import pandas as pd


@dataclass
class MinerviniConfig:
    pivot_lookback_min: int = 20
    pivot_lookback_max: int = 60
    pivot_buffer_pct: float = 0.0015  # 0.15%
    stop_pct: float = 0.075  # 7.5%
    rs_window: int = 252  # ~12m
    rs_min_percentile: float = 0.7  # 상위 30%
    min_dollar_vol_50d: float = 2.0e9  # 예: 20억(조정 가능)
    vol_breakout_mult: float = 1.3  # 돌파 거래량
    ma200_slope_days: int = 20


def _ma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n).mean()


def _atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean()


def _pct_return(s: pd.Series, n: int) -> float:
    if len(s) < n + 1:
        return float("nan")
    return float(s.iloc[-1] / s.iloc[-(n + 1)] - 1.0)


def _pivot_high(df: pd.DataFrame, lb_min: int, lb_max: int) -> Tuple[float, int]:
    """
    pivot = 최근 lb_max 구간에서, 마지막 lb_min 구간을 제외한 영역의 최고가
    (간단 버전: '최근에 너무 가까운 고점'을 피하려는 목적)
    """
    if len(df) < lb_max + 5:
        return float("nan"), -1
    window = df.iloc[-lb_max:]
    # 마지막 lb_min일은 pivot 확정 전 구간(너무 최근 고점 제거)
    candidate = window.iloc[:-lb_min]
    if candidate.empty:
        return float("nan"), -1
    idx = int(candidate["high"].values.argmax())
    pivot = float(candidate["high"].iloc[idx])
    pivot_age = int(len(candidate) - 1 - idx)
    return pivot, pivot_age


def compute_features(df: pd.DataFrame) -> Dict[str, float]:
    """
    df: columns: open, high, low, close, volume
    """
    df = df.sort_values("date")
    close = df["close"]
    vol = df["volume"]

    ma50 = _ma(close, 50)
    ma150 = _ma(close, 150)
    ma200 = _ma(close, 200)
    atr14 = _atr(df, 14)

    # 52주 고저는 252일 이상 데이터가 있을 때만 계산
    # 200일 데이터로는 왜곡되므로 None 처리
    hi_52w = float(close.rolling(252).max().iloc[-1]) if len(df) >= 252 else None
    lo_52w = float(close.rolling(252).min().iloc[-1]) if len(df) >= 252 else None

    # 거래대금(대략): close*volume (원 단위 가정) — 데이터 스케일에 맞춰 조정 가능
    dollar_vol_50 = float((close * vol).rolling(50).mean().iloc[-1]) if len(df) >= 50 else float("nan")
    value20 = float((close * vol).rolling(20).mean().iloc[-1]) if len(df) >= 20 else float("nan")
    vol20 = float(vol.rolling(20).mean().iloc[-1]) if len(df) >= 20 else float("nan")

    # MA200 slope
    ma200_slope = float(ma200.iloc[-1] - ma200.iloc[-(1 + 20)]) if len(df) >= 221 else float("nan")

    pivot, pivot_age = _pivot_high(df, 20, 60)

    atr_value = float(atr14.iloc[-1]) if not np.isnan(atr14.iloc[-1]) else float("nan")
    last_close = float(close.iloc[-1])
    # ATR ratio (0~1) 계산 - 비교는 ratio끼리, 표시만 %화
    atr_ratio = float(atr_value / last_close) if (last_close and last_close > 0 and np.isfinite(atr_value)) else float("nan")

    return {
        "close": last_close,
        "ma50": float(ma50.iloc[-1]),
        "ma150": float(ma150.iloc[-1]),
        "ma200": float(ma200.iloc[-1]),
        "ma200_slope": ma200_slope,
        "atr14": atr_value,
        "atr_pct": atr_ratio,  # ratio (0~1) 저장
        "hi_52w": hi_52w,
        "lo_52w": lo_52w,
        "dollar_vol_50": dollar_vol_50,
        "value20": value20,
        "vol20": vol20,
        "ret_63": _pct_return(close, 63),  # ~3m
        "ret_126": _pct_return(close, 126),  # ~6m
        "ret_252": _pct_return(close, 252),  # ~12m
        "pivot": pivot,
        "pivot_age": float(pivot_age),
    }


def evaluate_filters(feats: Dict[str, float], cfg: MinerviniConfig) -> Tuple[bool, list[str]]:
    c = feats["close"]
    ma50, ma150, ma200 = feats["ma50"], feats["ma150"], feats["ma200"]
    ma200_slope = feats["ma200_slope"]
    hi_52w, lo_52w = feats["hi_52w"], feats["lo_52w"]
    dv50 = feats["dollar_vol_50"]

    reasons = []

    if not (c > ma50 > ma150 > ma200):
        reasons.append("trend_template_fail")
    if not (ma200_slope > 0):
        reasons.append("ma200_not_rising")
    # 52주 고저: None이면 스킵 (탈락시키지 않음)
    if hi_52w is not None and not (np.isfinite(hi_52w) and c >= hi_52w * 0.75):
        reasons.append("too_far_from_52w_high")
    if lo_52w is not None and not (np.isfinite(lo_52w) and c >= lo_52w * 1.30):
        reasons.append("not_enough_off_52w_low")
    if not (np.isfinite(dv50) and dv50 >= cfg.min_dollar_vol_50d):
        reasons.append("illiquid")

    ok = len(reasons) == 0
    return ok, reasons


def entry_trigger(
    feats: Dict[str, float],
    last_price: float,
    last_volume: float,
    cfg: MinerviniConfig,
) -> Tuple[bool, dict]:
    """
    last_price/last_volume: 실시간(또는 당일) 값
    """
    pivot = feats["pivot"]
    vol20 = feats["vol20"]
    if not np.isfinite(pivot) or pivot <= 0:
        return False, {"reason": "no_pivot"}

    trigger = pivot * (1.0 + cfg.pivot_buffer_pct)
    vol_ok = np.isfinite(vol20) and last_volume >= vol20 * cfg.vol_breakout_mult

    if last_price >= trigger and vol_ok:
        return True, {
            "pivot": pivot,
            "trigger": trigger,
            "vol20": vol20,
            "vol_ok": True,
        }
    return False, {"pivot": pivot, "trigger": trigger, "vol_ok": vol_ok}


def initial_stop(entry_price: float, feats: Dict[str, float], cfg: MinerviniConfig) -> float:
    # 기본 7~8% 룰
    return entry_price * (1.0 - cfg.stop_pct)


def trail_stop(entry_price: float, max_price: float, feats: Dict[str, float]) -> float:
    """
    간단 버전:
    - +10%면 breakeven
    - +20%면 10일 저점(여기선 feats에 없으니 PB1 엔진에서 df로 계산하도록 연결)
    - 기본은 MA50 아래로 내려가면 정리
    """
    stop = entry_price * 0.925  # fallback
    if max_price >= entry_price * 1.10:
        stop = max(stop, entry_price * 0.995)
    return stop


def score_setup(feats: Dict[str, float], rs_percentile: float) -> float:
    """
    rs_percentile: 0~1 (PB1 엔진에서 유니버스 기준 계산해서 주입)
    """
    c = feats["close"]
    pivot = feats["pivot"]
    dist = 0.0
    if np.isfinite(pivot) and pivot > 0:
        dist = float((pivot - c) / pivot)  # pivot 아래에 가까울수록 좋음
        dist = max(0.0, min(0.2, dist))  # 0~0.2 clip

    # RS(가장 중요) + pivot 접근성
    return (rs_percentile * 100.0) - (dist * 100.0)
