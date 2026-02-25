from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Dict, Tuple

import numpy as np
import pandas as pd


@dataclass
class MinerviniConfig:
    # Trend template (단계 B 완화)
    rs_min_percentile: float = 0.70  # 80 -> 70으로 완화
    min_dollar_vol_50d: float = 2.0e9

    # VCP / base
    base_lookback_max: int = 80
    base_exclude_recent: int = 10
    vcp_min_contractions: int = 2
    vcp_max_contractions: int = 4
    contraction_decline_min: float = 0.15
    vol_dryup_ratio: float = 0.70
    tight_close_max_pct: float = 0.015

    # Entry (단계 B 완화)
    pivot_buffer_pct: float = 0.0030  # 0.0015 -> 0.0030으로 완화
    breakout_vol_mult_20: float = 1.5
    max_extension_from_pivot: float = 0.30  # 0.05 -> 0.30으로 완화

    # Stop / trail
    initial_stop_pct: float = 0.075
    breakeven_R: float = 1.0
    trail_use_ma50: bool = True
    heavy_volume_mult: float = 1.5

    # Risk sizing
    risk_pct_of_equity: float = 0.005

    # Pyramiding
    max_pyramid_levels: int = 3
    add_on_R: float = 1.5
    add_on_max_extension: float = 0.03
    add_on_size_frac: float = 0.5


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


def compute_features(df: pd.DataFrame) -> Dict[str, float]:
    df = df.sort_values("date")
    close = df["close"]
    vol = df["volume"]

    ma50 = _ma(close, 50)
    ma150 = _ma(close, 150)
    ma200 = _ma(close, 200)
    ma20 = _ma(close, 20)
    atr14 = _atr(df, 14)

    # 52주 고저 계산: 252일 이상 데이터가 있으면 정확한 52주 사용,
    # 없으면 120일(또는 min_candles) fallback으로 고저값 계산
    lookback = 252 if len(df) >= 252 else max(120, 60)  # 120일 또는 60일 중 큰 값
    hi_52w = float(close.rolling(lookback).max().iloc[-1]) if len(df) >= lookback else None
    lo_52w = float(close.rolling(lookback).min().iloc[-1]) if len(df) >= lookback else None
    hi_52w_available = int(len(df) >= 252)  # 정확한 52주 데이터 여부

    dollar_vol_50 = float((close * vol).rolling(50).mean().iloc[-1]) if len(df) >= 50 else float("nan")
    value20 = float((close * vol).rolling(20).mean().iloc[-1]) if len(df) >= 20 else float("nan")
    vol20 = float(vol.rolling(20).mean().iloc[-1]) if len(df) >= 20 else float("nan")
    vol50 = float(vol.rolling(50).mean().iloc[-1]) if len(df) >= 50 else float("nan")
    last_volume = float(vol.iloc[-1]) if len(df) >= 1 else float("nan")

    slope_lb = int(os.getenv("MA200_SLOPE_LOOKBACK", "20"))
    slope_min_rows = max(200 + slope_lb, 260)
    ma200_tail = ma200.tail(5)
    ma200_tail_nan = int(ma200_tail.isna().sum())
    ma200_slope_method = "standard"
    ma200_slope_reason = "ok"

    if len(df) < slope_min_rows:
        ma200_slope = 0.0
        ma200_slope_method = "unknown"
        ma200_slope_reason = "data_short"
    elif ma200_tail_nan > 0:
        ma200_slope = 0.0
        ma200_slope_method = "unknown"
        ma200_slope_reason = "ma200_nan"
    else:
        ma200_slope = float(ma200.iloc[-1] - ma200.iloc[-(1 + slope_lb)])
        if not np.isfinite(ma200_slope):
            fallback_min = 200 + slope_lb
            if len(df) >= fallback_min and len(ma200) >= slope_lb:
                try:
                    ma200_last = ma200.iloc[-1]
                    ma200_20ago = ma200.iloc[-slope_lb]
                    if np.isfinite(ma200_last) and np.isfinite(ma200_20ago):
                        ma200_slope = 1.0 if ma200_last > ma200_20ago else -1.0
                        ma200_slope_method = "simple_compare"
                except (IndexError, KeyError):
                    pass
            if not np.isfinite(ma200_slope):
                ma200_slope = 0.0
                ma200_slope_method = "unknown"
                ma200_slope_reason = "ma200_nan"

    atr_value = float(atr14.iloc[-1]) if not np.isnan(atr14.iloc[-1]) else float("nan")
    last_close = float(close.iloc[-1])
    # ATR ratio (0~1) 계산 - 비교는 ratio끼리, 표시만 %화
    atr_ratio = float(atr_value / last_close) if (last_close and last_close > 0 and np.isfinite(atr_value)) else float("nan")
    low_10 = float(df["low"].rolling(10).min().iloc[-1]) if len(df) >= 10 else float("nan")

    return {
        "close": last_close,
        "ma50": float(ma50.iloc[-1]),
        "ma150": float(ma150.iloc[-1]),
        "ma200": float(ma200.iloc[-1]),
        "ma20": float(ma20.iloc[-1]),
        "ma200_slope": ma200_slope,
        "ma200_slope_method": ma200_slope_method,
        "ma200_slope_reason": ma200_slope_reason,
        "ma200_slope_window": slope_lb,
        "ma200_tail_nan": ma200_tail_nan,
        "atr14": atr_value,
        "atr_pct": atr_ratio,
        "hi_52w": hi_52w,
        "lo_52w": lo_52w,
        "hi_52w_available": hi_52w_available,
        "dollar_vol_50": dollar_vol_50,
        "value20": value20,
        "vol20": vol20,
        "vol50": vol50,
        "last_volume": last_volume,
        "low_10": low_10,
        "ret_63": _pct_return(close, 63),
        "ret_126": _pct_return(close, 126),
        "ret_252": _pct_return(close, 252),
    }


def detect_vcp(df: pd.DataFrame, cfg: MinerviniConfig) -> dict:
    df = df.sort_values("date")
    required_cols = {"high", "low", "close", "volume"}
    if not required_cols.issubset(df.columns):
        return {
            "vcp_ok": None,
            "contractions": [],
            "contraction_ok": False,
            "vol_dryup": False,
            "tight_close": False,
            "score": None,
            "reason": "missing_required_columns",
        }

    window = df.tail(cfg.base_lookback_max)
    if window.empty or len(window) < cfg.base_lookback_max // 2:
        return {
            "vcp_ok": None,
            "contractions": [],
            "contraction_ok": False,
            "vol_dryup": False,
            "tight_close": False,
            "score": None,
            "reason": "insufficient_window",
        }

    ranges = (window["high"] - window["low"]).to_numpy()
    closes = window["close"].to_numpy()
    range_pct = np.where(closes > 0, ranges / closes, 0.0)

    segments = np.array_split(range_pct, cfg.vcp_max_contractions)
    
    # Compute contractions with NaN handling to avoid RuntimeWarning
    contractions = []
    for seg in segments:
        if len(seg) == 0:
            continue
        # Remove NaN values before computing max
        seg_clean = seg[~np.isnan(seg)]
        if len(seg_clean) == 0:
            continue  # Skip segments with all NaN
        max_val = float(np.max(seg_clean))
        if np.isfinite(max_val):
            contractions.append(max_val)

    contraction_ok = False
    if len(contractions) >= cfg.vcp_min_contractions:
        contraction_ok = True
        for prev, curr in zip(contractions, contractions[1:]):
            if prev <= 0:
                contraction_ok = False
                break
            decline = 1.0 - (curr / prev)
            if decline < cfg.contraction_decline_min:
                contraction_ok = False
                break

    vol_dryup = False
    base_n = min(50, len(window))
    recent_n = min(10, max(5, len(window) // 4))
    if base_n > recent_n and recent_n > 0:
        vol_recent = float(window["volume"].tail(recent_n).mean())
        vol_base = float(window["volume"].tail(base_n).mean())
        if vol_base > 0:
            vol_dryup = vol_recent <= vol_base * cfg.vol_dryup_ratio

    tight_close = False
    if len(window) >= 5:
        recent = window["close"].tail(5)
        mean_close = float(recent.mean())
        if mean_close > 0:
            tight_close = (float(recent.max()) - float(recent.min())) / mean_close <= cfg.tight_close_max_pct

    vcp_ok = contraction_ok and vol_dryup and tight_close

    contraction_component = 0.0
    if len(contractions) >= 2:
        improvements = 0
        comparable = 0
        for prev, curr in zip(contractions, contractions[1:]):
            if prev > 0 and np.isfinite(prev) and np.isfinite(curr):
                comparable += 1
                if curr < prev:
                    improvements += 1
        if comparable > 0:
            contraction_component = 8.0 * (improvements / comparable)

    vol_component = 4.0 if vol_dryup else 0.0
    tight_component = 3.0 if tight_close else 0.0
    score = float(max(0.0, min(15.0, contraction_component + vol_component + tight_component)))

    if vcp_ok:
        reason = "ok"
    else:
        failed = []
        if not contraction_ok:
            failed.append("contraction_not_confirmed")
        if not vol_dryup:
            failed.append("volume_not_dryup")
        if not tight_close:
            failed.append("tight_close_not_met")
        reason = ";".join(failed) if failed else "vcp_not_confirmed"

    return {
        "vcp_ok": vcp_ok,
        "contractions": contractions,
        "contraction_ok": contraction_ok,
        "vol_dryup": vol_dryup,
        "tight_close": tight_close,
        "score": score,
        "reason": reason,
    }


def compute_pivot(df: pd.DataFrame, cfg: MinerviniConfig) -> Tuple[float, dict]:
    df = df.sort_values("date")
    window = df.tail(cfg.base_lookback_max)
    if len(window) <= cfg.base_exclude_recent:
        return float("nan"), {"pivot_age": -1, "valid": False}
    candidate = window.iloc[:-cfg.base_exclude_recent]
    if candidate.empty:
        return float("nan"), {"pivot_age": -1, "valid": False}
    idx = int(candidate["high"].values.argmax())
    pivot = float(candidate["high"].iloc[idx])
    pivot_age = int(len(candidate) - 1 - idx)
    return pivot, {"pivot_age": pivot_age, "valid": np.isfinite(pivot)}


def evaluate_filters(feats: Dict[str, float], cfg: MinerviniConfig) -> Tuple[bool, list[str]]:
    import logging
    import os
    logger = logging.getLogger(__name__)
    debug_mode = os.getenv("MINERVINI_DEBUG") == "1"
    
    c = feats.get("close")
    ma50, ma150, ma200 = feats.get("ma50"), feats.get("ma150"), feats.get("ma200")
    ma200_slope = feats.get("ma200_slope")
    ma200_slope_method = feats.get("ma200_slope_method", "standard")
    dv50 = feats.get("dollar_vol_50")
    rs_percentile = feats.get("rs_percentile")
    vcp_ok = feats.get("vcp_ok")

    if debug_mode:
        logger.info(
            "[MINERVINI][FILTER][ENTER] c=%.2f ma50=%.2f ma150=%.2f ma200=%.2f ma200_slope=%.2f slope_method=%s rs_pct=%.2f vcp_ok=%s rs_min=%.2f",
            c or 0, ma50 or 0, ma150 or 0, ma200 or 0, ma200_slope or 0, ma200_slope_method, (rs_percentile or 0) * 100, vcp_ok, cfg.rs_min_percentile * 100
        )

    reasons: list[str] = []

    if c is None or ma50 is None or ma150 is None or ma200 is None:
        reasons.append("missing_ma")
    elif not (c > ma50 > ma150 > ma200):
        reasons.append("trend_template_fail")
    
    # MA200_slope NaN degrade: unknown일 때는 soft fail (점수 감점만, hard fail 금지)
    if ma200_slope_method == "unknown":
        # slope 조건 unknown: degrade 처리 (점수 감점용 flag만 추가, hard fail 금지)
        reasons.append("ma200_slope_unknown")
        feats["ma200_slope_degraded"] = True  # 점수 감점용 플래그
    elif ma200_slope_method == "simple_compare":
        # 대체 slope 사용: 상승 여부만 체크
        if not (ma200_slope is not None and ma200_slope > 0):
            reasons.append("ma200_not_rising_fallback")
    else:
        # 정상 slope 계산: 기존 로직
        if not (ma200_slope is not None and ma200_slope > 0):
            reasons.append("ma200_not_rising")
    
    if not (rs_percentile is not None and rs_percentile >= cfg.rs_min_percentile):
        reasons.append("rs_below_min")
    if not (dv50 is not None and np.isfinite(dv50) and dv50 >= cfg.min_dollar_vol_50d):
        reasons.append("illiquid")
    if not vcp_ok:
        reasons.append("vcp_fail")

    # ma200_slope_unknown은 soft fail만 (hard fail 금지)
    # 다른 조건들이 통과하면 후보로 유지 (단, 점수 감점 적용)
    hard_fail_reasons = [r for r in reasons if r != "ma200_slope_unknown"]
    ok = len(hard_fail_reasons) == 0
    
    if debug_mode:
        logger.info("[MINERVINI][FILTER][RESULT] ok=%s reasons=%s hard_fail_reasons=%s", ok, reasons, hard_fail_reasons)
    
    return ok, reasons


def entry_trigger(feats: dict, last_price: float, last_volume: float, cfg: MinerviniConfig) -> Tuple[bool, dict]:
    pivot = feats.get("pivot")
    vol20 = feats.get("vol20")
    vcp_ok = feats.get("vcp_ok")

    if not vcp_ok:
        return False, {"reason": "vcp_fail"}
    if pivot is None or not np.isfinite(pivot) or pivot <= 0:
        return False, {"reason": "no_pivot"}
    if vol20 is None or not np.isfinite(vol20) or last_volume <= 0:
        return False, {"reason": "volume_missing"}

    trigger = pivot * (1.0 + cfg.pivot_buffer_pct)
    max_chase = pivot * (1.0 + cfg.max_extension_from_pivot)
    vol_ok = last_volume >= vol20 * cfg.breakout_vol_mult_20

    if last_price >= trigger and last_price <= max_chase and vol_ok:
        return True, {
            "pivot": pivot,
            "trigger": trigger,
            "vol20": vol20,
            "vol_ok": True,
            "max_chase": max_chase,
        }
    return False, {"pivot": pivot, "trigger": trigger, "vol_ok": vol_ok, "max_chase": max_chase}


def initial_stop(entry_price: float, df: pd.DataFrame, feats: dict, cfg: MinerviniConfig) -> float:
    stop_candidates = [entry_price * (1.0 - cfg.initial_stop_pct)]
    low_10 = feats.get("low_10")
    if low_10 is None or not np.isfinite(low_10):
        low_10 = float(df["low"].rolling(10).min().iloc[-1]) if len(df) >= 10 else float("nan")
    if np.isfinite(low_10):
        buffer = entry_price * cfg.pivot_buffer_pct
        stop_candidates.append(low_10 - buffer)
    atr14 = feats.get("atr14")
    if atr14 is not None and np.isfinite(atr14):
        stop_candidates.append(entry_price - 2.0 * float(atr14))
    stop_price = max(stop_candidates)
    return min(stop_price, entry_price * 0.99)


def update_trailing_stop(
    entry_price: float,
    max_price: float,
    df: pd.DataFrame,
    feats: dict,
    cfg: MinerviniConfig,
) -> tuple[float, list[str]]:
    reasons: list[str] = []
    stop_price = float(feats.get("stop_price") or feats.get("initial_stop") or entry_price * (1.0 - cfg.initial_stop_pct))
    initial_stop = float(feats.get("initial_stop") or entry_price * (1.0 - cfg.initial_stop_pct))
    r_value = entry_price - initial_stop
    if r_value > 0 and max_price >= entry_price + cfg.breakeven_R * r_value:
        stop_price = max(stop_price, entry_price * 0.995)
        reasons.append("breakeven")
    if r_value > 0 and max_price >= entry_price + 2.0 * r_value:
        if cfg.trail_use_ma50:
            ma50 = feats.get("ma50")
            if ma50 is not None and np.isfinite(ma50):
                stop_price = max(stop_price, float(ma50))
                reasons.append("trail_ma50")
        low_10 = feats.get("low_10")
        if low_10 is None or not np.isfinite(low_10):
            if len(df) >= 10:
                low_10 = float(df["low"].rolling(10).min().iloc[-1])
        if low_10 is not None and np.isfinite(low_10):
            stop_price = max(stop_price, float(low_10))
            reasons.append("trail_low10")

    return stop_price, reasons


def score_setup(feats: dict, rs_percentile: float, vcp_info: dict, cfg: MinerviniConfig) -> float:
    pivot = feats.get("pivot")
    close = feats.get("close")
    score = rs_percentile * 100.0
    score += float(vcp_info.get("score") or 0.0)
    if pivot is not None and np.isfinite(pivot) and pivot > 0 and close is not None:
        extension = max(0.0, (float(close) - pivot) / pivot)
        score -= extension * 100.0
    
    # MA200_slope degraded 시 점수 감점
    if feats.get("ma200_slope_degraded"):
        score -= 10.0  # 10점 감점
    
    return float(max(0.0, min(120.0, score)))


def risk_position_size(
    *,
    entry_price: float,
    stop_price: float,
    risk_krw: float,
    max_capital_krw: float,
    min_order_krw: float,
) -> int:
    per_share_risk = entry_price - stop_price
    if per_share_risk <= 0:
        return 0
    qty_risk = int(risk_krw // per_share_risk) if risk_krw > 0 else 0
    qty_cap = int(max_capital_krw // entry_price) if max_capital_krw > 0 else qty_risk
    qty = max(0, min(qty_risk, qty_cap))
    if min_order_krw > 0 and qty * entry_price < min_order_krw:
        return 0
    return qty
