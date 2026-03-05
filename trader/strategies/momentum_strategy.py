"""
PB-CORE v6: Momentum Strategy
장중 모멘텀 포착 전략
"""
from __future__ import annotations

import math
from typing import Dict

import pandas as pd
import numpy as np


def compute_vwap(df: pd.DataFrame) -> pd.Series:
    """
    VWAP (Volume Weighted Average Price) 계산
    """
    if len(df) < 1:
        return pd.Series(dtype=float)
    
    df = df.sort_values("date")
    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    cumulative_tpv = (typical_price * df["volume"]).cumsum()
    cumulative_volume = df["volume"].cumsum()
    
    vwap = cumulative_tpv / cumulative_volume
    return vwap


def compute_momentum_features(df: pd.DataFrame, intraday_price: float | None = None) -> Dict[str, float]:
    """
    Momentum 전략을 위한 기술적 지표 계산
    
    Args:
        df: OHLCV 데이터프레임
        intraday_price: 장중 현재가 (None이면 최근 종가 사용)
    
    Returns:
        Dict containing momentum-specific features
    """
    df = df.sort_values("date")
    
    if len(df) < 20:
        return {
            "momentum_ok": False,
            "momentum_reason": "insufficient_data",
            "data_ok": False,
        }
    
    close = df["close"]
    volume = df["volume"]
    
    # VWAP 계산
    vwap = compute_vwap(df)
    last_vwap = float(vwap.iloc[-1]) if len(vwap) > 0 else 0.0
    
    # 이동평균
    volume_ma20 = volume.rolling(20).mean()
    
    # 최근 데이터
    last = df.iloc[-1]
    current_price = intraday_price if intraday_price is not None else float(last["close"])
    last_volume = float(last["volume"])
    volume_ma20_val = float(volume_ma20.iloc[-1]) if len(df) >= 20 else 0.0
    
    # 장중 고가 대비 하락 (intraday pullback)
    high_today = float(last["high"])
    drop_from_high = (high_today - current_price) / high_today if high_today > 0 else 0.0
    
    # VWAP 대비 위치
    above_vwap = current_price > last_vwap
    
    # 거래량 증가 확인 (1.2배 이상)
    volume_increase = volume_ma20_val > 0 and last_volume >= volume_ma20_val * 1.2
    
    features = {
        "current_price": current_price,
        "vwap": last_vwap,
        "above_vwap": above_vwap,
        "high_today": high_today,
        "drop_from_high_pct": drop_from_high * 100,
        "volume": last_volume,
        "volume_ma20": volume_ma20_val,
        "volume_ratio": last_volume / volume_ma20_val if volume_ma20_val > 0 else 0.0,
        "volume_increase": volume_increase,
        "data_ok": True,
    }
    
    return features


def check_momentum_signal(
    features: Dict[str, float],
    min_volume_mult: float = 1.2,
    max_drop_from_high_pct: float = 3.0,
) -> Dict[str, any]:
    """
    Momentum 매수 신호 판정
    
    Args:
        features: compute_momentum_features 결과
        min_volume_mult: 최소 거래량 배수 (default: 1.2)
        max_drop_from_high_pct: 장중 고가 대비 최대 허용 하락률 (default: 3%)
    
    Returns:
        Dict with signal_ok, score, reason
    """
    if not features.get("data_ok", False):
        return {
            "signal_ok": False,
            "score": 0.0,
            "reason": "data_missing",
            "strategy": "momentum",
        }
    
    above_vwap = features.get("above_vwap", False)
    volume_increase = features.get("volume_increase", False)
    volume_ratio = features.get("volume_ratio", 0.0)
    drop_from_high_pct = features.get("drop_from_high_pct", 100.0)
    
    # 조건 체크
    if not above_vwap:
        return {
            "signal_ok": False,
            "score": 0.0,
            "reason": "below_vwap",
            "strategy": "momentum",
        }
    
    if volume_ratio < min_volume_mult:
        return {
            "signal_ok": False,
            "score": 0.0,
            "reason": "insufficient_volume",
            "strategy": "momentum",
        }
    
    if drop_from_high_pct > max_drop_from_high_pct:
        return {
            "signal_ok": False,
            "score": 0.0,
            "reason": "too_much_pullback",
            "strategy": "momentum",
        }
    
    # 점수 계산 (0~100)
    score = (
        (20 if above_vwap else 0) +  # VWAP 위 20점
        min(volume_ratio * 15, 40) +  # Volume 최대 40점
        max(0, 40 - drop_from_high_pct * 10)  # 고점 근처일수록 높은 점수
    )
    
    return {
        "signal_ok": True,
        "score": min(score, 100.0),
        "reason": "momentum_confirmed",
        "strategy": "momentum",
        "volume_ratio": volume_ratio,
        "above_vwap": above_vwap,
    }


def score_momentum(features: Dict[str, float]) -> float:
    """
    Momentum 전략 점수 (간단 버전)
    
    Returns:
        0~100 점수
    """
    result = check_momentum_signal(features)
    return result.get("score", 0.0)
