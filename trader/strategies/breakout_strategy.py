"""
PB-CORE v6: Breakout Strategy
폭등장/강세장에서 작동하는 돌파 전략
"""
from __future__ import annotations

import math
from typing import Dict

import pandas as pd


def compute_breakout_features(df: pd.DataFrame) -> Dict[str, float]:
    """
    Breakout 전략을 위한 기술적 지표 계산
    
    Args:
        df: OHLCV 데이터프레임 (date, open, high, low, close, volume)
    
    Returns:
        Dict containing breakout-specific features
    """
    df = df.sort_values("date")
    
    if len(df) < 20:
        return {
            "breakout_ok": False,
            "breakout_reason": "insufficient_data",
            "data_ok": False,
        }
    
    close = df["close"]
    volume = df["volume"]
    
    # 20일 고가
    high_20 = df["high"].tail(20).max()
    
    # 이동평균
    volume_ma20 = volume.rolling(20).mean()
    
    # 최근 데이터
    last = df.iloc[-1]
    last_close = float(last["close"])
    last_volume = float(last["volume"])
    volume_ma20_val = float(volume_ma20.iloc[-1]) if len(df) >= 20 else 0.0
    
    # RS percentile (외부에서 주입될 것으로 예상, 여기서는 계산 안 함)
    # 실제로는 candidate pool에서 이미 계산된 값 사용
    
    # 돌파 여부
    breakout_confirmed = last_close > high_20
    
    # 거래량 증가 확인 (1.5배 이상)
    volume_surge = volume_ma20_val > 0 and last_volume >= volume_ma20_val * 1.5
    
    features = {
        "close": last_close,
        "high_20": float(high_20),
        "volume": last_volume,
        "volume_ma20": volume_ma20_val,
        "volume_ratio": last_volume / volume_ma20_val if volume_ma20_val > 0 else 0.0,
        "breakout_confirmed": breakout_confirmed,
        "volume_surge": volume_surge,
        "data_ok": True,
    }
    
    return features


def check_breakout_signal(
    features: Dict[str, float],
    rs_percentile: float = 0.0,
    min_rs_percentile: float = 80.0,
    min_volume_mult: float = 1.5,
) -> Dict[str, any]:
    """
    Breakout 매수 신호 판정
    
    Args:
        features: compute_breakout_features 결과
        rs_percentile: RS percentile (0~100)
        min_rs_percentile: 최소 RS 요구치 (default: 80)
        min_volume_mult: 최소 거래량 배수 (default: 1.5)
    
    Returns:
        Dict with signal_ok, score, reason
    """
    if not features.get("data_ok", False):
        return {
            "signal_ok": False,
            "score": 0.0,
            "reason": "data_missing",
            "strategy": "breakout",
        }
    
    breakout_confirmed = features.get("breakout_confirmed", False)
    volume_surge = features.get("volume_surge", False)
    volume_ratio = features.get("volume_ratio", 0.0)
    
    # 조건 체크
    if not breakout_confirmed:
        return {
            "signal_ok": False,
            "score": 0.0,
            "reason": "no_breakout",
            "strategy": "breakout",
        }
    
    if volume_ratio < min_volume_mult:
        return {
            "signal_ok": False,
            "score": 0.0,
            "reason": "insufficient_volume",
            "strategy": "breakout",
        }
    
    if rs_percentile < min_rs_percentile:
        return {
            "signal_ok": False,
            "score": 0.0,
            "reason": "low_rs",
            "strategy": "breakout",
        }
    
    # 점수 계산 (0~100)
    # RS, volume surge 기반
    score = (
        rs_percentile * 0.5 +  # RS 50%
        min(volume_ratio * 10, 30) +  # Volume 최대 30점
        (20 if breakout_confirmed else 0)  # Breakout 확정 20점
    )
    
    return {
        "signal_ok": True,
        "score": min(score, 100.0),
        "reason": "breakout_confirmed",
        "strategy": "breakout",
        "volume_ratio": volume_ratio,
        "rs_percentile": rs_percentile,
    }


def score_breakout(features: Dict[str, float], rs_percentile: float = 0.0) -> float:
    """
    Breakout 전략 점수 (간단 버전)
    
    Returns:
        0~100 점수
    """
    result = check_breakout_signal(features, rs_percentile=rs_percentile)
    return result.get("score", 0.0)
