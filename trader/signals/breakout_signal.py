"""
Breakout Entry Signal
돌파/폭등 전략 신호 (Minervini VCP)

조건:
1. 20일 고점 돌파
2. 거래량 1.5배 스파이크
3. RS 강세 (선택)
"""
from __future__ import annotations

import math
from typing import Dict, Optional

import pandas as pd


def breakout_signal(
    df: pd.DataFrame,
    min_volume_mult: float = 1.5,
    rs_percentile: float = 0.0,
) -> Optional[Dict[str, any]]:
    """
    Breakout 매수 신호 생성
    
    Args:
        df: OHLCV 데이터프레임 (date, open, high, low, close, volume)
        min_volume_mult: 최소 거래량 배수 (default: 1.5)
        rs_percentile: RS percentile (0~100, 0이면 체크 안 함)
    
    Returns:
        Dict with signal data or None if no signal
        {
            "strategy": "breakout",
            "price": float,
            "volume_ratio": float,
            "pivot": float,
            "confidence": float (0.0~1.0)
        }
    """
    
    # 최소 데이터 확인
    if df is None or len(df) < 20:
        return None
    
    df = df.sort_values("date")
    
    try:
        last = df.iloc[-1]
        close = float(last["close"])
        volume = float(last["volume"])
        
        # 20일 고점 (어제까지)
        pivot = float(df["high"].iloc[:-1].tail(20).max())
        
        # 거래량 평균
        volume_ma20 = df["volume"].rolling(20).mean().iloc[-1]
        if math.isnan(volume_ma20) or volume_ma20 <= 0:
            return None
        volume_ma20 = float(volume_ma20)
        
        # 거래량 배수
        volume_ratio = volume / volume_ma20
        
        # 조건 1: 20일 고점 돌파
        if close <= pivot:
            return None
        
        # 조건 2: 거래량 스파이크
        if volume_ratio < min_volume_mult:
            return None
        
        # 조건 3: RS 체크 (선택)
        if rs_percentile > 0 and rs_percentile < 70:
            return None
        
        # 신뢰도 계산
        # 거래량이 많을수록, 돌파폭이 클수록 높음
        breakout_pct = ((close - pivot) / pivot) * 100
        volume_score = min(volume_ratio / 3.0, 1.0)  # 3배 이상이면 만점
        breakout_score = min(breakout_pct / 2.0, 1.0)  # 2% 이상이면 만점
        
        confidence = (volume_score * 0.6 + breakout_score * 0.4)
        confidence = max(0.0, min(1.0, confidence))
        
        return {
            "strategy": "breakout",
            "price": close,
            "volume_ratio": volume_ratio,
            "pivot": pivot,
            "breakout_pct": breakout_pct,
            "confidence": confidence,
            "signal_strength": confidence,
        }
        
    except Exception as e:
        return None
