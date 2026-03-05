"""
Pullback Entry Signal
PB1 풀백 전략 신호

조건:
1. 20일 고점 대비 3~18% 풀백
2. 현재가 > MA20
3. 상승 추세 확인
"""
from __future__ import annotations

import math
from typing import Dict, Optional

import pandas as pd


def pullback_signal(df: pd.DataFrame) -> Optional[Dict[str, any]]:
    """
    Pullback 매수 신호 생성
    
    Args:
        df: OHLCV 데이터프레임 (date, open, high, low, close, volume)
    
    Returns:
        Dict with signal data or None if no signal
        {
            "strategy": "pullback",
            "price": float,
            "pullback_pct": float,
            "ma20": float,
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
        
        # 이동평균 계산
        ma20 = df["close"].rolling(20).mean().iloc[-1]
        if math.isnan(ma20):
            return None
        ma20 = float(ma20)
        
        # 20일 고점
        high20 = float(df["high"].tail(20).max())
        
        # 풀백 % 계산
        if high20 <= 0:
            return None
        
        pullback_pct = ((high20 - close) / high20) * 100
        
        # 조건 검증
        # 풀백 범위: 3% ~ 18%
        if not (3.0 <= pullback_pct <= 18.0):
            return None
        
        # MA20 위에 있어야 함
        if close < ma20:
            return None
        
        # 양봉 확인 (상승 추세)
        if close <= float(last["open"]):
            return None
        
        # 신뢰도 계산 (풀백이 적을수록, MA20에서 멀어질수록 높음)
        # 최적 풀백: 5~10%
        optimal_pullback_dist = abs(pullback_pct - 7.5)
        confidence = 1.0 - (optimal_pullback_dist / 10.0)
        confidence = max(0.0, min(1.0, confidence))  # 0.0 ~ 1.0
        
        return {
            "strategy": "pullback",
            "price": close,
            "pullback_pct": pullback_pct,
            "ma20": ma20,
            "high20": high20,
            "confidence": confidence,
            "signal_strength": confidence,
        }
        
    except Exception as e:
        return None
