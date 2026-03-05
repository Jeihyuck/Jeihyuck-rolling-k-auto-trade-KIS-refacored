"""
Momentum Entry Signal
모멘텀 /고속 상승 전략 신호

조건:
1. 20일 수익률 > 15%
2. 60일 수익률 > 25%
3. 거래량 증가
"""
from __future__ import annotations

import math
from typing import Dict, Optional

import pandas as pd


def momentum_signal(df: pd.DataFrame) -> Optional[Dict[str, any]]:
    """
    Momentum 매수 신호 생성
    
    Args:
        df: OHLCV 데이터프레임 (date, open, high, low, close, volume)
    
    Returns:
        Dict with signal data or None if no signal
        {
            "strategy": "momentum",
            "price": float,
            "return_20d": float,
            "return_60d": float,
            "confidence": float (0.0~1.0)
        }
    """
    
    # 최소 데이터 확인
    if df is None or len(df) < 60:
        return None
    
    df = df.sort_values("date")
    
    try:
        last = df.iloc[-1]
        current_price = float(last["close"])
        
        # 과거 종가
        price_20d_ago = float(df["close"].iloc[-20])
        price_60d_ago = float(df["close"].iloc[-60])
        
        # 수익률 계산
        ret_20d = ((current_price - price_20d_ago) / price_20d_ago) * 100
        ret_60d = ((current_price - price_60d_ago) / price_60d_ago) * 100
        
        # 조건 1: 20일 수익률 > 15%
        if ret_20d <= 15.0:
            return None
        
        # 조건 2: 60일 수익률 > 25%
        if ret_60d <= 25.0:
            return None
        
        # 거래량 확인 (선택)
        volume_ma20 = df["volume"].rolling(20).mean().iloc[-1]
        current_volume = float(last["volume"])
        
        volume_ratio = current_volume / volume_ma20 if volume_ma20 > 0 else 0.0
        
        # 신뢰도 계산
        # 수익률이 높을수록, 거래량이 많을수록 높음
        ret_20_score = min(ret_20d / 30.0, 1.0)  # 30% 이상이면 만점
        ret_60_score = min(ret_60d / 50.0, 1.0)  # 50% 이상이면 만점
        volume_score = min(volume_ratio / 2.0, 1.0)  # 2배 이상이면 만점
        
        confidence = (ret_20_score * 0.4 + ret_60_score * 0.4 + volume_score * 0.2)
        confidence = max(0.0, min(1.0, confidence))
        
        return {
            "strategy": "momentum",
            "price": current_price,
            "return_20d": ret_20d,
            "return_60d": ret_60d,
            "volume_ratio": volume_ratio,
            "confidence": confidence,
            "signal_strength": confidence,
        }
        
    except Exception as e:
        return None
