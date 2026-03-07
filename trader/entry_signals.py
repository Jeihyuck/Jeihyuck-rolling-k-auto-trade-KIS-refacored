from __future__ import annotations

import pandas as pd


def breakout_signal(df: pd.DataFrame) -> bool:
    """
    Breakout 신호
    - 최근 종가가 20일 고점 돌파
    - 거래량이 20일 평균 거래량의 1.5배 이상
    """
    if len(df) < 21:
        return False
    
    high20 = df['high'].rolling(20).max().iloc[-2]  # 전일까지의 20일 고점
    vol_avg = df['volume'].rolling(20).mean().iloc[-1]
    
    current_close = df['close'].iloc[-1]
    current_volume = df['volume'].iloc[-1]
    
    return (
        current_close > high20
        and current_volume > vol_avg * 1.5
    )


def pullback_signal(df: pd.DataFrame) -> bool:
    """
    Pullback 신호
    - 최근 고점 대비 3% ~ 15% 하락
    - 가격이 20일 이동평균선 위에 있음
    """
    if len(df) < 21:
        return False
    
    ma20 = df['close'].rolling(20).mean().iloc[-1]
    price = df['close'].iloc[-1]
    recent_high = df['high'].rolling(20).max().iloc[-1]
    
    pullback = (price - recent_high) / recent_high
    
    return (
        -0.15 < pullback < -0.03
        and price > ma20
    )


def momentum_signal(df: pd.DataFrame) -> bool:
    """
    Momentum 신호
    - 가격이 50일 이동평균선 위에 있음
    - 최근 3일간 3% 이상 상승
    """
    if len(df) < 51:
        return False
    
    ma50 = df['close'].rolling(50).mean().iloc[-1]
    price = df['close'].iloc[-1]
    
    # 3일 전 대비 수익률
    price_3days_ago = df['close'].iloc[-4]
    pct_change_3d = (price - price_3days_ago) / price_3days_ago
    
    return (
        price > ma50
        and pct_change_3d > 0.03
    )
