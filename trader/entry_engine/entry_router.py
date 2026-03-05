"""
Entry Router - Multi-Strategy Signal Generation & Filtering
멀티 전략 신호 생성, 필터링, 랭킹
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Any

import pandas as pd

from trader.signals import (
    pullback_signal,
    breakout_signal,
    momentum_signal,
)
from trader.entry_engine.entry_config import (
    ENTRY_WEIGHTS,
    STOP_RULES,
    STRATEGY_REGIME,
    SIGNAL_CONFIG,
)

logger = logging.getLogger(__name__)


def generate_entry_signals(
    symbol: str,
    df: pd.DataFrame,
    market_regime: str = "bull",
    rs_percentile: float = 0.0,
) -> List[Dict[str, Any]]:
    """
    멀티 전략 신호 생성
    
    주어진 종목의 OHLCV 데이터에서 3가지 전략의 신호를 생성합니다.
    
    Args:
        symbol: 종목 코드
        df: OHLCV 데이터프레임
        market_regime: 시장 레짐 ("bull", "sideways", "bear")
        rs_percentile: RS percentile (0~100)
    
    Returns:
        List of signal dicts, each with:
        {
            "symbol": str,
            "strategy": str,
            "price": float,
            "confidence": float,
            "stop_loss_pct": float,
            "position_weight": float,
            ...strategy specific fields...
        }
    """
    signals = []
    
    if df is None or len(df) < 60:
        return signals
    
    # 1. Pullback 신호 (모든 시장)
    if is_strategy_enabled("pullback", market_regime):
        try:
            pb_sig = pullback_signal(df)
            if pb_sig is not None:
                pb_sig.update({
                    "symbol": symbol,
                    "stop_loss_pct": STOP_RULES["pullback"],
                    "position_weight": ENTRY_WEIGHTS["pullback"],
                    "entry_type": "pullback_entry",
                })
                if pb_sig.get("confidence", 0) >= SIGNAL_CONFIG["min_confidence"]:
                    signals.append(pb_sig)
                    logger.debug(
                        f"[ENTRY][PULLBACK] {symbol} price={pb_sig['price']:.0f} "
                        f"pullback={pb_sig.get('pullback_pct', 0):.1f}% confidence={pb_sig['confidence']:.2f}"
                    )
        except Exception as e:
            logger.warning(f"[PULLBACK ERROR] {symbol}: {e}")
    
    # 2. Breakout 신호 (강세장)
    if is_strategy_enabled("breakout", market_regime):
        try:
            bo_sig = breakout_signal(df, rs_percentile=rs_percentile)
            if bo_sig is not None:
                bo_sig.update({
                    "symbol": symbol,
                    "stop_loss_pct": STOP_RULES["breakout"],
                    "position_weight": ENTRY_WEIGHTS["breakout"],
                    "entry_type": "breakout_entry",
                })
                if bo_sig.get("confidence", 0) >= SIGNAL_CONFIG["min_confidence"]:
                    signals.append(bo_sig)
                    logger.debug(
                        f"[ENTRY][BREAKOUT] {symbol} price={bo_sig['price']:.0f} "
                        f"vol_ratio={bo_sig.get('volume_ratio', 0):.2f}x confidence={bo_sig['confidence']:.2f}"
                    )
        except Exception as e:
            logger.warning(f"[BREAKOUT ERROR] {symbol}: {e}")
    
    # 3. Momentum 신호 (강세장)
    if is_strategy_enabled("momentum", market_regime):
        try:
            mm_sig = momentum_signal(df)
            if mm_sig is not None:
                mm_sig.update({
                    "symbol": symbol,
                    "stop_loss_pct": STOP_RULES["momentum"],
                    "position_weight": ENTRY_WEIGHTS["momentum"],
                    "entry_type": "momentum_entry",
                })
                if mm_sig.get("confidence", 0) >= SIGNAL_CONFIG["min_confidence"]:
                    signals.append(mm_sig)
                    logger.debug(
                        f"[ENTRY][MOMENTUM] {symbol} price={mm_sig['price']:.0f} "
                        f"ret_20d={mm_sig.get('return_20d', 0):.1f}% confidence={mm_sig['confidence']:.2f}"
                    )
        except Exception as e:
            logger.warning(f"[MOMENTUM ERROR] {symbol}: {e}")
    
    return signals


def is_strategy_enabled(strategy: str, regime: str) -> bool:
    """
    전략이 현재 시장 레짐에서 활성화되는지 확인
    
    Args:
        strategy: 전략명 ("pullback", "breakout", "momentum")
        regime: 시장 레짐 ("bull", "sideways", "bear")
    
    Returns:
        True if strategy is enabled, False otherwise
    """
    enabled_regimes = STRATEGY_REGIME.get(strategy, [])
    return regime in enabled_regimes


def rank_signals(
    signals: List[Dict[str, Any]],
    ranking_method: str = "confidence",
) -> List[Dict[str, Any]]:
    """
    신호를 신뢰도 또는 다른 지표로 랭킹
    
    Args:
        signals: 신호 리스트
        ranking_method: 랭킹 방식 ("confidence", "strategy_weight", "composite")
    
    Returns:
        Ranked signals (highest score first)
    """
    if not signals:
        return signals
    
    if ranking_method == "confidence":
        # 신뢰도 기반
        ranked = sorted(
            signals,
            key=lambda x: x.get("confidence", 0),
            reverse=True,
        )
    elif ranking_method == "strategy_weight":
        # 전략 가중치 기반
        ranked = sorted(
            signals,
            key=lambda x: x.get("position_weight", 0),
            reverse=True,
        )
    elif ranking_method == "composite":
        # 혼합 (신뢰도 60% + 전략 가중치 40%)
        ranked = sorted(
            signals,
            key=lambda x: (
                x.get("confidence", 0) * 0.6 +
                x.get("position_weight", 0) / min(ENTRY_WEIGHTS.values()) * 0.4
            ),
            reverse=True,
        )
    else:
        ranked = signals
    
    # 점수 추가
    for i, sig in enumerate(ranked):
        sig["rank"] = i + 1
    
    return ranked


def filter_by_regime(
    signals: List[Dict[str, Any]],
    regime: str,
) -> List[Dict[str, Any]]:
    """
    시장 레짐에 맞는 신호만 필터링
    
    Args:
        signals: 신호 리스트
        regime: 시장 레짐
    
    Returns:
        Filtered signals
    """
    filtered = []
    
    for sig in signals:
        strategy = sig.get("strategy", "")
        if is_strategy_enabled(strategy, regime):
            filtered.append(sig)
    
    return filtered


def get_stop_loss(strategy: str) -> float:
    """
    전략별 손절 수익률 반환
    
    Args:
        strategy: 전략명
    
    Returns:
        Stop loss percentage (0.0~1.0)
    """
    return STOP_RULES.get(strategy, 0.07)


def get_position_weight(strategy: str) -> float:
    """
    전략별 포지션 가중치 반환
    
    Args:
        strategy: 전략명
    
    Returns:
        Position weight (0.0~1.0)
    """
    return ENTRY_WEIGHTS.get(strategy, 0.33)


def get_entry_strength(signal: Dict[str, Any]) -> float:
    """
    신호의 진입 강도 계산 (최종 점수)
    
    1. 신뢰도 (confidence)
    2. 신호 강도 (signal_strength)
    3. 전략 가중치 (position_weight)
    
    Args:
        signal: 신호
    
    Returns:
        Composite score (0.0~1.0)
    """
    confidence = signal.get("confidence", 0.5)
    signal_strength = signal.get("signal_strength", 0.5)
    position_weight = signal.get("position_weight", 1.0/3)
    
    # 정규화
    norm_weight = position_weight / sum(ENTRY_WEIGHTS.values())
    
    # 혼합 점수
    strength = (
        confidence * 0.4 +
        signal_strength * 0.4 +
        norm_weight * 0.2
    )
    
    return min(1.0, strength)
