"""
PB-CORE v6: Market Regime Engine
시장 레짐 감지 및 전략 활성화 관리
"""
from __future__ import annotations

import logging
from typing import Dict, Any, Tuple
from dataclasses import dataclass
from datetime import date

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class RegimeState:
    """시장 레짐 상태"""
    regime: str  # "bull", "sideways", "bear"
    confidence: float  # 0.0 ~ 1.0
    reason: str
    index_price: float
    ma50: float
    ma200: float
    trend_strength: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "regime": self.regime,
            "confidence": self.confidence,
            "reason": self.reason,
            "index_price": self.index_price,
            "ma50": self.ma50,
            "ma200": self.ma200,
            "trend_strength": self.trend_strength,
        }


class MarketRegimeEngine:
    """
    Market Regime Engine
    
    시장 상황을 감지하고 전략 활성화를 결정
    """
    
    def __init__(
        self,
        index_symbol: str = "229200",  # KOSPI200 ETF
        bull_threshold: float = 1.02,  # MA50 대비 2% 이상
        bear_threshold: float = 0.98,  # MA200 대비 2% 이하
    ):
        self.index_symbol = index_symbol
        self.bull_threshold = bull_threshold
        self.bear_threshold = bear_threshold
    
    def detect_regime(
        self,
        index_df: pd.DataFrame,
    ) -> RegimeState:
        """
        시장 레짐 감지
        
        Args:
            index_df: 지수 OHLCV 데이터프레임
        
        Returns:
            RegimeState
        """
        if len(index_df) < 200:
            logger.warning("[REGIME] insufficient data, defaulting to sideways")
            return RegimeState(
                regime="sideways",
                confidence=0.5,
                reason="insufficient_data",
                index_price=0.0,
                ma50=0.0,
                ma200=0.0,
                trend_strength=0.0,
            )
        
        index_df = index_df.sort_values("date")
        
        # 이동평균 계산
        close = index_df["close"]
        ma50 = close.rolling(50).mean()
        ma200 = close.rolling(200).mean()
        
        last_price = float(close.iloc[-1])
        last_ma50 = float(ma50.iloc[-1])
        last_ma200 = float(ma200.iloc[-1])
        
        # 추세 강도 (현재가 / MA200)
        trend_strength = last_price / last_ma200 if last_ma200 > 0 else 1.0
        
        # 레짐 판정
        regime, confidence, reason = self._classify_regime(
            last_price,
            last_ma50,
            last_ma200,
            trend_strength,
        )
        
        logger.info(
            "[REGIME] regime=%s confidence=%.2f price=%.2f ma50=%.2f ma200=%.2f trend=%.3f",
            regime,
            confidence,
            last_price,
            last_ma50,
            last_ma200,
            trend_strength,
        )
        
        return RegimeState(
            regime=regime,
            confidence=confidence,
            reason=reason,
            index_price=last_price,
            ma50=last_ma50,
            ma200=last_ma200,
            trend_strength=trend_strength,
        )
    
    def _classify_regime(
        self,
        price: float,
        ma50: float,
        ma200: float,
        trend_strength: float,
    ) -> Tuple[str, float, str]:
        """
        레짐 분류 로직
        
        Returns:
            (regime, confidence, reason)
        """
        # Bull: price > MA50 && MA50 > MA200
        if price > ma50 and ma50 > ma200:
            # 강도에 따라 신뢰도 결정
            if trend_strength > self.bull_threshold:
                return "bull", 0.9, "strong_uptrend"
            else:
                return "bull", 0.7, "uptrend"
        
        # Bear: price < MA200 && MA50 < MA200
        if price < ma200 and ma50 < ma200:
            # 강도에 따라 신뢰도 결정
            if trend_strength < self.bear_threshold:
                return "bear", 0.9, "strong_downtrend"
            else:
                return "bear", 0.7, "downtrend"
        
        # Sideways: 그 외 모든 경우
        return "sideways", 0.6, "range_bound"
    
    def get_enabled_strategies(self, regime: str) -> Dict[str, bool]:
        """
        레짐에 따른 전략 활성화 맵
        
        Args:
            regime: "bull", "sideways", "bear"
        
        Returns:
            Dict[strategy_name, enabled]
        """
        if regime == "bull":
            return {
                "pullback": True,
                "breakout": True,
                "momentum": True,
            }
        elif regime == "sideways":
            return {
                "pullback": True,
                "breakout": False,  # 횡보장에서는 돌파 위험
                "momentum": True,
            }
        elif regime == "bear":
            return {
                "pullback": True,  # 리바운드 노림
                "breakout": False,
                "momentum": False,
            }
        else:
            # 기본값: 모든 전략 활성화
            return {
                "pullback": True,
                "breakout": True,
                "momentum": True,
            }


def detect_market_regime(
    index_df: pd.DataFrame,
    index_symbol: str = "229200",
) -> str:
    """
    편의 함수: 시장 레짐 감지
    
    Args:
        index_df: 지수 OHLCV 데이터프레임
        index_symbol: 지수 심볼
    
    Returns:
        regime: "bull", "sideways", "bear"
    """
    engine = MarketRegimeEngine(index_symbol=index_symbol)
    state = engine.detect_regime(index_df)
    return state.regime


def get_strategy_config_for_regime(regime: str) -> Dict[str, bool]:
    """
    편의 함수: 레짐에 따른 전략 활성화
    
    Args:
        regime: "bull", "sideways", "bear"
    
    Returns:
        Dict[strategy_name, enabled]
    """
    engine = MarketRegimeEngine()
    return engine.get_enabled_strategies(regime)
