"""
PB-CORE v6: Multi-Strategy Engine
여러 전략을 통합 관리하고 신호를 생성
"""
from __future__ import annotations

import logging
from typing import Dict, List, Any
from dataclasses import dataclass

import pandas as pd

from trader.strategies.pb1_pullback_close import compute_features as compute_pullback_features
from trader.strategies.breakout_strategy import (
    compute_breakout_features,
    check_breakout_signal,
)
from trader.strategies.momentum_strategy import (
    compute_momentum_features,
    check_momentum_signal,
)

logger = logging.getLogger(__name__)


@dataclass
class StrategySignal:
    """전략 신호 데이터 클래스"""
    symbol: str
    strategy: str  # "pullback", "breakout", "momentum"
    score: float  # 0~100
    signal_ok: bool
    reason: str
    features: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "strategy": self.strategy,
            "score": self.score,
            "signal_ok": self.signal_ok,
            "reason": self.reason,
            "features": self.features,
        }


class MultiStrategyEngine:
    """
    Multi-Strategy Engine
    
    Pullback, Breakout, Momentum 전략을 통합 관리
    각 전략의 신호를 생성하고 점수를 계산
    """
    
    def __init__(
        self,
        enable_pullback: bool = True,
        enable_breakout: bool = True,
        enable_momentum: bool = True,
        pullback_config: Dict[str, Any] | None = None,
        breakout_config: Dict[str, Any] | None = None,
        momentum_config: Dict[str, Any] | None = None,
    ):
        self.enable_pullback = enable_pullback
        self.enable_breakout = enable_breakout
        self.enable_momentum = enable_momentum
        
        self.pullback_config = pullback_config or {}
        self.breakout_config = breakout_config or {
            "min_rs_percentile": 80.0,
            "min_volume_mult": 1.5,
        }
        self.momentum_config = momentum_config or {
            "min_volume_mult": 1.2,
            "max_drop_from_high_pct": 3.0,
        }
    
    def generate_signals(
        self,
        symbol: str,
        ohlcv_df: pd.DataFrame,
        rs_percentile: float = 0.0,
        minervini_features: Dict[str, Any] | None = None,
        intraday_price: float | None = None,
    ) -> List[StrategySignal]:
        """
        주어진 종목에 대해 모든 활성화된 전략의 신호 생성
        
        Args:
            symbol: 종목 코드
            ohlcv_df: OHLCV 데이터프레임
            rs_percentile: RS percentile (0~100)
            minervini_features: Minervini 필터 결과 (선택사항)
            intraday_price: 장중 현재가 (Momentum 전략용)
        
        Returns:
            List of StrategySignal
        """
        signals: List[StrategySignal] = []
        
        # Pullback 전략
        if self.enable_pullback:
            try:
                pullback_signal = self._check_pullback(symbol, ohlcv_df, minervini_features)
                if pullback_signal:
                    signals.append(pullback_signal)
            except Exception as e:
                logger.warning("[STRATEGY][PULLBACK] symbol=%s error=%s", symbol, e)
        
        # Breakout 전략
        if self.enable_breakout:
            try:
                breakout_signal = self._check_breakout(symbol, ohlcv_df, rs_percentile)
                if breakout_signal:
                    signals.append(breakout_signal)
            except Exception as e:
                logger.warning("[STRATEGY][BREAKOUT] symbol=%s error=%s", symbol, e)
        
        # Momentum 전략
        if self.enable_momentum:
            try:
                momentum_signal = self._check_momentum(symbol, ohlcv_df, intraday_price)
                if momentum_signal:
                    signals.append(momentum_signal)
            except Exception as e:
                logger.warning("[STRATEGY][MOMENTUM] symbol=%s error=%s", symbol, e)
        
        return signals
    
    def _check_pullback(
        self,
        symbol: str,
        ohlcv_df: pd.DataFrame,
        minervini_features: Dict[str, Any] | None = None,
    ) -> StrategySignal | None:
        """Pullback 전략 체크 (기존 PB1 로직 재사용)"""
        try:
            features = compute_pullback_features(ohlcv_df)
            
            # 간단한 조건 체크 (실제로는 PB1 전체 로직 사용)
            pullback_pct = features.get("pullback_pct", 0.0)
            vol_contraction = features.get("vol_contraction", 1.0)
            
            # 기본 조건
            pullback_ok = 3.0 <= pullback_pct <= 18.0
            vol_ok = vol_contraction <= 1.0
            ma20_ok = features.get("close", 0.0) >= features.get("ma20", 0.0)
            
            signal_ok = pullback_ok and vol_ok and ma20_ok
            
            # 점수 계산 (간단 버전)
            score = 0.0
            if signal_ok:
                score = min(
                    70.0 +  # 기본 점수
                    (10 - pullback_pct) * 2 +  # pullback이 작을수록 높은 점수
                    (1.0 - vol_contraction) * 20,  # 변동성 축소가 강할수록 높은 점수
                    100.0
                )
            
            reason = "pullback_confirmed" if signal_ok else "conditions_not_met"
            
            return StrategySignal(
                symbol=symbol,
                strategy="pullback",
                score=score,
                signal_ok=signal_ok,
                reason=reason,
                features=features,
            )
        except Exception as e:
            logger.warning("[PULLBACK] symbol=%s error=%s", symbol, e)
            return None
    
    def _check_breakout(
        self,
        symbol: str,
        ohlcv_df: pd.DataFrame,
        rs_percentile: float,
    ) -> StrategySignal | None:
        """Breakout 전략 체크"""
        try:
            features = compute_breakout_features(ohlcv_df)
            result = check_breakout_signal(
                features,
                rs_percentile=rs_percentile,
                **self.breakout_config,
            )
            
            return StrategySignal(
                symbol=symbol,
                strategy=result["strategy"],
                score=result["score"],
                signal_ok=result["signal_ok"],
                reason=result["reason"],
                features=features,
            )
        except Exception as e:
            logger.warning("[BREAKOUT] symbol=%s error=%s", symbol, e)
            return None
    
    def _check_momentum(
        self,
        symbol: str,
        ohlcv_df: pd.DataFrame,
        intraday_price: float | None = None,
    ) -> StrategySignal | None:
        """Momentum 전략 체크"""
        try:
            features = compute_momentum_features(ohlcv_df, intraday_price)
            result = check_momentum_signal(features, **self.momentum_config)
            
            return StrategySignal(
                symbol=symbol,
                strategy=result["strategy"],
                score=result["score"],
                signal_ok=result["signal_ok"],
                reason=result["reason"],
                features=features,
            )
        except Exception as e:
            logger.warning("[MOMENTUM] symbol=%s error=%s", symbol, e)
            return None


def generate_multi_strategy_signals(
    symbol: str,
    ohlcv_df: pd.DataFrame,
    rs_percentile: float = 0.0,
    minervini_features: Dict[str, Any] | None = None,
    regime: str = "bull",
    intraday_price: float | None = None,
) -> List[StrategySignal]:
    """
    편의 함수: 시장 레짐에 따라 전략을 활성화하고 신호 생성
    
    Args:
        symbol: 종목 코드
        ohlcv_df: OHLCV 데이터프레임
        rs_percentile: RS percentile
        minervini_features: Minervini 필터 결과
        regime: 시장 레짐 ("bull", "sideways", "bear")
        intraday_price: 장중 현재가
    
    Returns:
        List of StrategySignal
    """
    # 시장 레짐에 따라 전략 활성화
    if regime == "bull":
        enable_pullback = True
        enable_breakout = True
        enable_momentum = True
    elif regime == "sideways":
        enable_pullback = True
        enable_breakout = False
        enable_momentum = True
    elif regime == "bear":
        enable_pullback = True
        enable_breakout = False
        enable_momentum = False
    else:
        # 기본값: 모든 전략 활성화
        enable_pullback = True
        enable_breakout = True
        enable_momentum = True
    
    engine = MultiStrategyEngine(
        enable_pullback=enable_pullback,
        enable_breakout=enable_breakout,
        enable_momentum=enable_momentum,
    )
    
    return engine.generate_signals(
        symbol=symbol,
        ohlcv_df=ohlcv_df,
        rs_percentile=rs_percentile,
        minervini_features=minervini_features,
        intraday_price=intraday_price,
    )
