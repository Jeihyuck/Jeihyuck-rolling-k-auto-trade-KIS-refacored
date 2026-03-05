"""
PB-CORE v6: Signal Aggregator
여러 전략의 신호를 통합하고 충돌 해결
"""
from __future__ import annotations

import logging
from typing import List, Dict, Any
from collections import defaultdict

from trader.multi_strategy_engine import StrategySignal

logger = logging.getLogger(__name__)


class SignalAggregator:
    """
    Signal Aggregator
    
    같은 종목에 대해 여러 전략이 신호를 생성한 경우 충돌 해결
    가장 높은 점수의 전략을 선택하거나, 가중 평균 등을 사용
    """
    
    def __init__(self, mode: str = "best_score"):
        """
        Args:
            mode: 통합 방식
                - "best_score": 가장 높은 점수의 전략 선택
                - "weighted_avg": 모든 전략의 가중 평균
                - "all": 모든 신호 유지 (별도 처리 필요)
        """
        self.mode = mode
    
    def aggregate(self, signals: List[StrategySignal]) -> List[StrategySignal]:
        """
        신호 통합
        
        Args:
            signals: 모든 전략의 신호 리스트
        
        Returns:
            통합된 신호 리스트 (종목당 하나씩)
        """
        if not signals:
            return []
        
        # 종목별로 그룹화
        grouped: Dict[str, List[StrategySignal]] = defaultdict(list)
        for signal in signals:
            if signal.signal_ok:  # OK 신호만 처리
                grouped[signal.symbol].append(signal)
        
        # 통합 방식에 따라 처리
        if self.mode == "best_score":
            return self._aggregate_best_score(grouped)
        elif self.mode == "weighted_avg":
            return self._aggregate_weighted_avg(grouped)
        elif self.mode == "all":
            return signals  # 모든 신호 그대로 반환
        else:
            logger.warning("[SIGNAL_AGGREGATOR] unknown mode=%s, using best_score", self.mode)
            return self._aggregate_best_score(grouped)
    
    def _aggregate_best_score(
        self,
        grouped: Dict[str, List[StrategySignal]],
    ) -> List[StrategySignal]:
        """가장 높은 점수의 전략 선택"""
        result: List[StrategySignal] = []
        
        for symbol, symbol_signals in grouped.items():
            if not symbol_signals:
                continue
            
            # 점수가 가장 높은 신호 선택
            best_signal = max(symbol_signals, key=lambda s: s.score)
            
            # 디버그 로그
            if len(symbol_signals) > 1:
                strategies = [f"{s.strategy}:{s.score:.1f}" for s in symbol_signals]
                logger.info(
                    "[SIGNAL_AGGREGATOR] symbol=%s strategies=%s selected=%s",
                    symbol,
                    ", ".join(strategies),
                    f"{best_signal.strategy}:{best_signal.score:.1f}",
                )
            
            result.append(best_signal)
        
        return result
    
    def _aggregate_weighted_avg(
        self,
        grouped: Dict[str, List[StrategySignal]],
    ) -> List[StrategySignal]:
        """
        여러 전략의 점수를 가중 평균
        (주의: 실제 거래에서는 전략 특성이 다르므로 단순 평균은 위험할 수 있음)
        """
        result: List[StrategySignal] = []
        
        for symbol, symbol_signals in grouped.items():
            if not symbol_signals:
                continue
            
            # 가중 평균 (단순 평균)
            avg_score = sum(s.score for s in symbol_signals) / len(symbol_signals)
            
            # 대표 신호 선택 (가장 높은 점수)
            best_signal = max(symbol_signals, key=lambda s: s.score)
            
            # 점수만 가중 평균으로 교체
            aggregated = StrategySignal(
                symbol=symbol,
                strategy=f"multi:{best_signal.strategy}",  # 대표 전략 표시
                score=avg_score,
                signal_ok=True,
                reason=f"weighted_avg_of_{len(symbol_signals)}_strategies",
                features=best_signal.features,
            )
            
            logger.info(
                "[SIGNAL_AGGREGATOR] symbol=%s strategies=%d avg_score=%.1f",
                symbol,
                len(symbol_signals),
                avg_score,
            )
            
            result.append(aggregated)
        
        return result


def resolve_signals(signals: List[StrategySignal], mode: str = "best_score") -> List[StrategySignal]:
    """
    편의 함수: 신호 통합
    
    Args:
        signals: 모든 전략의 신호 리스트
        mode: 통합 방식 ("best_score", "weighted_avg", "all")
    
    Returns:
        통합된 신호 리스트
    """
    aggregator = SignalAggregator(mode=mode)
    return aggregator.aggregate(signals)


def group_by_symbol(signals: List[StrategySignal]) -> Dict[str, List[StrategySignal]]:
    """
    편의 함수: 종목별로 신호 그룹화
    
    Returns:
        Dict[symbol, List[StrategySignal]]
    """
    grouped: Dict[str, List[StrategySignal]] = defaultdict(list)
    for signal in signals:
        grouped[signal.symbol].append(signal)
    return dict(grouped)
