"""
Entry Strategy Comparator
3가지 전략의 성과 비교
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class EntryStrategyComparator:
    """
    Pullback, Breakout, Momentum 전략 성과 비교
    """
    
    def __init__(self, lookback_days: int = 30):
        """
        Args:
            lookback_days: 진입 후 수익률 평가 기간 (일)
        """
        self.lookback_days = lookback_days
        self.results = []
    
    def evaluate_strategy_performance(
        self,
        signals: List[Dict],
        ohlcv_data: Dict[str, pd.DataFrame],
    ) -> Dict[str, any]:
        """
        각 전략별 성과 평가
        
        Args:
            signals: 진입 신호 리스트
            ohlcv_data: {symbol: ohlcv_df}
        
        Returns:
            Performance metrics by strategy
        """
        results_by_strategy = {}
        
        for strategy in ["pullback", "breakout", "momentum"]:
            strategy_signals = [s for s in signals if s.get("strategy") == strategy]
            
            if not strategy_signals:
                results_by_strategy[strategy] = {
                    "count": 0,
                    "avg_return": 0.0,
                    "win_rate": 0.0,
                    "max_gain": 0.0,
                    "max_loss": 0.0,
                }
                continue
            
            returns = []
            winners = 0
            
            for sig in strategy_signals:
                symbol = sig.get("symbol")
                entry_price = sig.get("price")
                
                if symbol not in ohlcv_data or entry_price <= 0:
                    continue
                
                df = ohlcv_data[symbol]
                entry_idx = len(df) - 1
                
                # lookback_days 후 수익률 계산
                if entry_idx + self.lookback_days < len(df):
                    future_price = float(df.iloc[entry_idx + self.lookback_days]["close"])
                else:
                    future_price = float(df.iloc[-1]["close"])
                
                ret = ((future_price - entry_price) / entry_price) * 100
                returns.append(ret)
                
                if ret > 0:
                    winners += 1
            
            if returns:
                results_by_strategy[strategy] = {
                    "count": len(strategy_signals),
                    "avg_return": np.mean(returns),
                    "win_rate": winners / len(returns),
                    "max_gain": max(returns),
                    "max_loss": min(returns),
                    "sharpe_ratio": self._calculate_sharpe(returns),
                }
            else:
                results_by_strategy[strategy] = {
                    "count": 0,
                    "avg_return": 0.0,
                    "win_rate": 0.0,
                    "max_gain": 0.0,
                    "max_loss": 0.0,
                }
        
        return results_by_strategy
    
    def _calculate_sharpe(self, returns: List[float], risk_free_rate: float = 0.02) -> float:
        """
        Sharpe Ratio 계산
        """
        if len(returns) < 2:
            return 0.0
        
        annual_return = np.mean(returns) * 252 / 100
        annual_std = np.std(returns) * np.sqrt(252) / 100
        
        if annual_std <= 0:
            return 0.0
        
        return (annual_return - risk_free_rate) / annual_std
    
    def compare_strategies(
        self,
        signals: List[Dict],
        ohlcv_data: Dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        """
        전략별 성과를 표로 정리
        
        Args:
            signals: 진입 신호
            ohlcv_data: OHLCV 데이터
        
        Returns:
            Performance comparison table
        """
        results = self.evaluate_strategy_performance(signals, ohlcv_data)
        
        df = pd.DataFrame(results).T
        df.index.name = "Strategy"
        
        logger.info(f"\n{df}\n")
        
        return df
    
    def get_backtest_summary(
        self,
        results: Dict[str, any],
    ) -> str:
        """
        백테스트 결과 요약
        """
        summary = "[BACKTEST SUMMARY]\n"
        summary += "=" * 50 + "\n"
        
        for strategy, metrics in results.items():
            summary += f"\n{strategy.upper()}\n"
            summary += f"  Signal Count: {metrics['count']}\n"
            summary += f"  Avg Return:  {metrics['avg_return']:.2f}%\n"
            summary += f"  Win Rate:    {metrics['win_rate']:.1%}\n"
            summary += f"  Max Gain:    {metrics['max_gain']:.2f}%\n"
            summary += f"  Max Loss:    {metrics['max_loss']:.2f}%\n"
            summary += f"  Sharpe:      {metrics.get('sharpe_ratio', 0):.2f}\n"
        
        return summary
