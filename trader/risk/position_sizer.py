"""
Position Sizer - 전략별 포지션 사이징

Kelly Criterion, Risk Parity 등 다양한 방식 지원
"""
from __future__ import annotations

import logging
import math
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class PositionSizer:
    """
    포지션 크기 계산 (자본 관리)
    """
    
    def __init__(
        self,
        capital: float,
        strategy_weights: Optional[Dict[str, float]] = None,
        cash_reserve_pct: float = 0.10,
    ):
        """
        Args:
            capital: 총 자본 (원)
            strategy_weights: 전략별 가중치
                {
                    "pullback": 0.12,
                    "breakout": 0.15,
                    "momentum": 0.10,
                }
            cash_reserve_pct: 현금 보유 비율 (0~1)
        """
        self.capital = capital
        self.cash_reserve_pct = cash_reserve_pct
        self.available_capital = capital * (1 - cash_reserve_pct)
        
        if strategy_weights is None:
            self.strategy_weights = {
                "pullback": 0.12,
                "breakout": 0.15,
                "momentum": 0.10,
            }
        else:
            self.strategy_weights = strategy_weights
    
    def calculate_position_size_by_strategy(
        self,
        strategy: str,
        entry_price: float,
    ) -> float:
        """
        전략별 포지션 크기 계산 (주식 수)
        
        Args:
            strategy: 전략명
            entry_price: 진입가
        
        Returns:
            Position size (주식 수)
        """
        if entry_price <= 0:
            return 0.0
        
        weight = self.strategy_weights.get(strategy, 0.10)
        position_budget = self.available_capital * weight
        
        shares = position_budget / entry_price
        
        return int(shares)
    
    def calculate_position_size_by_kelly(
        self,
        win_rate: float,
        avg_win: float,
        avg_loss: float,
        entry_price: float,
    ) -> float:
        """
        Kelly Criterion 기반 포지션 크기 (고급)
        
        Kelly % = (win_rate * avg_win - (1 - win_rate) * avg_loss) / avg_win
        
        Args:
            win_rate: 승률 (0~1)
            avg_win: 평균 수익 (%)
            avg_loss: 평균 손실 (%)
            entry_price: 진입가
        
        Returns:
            Position size (주식 수)
        """
        if entry_price <= 0 or avg_win <= 0 or avg_loss <= 0:
            return 0.0
        
        # Kelly fraction 계산
        kelly_fraction = (
            (win_rate * avg_win - (1 - win_rate) * avg_loss) / avg_win
        )
        
        # 안전성을 위해 절반만 사용 (Half Kelly)
        half_kelly = kelly_fraction / 2
        half_kelly = max(0.0, min(0.25, half_kelly))  # 최대 25% 포지션
        
        position_budget = self.available_capital * half_kelly
        shares = position_budget / entry_price
        
        logger.debug(
            f"[POSITION_SIZER] Kelly: {kelly_fraction:.2%}, Half-Kelly: {half_kelly:.2%}, "
            f"Shares: {int(shares)}"
        )
        
        return int(shares)
    
    def calculate_position_size_by_risk(
        self,
        entry_price: float,
        stop_loss_price: float,
        risk_pct: float = 0.02,
    ) -> float:
        """
        리스크 기반 포지션 크기
        
        포지션 크기 = (자본 * 리스크%) / (진입가 - 손절가)
        
        Args:
            entry_price: 진입가
            stop_loss_price: 손절 가격
            risk_pct: 1거래 최대 손실 비율 (0~1)
        
        Returns:
            Position size (주식 수)
        """
        if entry_price <= 0 or stop_loss_price < 0:
            return 0.0
        
        risk_amount = entry_price - stop_loss_price
        
        if risk_amount <= 0:
            return 0.0
        
        max_loss = self.capital * risk_pct
        shares = max_loss / risk_amount
        
        return int(shares)
    
    def calculate_stop_loss_price(
        self,
        entry_price: float,
        strategy: str,
        stop_loss_pct: Optional[float] = None,
    ) -> float:
        """
        손절 가격 계산
        
        Args:
            entry_price: 진입가
            strategy: 전략명
            stop_loss_pct: 손절 비율 (없으면 전략별 기본값)
        
        Returns:
            Stop loss price
        """
        if stop_loss_pct is None:
            # 전략별 기본 손절률
            stop_loss_pct = {
                "pullback": 0.07,
                "breakout": 0.05,
                "momentum": 0.08,
            }.get(strategy, 0.07)
        
        stop_loss_price = entry_price * (1 - stop_loss_pct)
        
        return stop_loss_price
    
    def calculate_take_profit_price(
        self,
        entry_price: float,
        strategy: str = "default",
        target_ratio: float = 2.0,
        stop_loss_pct: float = 0.07,
    ) -> Dict[str, float]:
        """
        목표 가격 계산 (Risk:Reward ratio 기반)
        
        Args:
            entry_price: 진입가
            strategy: 전략명
            target_ratio: Risk:Reward 비율 (기본 2:1)
            stop_loss_pct: 손절 비율
        
        Returns:
            {
                "tp1": first target,
                "tp2": second target,
                "tp3": final target,
            }
        """
        risk_amount = entry_price * stop_loss_pct
        
        # 3단계 익절
        tp1 = entry_price + (risk_amount * target_ratio * 0.5)
        tp2 = entry_price + (risk_amount * target_ratio)
        tp3 = entry_price + (risk_amount * target_ratio * 1.5)
        
        if strategy == "momentum":
            # 모멘텀은 더 큰 목표
            tp3 = entry_price + (risk_amount * target_ratio * 2.0)
        
        return {
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
        }
    
    def get_position_summary(
        self,
        symbol: str,
        entry_price: float,
        strategy: str,
        shares: int,
    ) -> Dict[str, float]:
        """
        포지션 정보 요약
        
        Args:
            symbol: 종목 코드
            entry_price: 진입가
            strategy: 전략명
            shares: 주식 수
        
        Returns:
            Position summary dictionary
        """
        stop_loss_pct = {
            "pullback": 0.07,
            "breakout": 0.05,
            "momentum": 0.08,
        }.get(strategy, 0.07)
        
        stop_loss_price = self.calculate_stop_loss_price(
            entry_price, strategy, stop_loss_pct
        )
        
        tp_prices = self.calculate_take_profit_price(
            entry_price, strategy
        )
        
        position_amount = entry_price * shares
        max_loss = (entry_price - stop_loss_price) * shares
        potential_gain_tp3 = (tp_prices["tp3"] - entry_price) * shares
        
        return {
            "symbol": symbol,
            "strategy": strategy,
            "entry_price": entry_price,
            "shares": shares,
            "position_amount": position_amount,
            "stop_loss_price": stop_loss_price,
            "stop_loss_pct": stop_loss_pct,
            "max_loss": max_loss,
            "tp1": tp_prices["tp1"],
            "tp2": tp_prices["tp2"],
            "tp3": tp_prices["tp3"],
            "potential_gain_tp3": potential_gain_tp3,
            "risk_reward_ratio": potential_gain_tp3 / max_loss if max_loss > 0 else 0.0,
        }
