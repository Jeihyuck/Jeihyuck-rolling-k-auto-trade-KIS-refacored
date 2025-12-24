from __future__ import annotations

from typing import Any, Dict

from .base import BaseStrategy


class BreakoutStrategy(BaseStrategy):
    """신고가 돌파 전략."""

    name = "breakout"

    def should_enter(self, symbol: str, market_data: Dict[str, Any]) -> bool:
        price = float(market_data.get("price") or 0.0)
        recent_high = float(market_data.get("recent_high") or market_data.get("high") or 0.0)
        if price <= 0 or recent_high <= 0:
            return False
        k_factor = self._pct_value("k_factor", 0.5)
        breakout_level = recent_high * (1 + k_factor / 100.0)
        return price >= breakout_level

    def compute_entry_price(self, symbol: str, market_data: Dict[str, Any]) -> float:
        price = float(market_data.get("price") or 0.0)
        recent_high = float(market_data.get("recent_high") or market_data.get("high") or 0.0)
        k_factor = self._pct_value("k_factor", 0.5)
        target = recent_high * (1 + k_factor / 100.0) if recent_high > 0 else price
        return float(target or price)

    def compute_stop_loss(self, position_state: Dict[str, Any], market_data: Dict[str, Any]) -> float:
        avg_price = float(position_state.get("avg_price") or market_data.get("price") or 0.0)
        return self._stop_loss_price(avg_price, self._pct_value("stop_loss_pct", 5.0))

    def compute_take_profit(self, position_state: Dict[str, Any], market_data: Dict[str, Any]) -> float:
        avg_price = float(position_state.get("avg_price") or market_data.get("price") or 0.0)
        return self._take_profit_price(avg_price, self._pct_value("profit_target_pct", 3.0))

    def should_exit(self, position_state: Dict[str, Any], market_data: Dict[str, Any]) -> bool:
        price = float(market_data.get("price") or 0.0)
        if price <= 0:
            return False
        stop = self.compute_stop_loss(position_state, market_data)
        target = self.compute_take_profit(position_state, market_data)
        return price <= stop or price >= target
