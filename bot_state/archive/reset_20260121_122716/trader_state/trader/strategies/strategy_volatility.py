from __future__ import annotations

from typing import Any, Dict

from .base import BaseStrategy


class VolatilityStrategy(BaseStrategy):
    """변동성 돌파 및 확장 전략."""

    name = "volatility"

    def should_enter(self, symbol: str, market_data: Dict[str, Any]) -> bool:
        price = float(market_data.get("price") or 0.0)
        recent_high = float(market_data.get("recent_high") or 0.0)
        fast = float(market_data.get("ma_fast") or 0.0)
        volatility = float(market_data.get("volatility") or 0.0)
        threshold = self._pct_value("volatility_threshold_pct", 1.5)
        if price <= 0 or recent_high <= 0 or fast <= 0:
            return False
        if not (volatility >= threshold and price >= recent_high):
            return False
        if price < fast:
            return False
        return True

    def compute_entry_price(self, symbol: str, market_data: Dict[str, Any]) -> float:
        return float(market_data.get("price") or 0.0)

    def compute_stop_loss(self, position_state: Dict[str, Any], market_data: Dict[str, Any]) -> float:
        avg_price = float(position_state.get("avg_price") or market_data.get("price") or 0.0)
        return self._stop_loss_price(avg_price, self._pct_value("stop_loss_pct", 4.0))

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
