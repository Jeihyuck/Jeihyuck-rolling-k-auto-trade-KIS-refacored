from __future__ import annotations

from typing import Any, Dict

from .base import BaseStrategy


class MomentumStrategy(BaseStrategy):
    """단기 모멘텀 교차 기반 전략."""

    name = "momentum"

    def should_enter(self, symbol: str, market_data: Dict[str, Any]) -> bool:
        price = float(market_data.get("price") or 0.0)
        vwap = float(market_data.get("vwap") or 0.0)
        fast = float(market_data.get("ma_fast") or 0.0)
        slow = float(market_data.get("ma_slow") or 0.0)
        prev_close = float(market_data.get("prev_close") or 0.0)
        min_mom = self._pct_value("min_momentum_pct", 0.5)
        if price <= 0 or fast <= 0 or slow <= 0 or prev_close <= 0:
            return False
        momentum_pct = (price - prev_close) / prev_close * 100
        return fast > slow and price >= vwap and momentum_pct >= min_mom

    def compute_entry_price(self, symbol: str, market_data: Dict[str, Any]) -> float:
        return float(market_data.get("price") or 0.0)

    def compute_stop_loss(self, position_state: Dict[str, Any], market_data: Dict[str, Any]) -> float:
        avg_price = float(position_state.get("avg_price") or market_data.get("price") or 0.0)
        return self._stop_loss_price(avg_price, self._pct_value("stop_loss_pct", 3.0))

    def compute_take_profit(self, position_state: Dict[str, Any], market_data: Dict[str, Any]) -> float:
        avg_price = float(position_state.get("avg_price") or market_data.get("price") or 0.0)
        return self._take_profit_price(avg_price, self._pct_value("profit_target_pct", 2.5))

    def should_exit(self, position_state: Dict[str, Any], market_data: Dict[str, Any]) -> bool:
        price = float(market_data.get("price") or 0.0)
        if price <= 0:
            return False
        stop = self.compute_stop_loss(position_state, market_data)
        target = self.compute_take_profit(position_state, market_data)
        vwap = float(market_data.get("vwap") or 0.0)
        lost_momentum = vwap > 0 and price < vwap
        return price <= stop or price >= target or lost_momentum
