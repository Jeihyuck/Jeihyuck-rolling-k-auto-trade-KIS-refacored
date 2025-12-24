from __future__ import annotations

from typing import Any, Dict

from .base import BaseStrategy


class PullbackStrategy(BaseStrategy):
    """신고가 이후 눌림목 + 반등 전략."""

    name = "pullback"

    def should_enter(self, symbol: str, market_data: Dict[str, Any]) -> bool:
        price = float(market_data.get("price") or 0.0)
        high = float(market_data.get("recent_high") or market_data.get("high") or 0.0)
        recent_low = float(market_data.get("recent_low") or market_data.get("low") or 0.0)
        reversal_price = float(market_data.get("reversal_price") or 0.0)
        if price <= 0 or high <= 0 or recent_low <= 0:
            return False
        drop_pct = (high - price) / high * 100 if high else 0.0
        reversal_buffer = self._pct_value("reversal_buffer_pct", 0.2)
        has_reversal = price >= max(recent_low, reversal_price) * (1 - reversal_buffer / 100.0)
        return drop_pct >= 3.0 and has_reversal

    def compute_entry_price(self, symbol: str, market_data: Dict[str, Any]) -> float:
        reversal_price = float(market_data.get("reversal_price") or 0.0)
        price = float(market_data.get("price") or 0.0)
        return reversal_price or price

    def compute_stop_loss(self, position_state: Dict[str, Any], market_data: Dict[str, Any]) -> float:
        avg_price = float(position_state.get("avg_price") or market_data.get("price") or 0.0)
        return self._stop_loss_price(avg_price, self._pct_value("stop_loss_pct", 4.0))

    def compute_take_profit(self, position_state: Dict[str, Any], market_data: Dict[str, Any]) -> float:
        avg_price = float(position_state.get("avg_price") or market_data.get("price") or 0.0)
        return self._take_profit_price(avg_price, self._pct_value("profit_target_pct", 3.5))

    def should_exit(self, position_state: Dict[str, Any], market_data: Dict[str, Any]) -> bool:
        price = float(market_data.get("price") or 0.0)
        if price <= 0:
            return False
        stop = self.compute_stop_loss(position_state, market_data)
        target = self.compute_take_profit(position_state, market_data)
        # 추가: 반등 실패 시 직전 저점 하회 체크
        recent_low = float(market_data.get("recent_low") or market_data.get("low") or 0.0)
        failed_rebound = recent_low > 0 and price < recent_low
        return price <= stop or price >= target or failed_rebound
