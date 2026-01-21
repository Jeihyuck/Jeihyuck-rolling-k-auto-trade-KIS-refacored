from __future__ import annotations

from typing import Any, Dict

from .base import BaseStrategy


class MeanReversionStrategy(BaseStrategy):
    """밴드 하단 진입 / 평균 회귀 전략."""

    name = "mean_reversion"

    def should_enter(self, symbol: str, market_data: Dict[str, Any]) -> bool:
        price = float(market_data.get("price") or 0.0)
        mean = float(market_data.get("mean_price") or market_data.get("ma_slow") or 0.0)
        fast = float(market_data.get("ma_fast") or 0.0)
        band_width_pct = self._pct_value("band_width_pct", 2.0)
        if price <= 0 or mean <= 0 or fast <= 0:
            return False
        lower_band = mean * (1 - band_width_pct / 100.0)
        if not (price <= lower_band and fast < mean):
            return False
        return True

    def compute_entry_price(self, symbol: str, market_data: Dict[str, Any]) -> float:
        return float(market_data.get("price") or 0.0)

    def compute_stop_loss(self, position_state: Dict[str, Any], market_data: Dict[str, Any]) -> float:
        avg_price = float(position_state.get("avg_price") or market_data.get("price") or 0.0)
        return self._stop_loss_price(avg_price, self._pct_value("stop_loss_pct", 2.5))

    def compute_take_profit(self, position_state: Dict[str, Any], market_data: Dict[str, Any]) -> float:
        mean = float(market_data.get("mean_price") or market_data.get("ma_slow") or 0.0)
        if mean > 0:
            return mean
        avg_price = float(position_state.get("avg_price") or market_data.get("price") or 0.0)
        return self._take_profit_price(avg_price, self._pct_value("profit_target_pct", 2.0))

    def should_exit(self, position_state: Dict[str, Any], market_data: Dict[str, Any]) -> bool:
        price = float(market_data.get("price") or 0.0)
        if price <= 0:
            return False
        stop = self.compute_stop_loss(position_state, market_data)
        target = self.compute_take_profit(position_state, market_data)
        return price <= stop or price >= target
