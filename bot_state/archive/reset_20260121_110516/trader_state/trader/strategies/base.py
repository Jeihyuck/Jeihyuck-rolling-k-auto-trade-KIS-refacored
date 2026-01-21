from __future__ import annotations

from typing import Any, Dict


class BaseStrategy:
    """Common strategy interface used by the StrategyManager."""

    name: str = "base"

    def __init__(self, config: Dict[str, Any] | None = None):
        self.config = config or {}
        self.strategy_id = self.config.get("strategy_id")

    def should_enter(self, symbol: str, market_data: Dict[str, Any]) -> bool:
        raise NotImplementedError

    def compute_entry_price(self, symbol: str, market_data: Dict[str, Any]) -> float:
        raise NotImplementedError

    def compute_stop_loss(self, position_state: Dict[str, Any], market_data: Dict[str, Any]) -> float:
        raise NotImplementedError

    def compute_take_profit(self, position_state: Dict[str, Any], market_data: Dict[str, Any]) -> float:
        raise NotImplementedError

    def should_exit(self, position_state: Dict[str, Any], market_data: Dict[str, Any]) -> bool:
        raise NotImplementedError

    def _pct_value(self, key: str, default: float) -> float:
        try:
            return float(self.config.get(key, default))
        except Exception:
            return float(default)

    def _stop_loss_price(self, avg_price: float, pct: float) -> float:
        pct = abs(pct)
        return max(0.0, avg_price * (1 - pct / 100.0))

    def _take_profit_price(self, avg_price: float, pct: float) -> float:
        return avg_price * (1 + pct / 100.0)
