from __future__ import annotations

from typing import Any, Dict

from strategy.base import BaseStrategy


class VolatilityStrategy(BaseStrategy):
    name = "volatility"
    sid = 5

    def update_state(self, market_data: Dict[str, Any]) -> None:
        return

    def should_enter(self, market_data: Dict[str, Any], portfolio_state: Dict[str, Any]) -> bool:
        return False

    def compute_entry(
        self, market_data: Dict[str, Any], portfolio_state: Dict[str, Any]
    ) -> Dict[str, Any] | None:
        return None

    def should_exit(
        self, position: Dict[str, Any], market_data: Dict[str, Any], portfolio_state: Dict[str, Any]
    ) -> bool:
        return False

    def compute_exit(
        self, position: Dict[str, Any], market_data: Dict[str, Any], portfolio_state: Dict[str, Any]
    ) -> Dict[str, Any] | None:
        return None
