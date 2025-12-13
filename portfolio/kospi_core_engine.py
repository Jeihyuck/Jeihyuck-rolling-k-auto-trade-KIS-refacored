from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from trader import state_manager
from .base_engine import BaseEngine
from strategy.kospi.rebalance import build_target_allocations
from strategy.kospi.signals import execute_rebalance

logger = logging.getLogger(__name__)


class KospiCoreEngine(BaseEngine):
    def __init__(self, capital: float, top_n: int = 50, rebalance_days: int = 7) -> None:
        super().__init__("kospi_core", capital)
        self.top_n = top_n
        self.rebalance_days = rebalance_days
        self._last_rebalance: datetime | None = None

    def _should_rebalance(self) -> bool:
        if self._last_rebalance is None:
            return True
        return datetime.now() - self._last_rebalance >= timedelta(days=self.rebalance_days)

    def rebalance_if_needed(self) -> Dict[str, Any]:
        if not self._should_rebalance():
            return {"status": "skip"}
        targets = build_target_allocations(self.capital, self.top_n)
        fills = execute_rebalance(targets, self.capital, self.tag)
        self._last_rebalance = datetime.now()
        holding, traded = state_manager.load_state(self.name)
        state_manager.save_state(self.name, holding, traded)
        self._log(f"rebalance targets={len(targets)} fills={len(fills)}")
        return {"targets": targets, "fills": fills}

    def trade_loop(self) -> Dict[str, Any]:
        return self.rebalance_if_needed()
