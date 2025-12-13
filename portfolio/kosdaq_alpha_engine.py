from __future__ import annotations

from typing import Any, Dict

from trader import state_manager
from .base_engine import BaseEngine
from strategy.kosdaq.rolling_entry import run_trade_loop


class KosdaqAlphaEngine(BaseEngine):
    def __init__(self, capital: float) -> None:
        super().__init__("kosdaq_alpha", capital)

    def rebalance_if_needed(self) -> Dict[str, Any]:
        # kosdaq engine keeps its own intraday logic; rebalance handled inside legacy loop
        return {"status": "delegated"}

    def trade_loop(self) -> Any:
        self._log("starting legacy KOSDAQ loop")
        result = run_trade_loop(capital_override=self.capital)
        holding, traded = state_manager.load_state(self.name)
        state_manager.save_state(self.name, holding, traded)
        return result
