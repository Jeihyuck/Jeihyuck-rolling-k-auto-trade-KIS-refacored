from __future__ import annotations

import logging
import os
from typing import Any, Dict

from rolling_k_auto_trade_api.best_k_meta_strategy import run_rebalance
from strategy.manager import StrategyManager
from strategy.market_data import build_market_data
import trader.intent_store as intent_store
from trader.config import (
    DAILY_CAPITAL,
    DIAG_ENABLED,
    DIAGNOSTIC_ONLY,
    DISABLE_KOSDAQ_LOOP,
    DISABLE_KOSPI_ENGINE,
    STRATEGY_INTENTS_PATH,
)
from trader.diagnostics_runner import run_diagnostics_once
from trader.intent_executor import IntentExecutor
import trader.state_store as state_store
from trader.core_utils import get_rebalance_anchor_date
from trader.subject_flow import reset_flow_call_count
from .kospi_core_engine import KospiCoreEngine
from .kosdaq_alpha_engine import KosdaqAlphaEngine
from .performance import PerformanceTracker

logger = logging.getLogger(__name__)


class PortfolioManager:
    def __init__(
        self,
        total_capital: float | None = None,
        kospi_ratio: float = 0.6,
        kosdaq_ratio: float = 0.4,
    ) -> None:
        self.total_capital = float(total_capital or DAILY_CAPITAL)
        if kospi_ratio + kosdaq_ratio == 0:
            kospi_ratio, kosdaq_ratio = 0.6, 0.4
        norm = kospi_ratio + kosdaq_ratio
        self.kospi_ratio = kospi_ratio / norm
        self.kosdaq_ratio = kosdaq_ratio / norm
        self.kospi_engine = KospiCoreEngine(capital=self.total_capital * self.kospi_ratio)
        self.kosdaq_engine = KosdaqAlphaEngine(capital=self.total_capital * self.kosdaq_ratio)
        self.performance = PerformanceTracker()
        self.strategy_manager = StrategyManager(total_capital=self.total_capital)
        self.intent_executor = IntentExecutor()
        logger.info(
            "[PORTFOLIO] capital=%s kospi=%.0f%% kosdaq=%.0f%%",
            int(self.total_capital),
            self.kospi_ratio * 100,
            self.kosdaq_ratio * 100,
        )

    def run_once(self) -> Dict[str, Any]:
        reset_flow_call_count()
        selected_by_market: Dict[str, Any] = {}
        diag_result: Dict[str, Any] | None = None
        try:
            rebalance_date = str(get_rebalance_anchor_date())
            rebalance_payload = run_rebalance(rebalance_date, return_by_market=True)
            selected_by_market = rebalance_payload.get("selected_by_market") or {}
            logger.info(
                "[PORTFOLIO][REBALANCE] date=%s kospi=%d kosdaq=%d",
                rebalance_date,
                len(selected_by_market.get("KOSPI", [])),
                len(selected_by_market.get("KOSDAQ", [])),
            )
        except Exception as e:
            logger.exception("[PORTFOLIO] rebalance fetch failed: %s", e)

        runtime_state = state_store.load_state()
        logger.info(
            "[DIAG][PM] diagnostic_mode=%s diagnostic_only=%s",
            DIAG_ENABLED,
            DIAGNOSTIC_ONLY,
        )
        if DIAG_ENABLED:
            os.environ["DISABLE_LIVE_TRADING"] = "true"
            logger.info("[DIAG][PM] forcing DISABLE_LIVE_TRADING=true diag_enabled=%s", DIAG_ENABLED)
            diag_result = run_diagnostics_once(selected_by_market=selected_by_market)
            if DIAGNOSTIC_ONLY:
                return {
                    "diagnostics": diag_result,
                    "kospi": {"status": "skipped"},
                    "kosdaq": {"status": "skipped"},
                }

        try:
            market_data = build_market_data(selected_by_market)
            strategy_result = self.strategy_manager.run_once(
                market_data=market_data, portfolio_state=runtime_state
            )
            intents = strategy_result.get("intents") or []
        except Exception as e:
            logger.exception("[PORTFOLIO] strategy manager failure: %s", e)
            strategy_result = {"status": "error", "message": str(e), "enabled": [], "intents": []}
            intents = []

        try:
            STRATEGY_INTENTS_PATH.parent.mkdir(parents=True, exist_ok=True)
            STRATEGY_INTENTS_PATH.touch(exist_ok=True)
            intent_store.append_intents(intents, STRATEGY_INTENTS_PATH)
            executor_result = self.intent_executor.run_once()
        except Exception as e:
            logger.exception("[PORTFOLIO] intent executor failure: %s", e)
            executor_result = {"status": "error", "message": str(e)}

        try:
            if DISABLE_KOSPI_ENGINE:
                kospi = {"status": "disabled"}
            else:
                kospi = self.kospi_engine.rebalance_if_needed(
                    selected_stocks=selected_by_market.get("KOSPI")
                )
        except Exception as e:
            logger.exception("[PORTFOLIO] KOSPI engine failure: %s", e)
            kospi = {"status": "error", "message": str(e)}
        try:
            if DISABLE_KOSDAQ_LOOP:
                kosdaq = {"status": "disabled"}
            else:
                kosdaq = self.kosdaq_engine.trade_loop(
                    selected_stocks=selected_by_market.get("KOSDAQ")
                )
        except Exception as e:
            logger.exception("[PORTFOLIO] KOSDAQ engine failure: %s", e)
            kosdaq = {"status": "error", "message": str(e)}
        perf = self.performance.snapshot(
            {
                "kospi_core": self.kospi_engine.capital,
                "kosdaq_alpha": self.kosdaq_engine.capital,
            }
        )
        return {
            "strategies": {"manager": strategy_result, "executor": executor_result},
            "diagnostics": diag_result,
            "kospi": kospi,
            "kosdaq": kosdaq,
            "performance": perf,
        }
