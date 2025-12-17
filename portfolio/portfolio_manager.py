from __future__ import annotations

import logging
from typing import Any, Dict

from rolling_k_auto_trade_api.best_k_meta_strategy import run_rebalance
from trader.config import DAILY_CAPITAL
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
        logger.info(
            "[PORTFOLIO] capital=%s kospi=%.0f%% kosdaq=%.0f%%",
            int(self.total_capital),
            self.kospi_ratio * 100,
            self.kosdaq_ratio * 100,
        )

    def run_once(self) -> Dict[str, Any]:
        reset_flow_call_count()
        selected_by_market: Dict[str, Any] = {}
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

        try:
            kospi = self.kospi_engine.rebalance_if_needed(
                selected_stocks=selected_by_market.get("KOSPI")
            )
        except Exception as e:
            logger.exception("[PORTFOLIO] KOSPI engine failure: %s", e)
            kospi = {"status": "error", "message": str(e)}
        try:
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
        return {"kospi": kospi, "kosdaq": kosdaq, "performance": perf}
