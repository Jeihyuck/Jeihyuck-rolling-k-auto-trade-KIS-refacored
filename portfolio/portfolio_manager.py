from __future__ import annotations

import logging
from typing import Any, Dict

from trader.config import DAILY_CAPITAL
from .kospi_core_engine import KospiCoreEngine
from .kosdaq_alpha_engine import KosdaqAlphaEngine

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
        logger.info(
            "[PORTFOLIO] capital=%s kospi=%.0f%% kosdaq=%.0f%%",
            int(self.total_capital),
            self.kospi_ratio * 100,
            self.kosdaq_ratio * 100,
        )

    def run_cycle(self) -> Dict[str, Any]:
        try:
            kospi = self.kospi_engine.rebalance_if_needed()
        except Exception as e:
            logger.exception("[PORTFOLIO] KOSPI engine failure: %s", e)
            kospi = {"status": "error", "message": str(e)}
        try:
            kosdaq = self.kosdaq_engine.trade_loop()
        except Exception as e:
            logger.exception("[PORTFOLIO] KOSDAQ engine failure: %s", e)
            kosdaq = {"status": "error", "message": str(e)}
        return {"kospi": kospi, "kosdaq": kosdaq}
