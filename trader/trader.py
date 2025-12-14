# -*- coding: utf-8 -*-
"""Thin entrypoint orchestrating KOSPI core + KOSDAQ alpha engines."""
from __future__ import annotations

import logging

from portfolio.portfolio_manager import PortfolioManager
from trader.time_utils import is_trading_day, now_kst

logger = logging.getLogger(__name__)


def main() -> None:
    now = now_kst()
    if not is_trading_day(now):
        logger.warning("[TRADER] 비거래일(%s) → 즉시 종료", now.date())
        return

    mgr = PortfolioManager()
    result = mgr.run_once()
    logger.info("[TRADER] cycle complete %s", result)


if __name__ == "__main__":
    main()
