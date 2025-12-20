# -*- coding: utf-8 -*-
"""Thin entrypoint orchestrating KOSPI core + KOSDAQ alpha engines."""
from __future__ import annotations

import logging

from portfolio.portfolio_manager import PortfolioManager
from trader.kis_wrapper import KisAPI
from trader import state_store as runtime_state_store
from trader.time_utils import is_trading_day, now_kst
from trader.subject_flow import get_subject_flow_with_fallback  # noqa: F401 - exported for engines

logger = logging.getLogger(__name__)


def main() -> None:
    now = now_kst()
    if not is_trading_day(now):
        logger.warning("[TRADER] 비거래일(%s) → 즉시 종료", now.date())
        return
    try:
        runtime_state = runtime_state_store.load_state()
        kis = KisAPI()
        balance = kis.get_balance()
        runtime_state = runtime_state_store.reconcile_with_kis_balance(
            runtime_state, balance
        )
        runtime_state_store.save_state(runtime_state)
        logger.info("[TRADER] runtime state reconciled")
    except Exception:
        logger.exception("[TRADER] runtime state reconcile failed")

    mgr = PortfolioManager()
    result = mgr.run_once()
    logger.info("[TRADER] cycle complete %s", result)


if __name__ == "__main__":
    main()
