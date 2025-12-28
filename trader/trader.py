# -*- coding: utf-8 -*-
"""Thin entrypoint orchestrating KOSPI core + KOSDAQ alpha engines."""
from __future__ import annotations

import logging
import os

from portfolio.portfolio_manager import PortfolioManager
from trader.kis_wrapper import KisAPI
from trader import state_store as runtime_state_store
from trader.time_utils import is_trading_day, now_kst
from trader.subject_flow import get_subject_flow_with_fallback  # noqa: F401 - exported for engines
from trader.config import DIAG_ENABLED, DIAGNOSTIC_FORCE_RUN, DIAGNOSTIC_MODE, DIAGNOSTIC_ONLY

logger = logging.getLogger(__name__)


def main() -> None:
    now = now_kst()
    diag_enabled = DIAG_ENABLED
    if diag_enabled:
        os.environ["DISABLE_LIVE_TRADING"] = "true"
        logger.info(
            "[DIAG][TRADER] forcing DISABLE_LIVE_TRADING=true diag_enabled=%s",
            diag_enabled,
        )
    if not is_trading_day(now) and not (diag_enabled and DIAGNOSTIC_FORCE_RUN):
        logger.warning("[TRADER] 비거래일(%s) → 즉시 종료", now.date())
        return
    if DIAGNOSTIC_ONLY:
        from trader.diagnostics_runner import run_diagnostics_once

        run_diagnostics_once()
        logger.info("[DIAG][TRADER] diagnostic_only complete")
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

    diag_result = None
    if DIAGNOSTIC_MODE:
        try:
            from trader.diagnostics_runner import run_diagnostics_once

            diag_result = run_diagnostics_once()
        except Exception:
            logger.exception("[DIAG][TRADER] diagnostics run failed")

    mgr = PortfolioManager()
    result = mgr.run_once()
    if isinstance(result, dict) and diag_result is not None:
        result.setdefault("diagnostics", diag_result)
    logger.info("[TRADER] cycle complete %s", result)


if __name__ == "__main__":
    main()
