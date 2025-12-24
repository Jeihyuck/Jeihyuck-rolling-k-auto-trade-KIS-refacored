# -*- coding: utf-8 -*-
"""Thin entrypoint orchestrating KOSPI core + KOSDAQ alpha engines."""
from __future__ import annotations

import logging

from rolling_k_auto_trade_api.best_k_meta_strategy import run_rebalance
from trader.kis_wrapper import KisAPI
from trader import state_store as runtime_state_store
from trader.strategy_manager import StrategyManager
from trader.ledger import load_ledger_entries, strategy_map_from_ledger
from trader.time_utils import is_trading_day, now_kst
from trader.core_utils import get_rebalance_anchor_date
from trader.code_utils import normalize_code
from trader.subject_flow import get_subject_flow_with_fallback  # noqa: F401 - exported for engines

logger = logging.getLogger(__name__)


def _collect_rebalance_candidates() -> set[str]:
    candidates: set[str] = set()
    try:
        rebalance_date = str(get_rebalance_anchor_date())
        payload = run_rebalance(rebalance_date, return_by_market=True)
        selected_by_market = payload.get("selected_by_market") or {}
        for items in selected_by_market.values():
            for row in items or []:
                code = normalize_code(row.get("code") or row.get("pdno") or row.get("symbol") or "")
                if code:
                    candidates.add(code)
        logger.info("[TRADER] rebalance candidates=%d (date=%s)", len(candidates), rebalance_date)
    except Exception:
        logger.exception("[TRADER] rebalance candidate fetch failed")
    return candidates


def main() -> None:
    now = now_kst()
    if not is_trading_day(now):
        logger.warning("[TRADER] 비거래일(%s) → 즉시 종료", now.date())
        return
    runtime_state = runtime_state_store.load_state()
    kis: KisAPI | None = None
    balance: dict[str, object] = {}
    try:
        kis = KisAPI()
        ledger_entries = load_ledger_entries()
        preferred_strategy = strategy_map_from_ledger(ledger_entries)
        balance = kis.get_balance()
        runtime_state = runtime_state_store.reconcile_with_kis_balance(
            runtime_state, balance, preferred_strategy=preferred_strategy
        )
        runtime_state_store.save_state(runtime_state)
        logger.info("[TRADER] runtime state reconciled (positions=%d)", len(runtime_state.get("positions", {})))
    except Exception:
        logger.exception("[TRADER] runtime state reconcile failed")

    try:
        kis = kis or KisAPI()
        manager = StrategyManager(kis=kis)
        candidates = _collect_rebalance_candidates()
        # open positions도 관찰 대상에 포함
        candidates.update(runtime_state.get("positions", {}).keys())
        result = manager.run_cycle(runtime_state, balance, candidates)
        runtime_state_store.save_state(runtime_state)
        logger.info("[TRADER] strategy cycle complete %s", result)
    except Exception:
        logger.exception("[TRADER] strategy cycle failed")


if __name__ == "__main__":
    main()
