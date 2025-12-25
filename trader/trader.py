# -*- coding: utf-8 -*-
"""Thin entrypoint orchestrating KOSPI core + KOSDAQ alpha engines."""
from __future__ import annotations

import logging
import os
import time

from rolling_k_auto_trade_api.best_k_meta_strategy import run_rebalance
from trader.kis_wrapper import KisAPI
from trader import state_store as runtime_state_store
from trader.state_store import ensure_minimum_files
from trader.strategy_manager import StrategyManager
from trader.ledger import load_ledger_entries, strategy_map_from_ledger
from trader.time_utils import MARKET_CLOSE, is_trading_day, is_trading_window, now_kst
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
    ensure_minimum_files()
    logger.info("[BOOT] ensured state/orders_map/ledger placeholders")
    start_ts = time.time()
    max_runtime_sec = int(os.getenv("MAX_RUNTIME_SEC", "900"))

    def _finalize(state: dict[str, object]) -> None:
        runtime_state_store.save_state_atomic(state)
        try:
            from trader import report_ceo

            report_ceo.ceo_report(now_kst(), period=os.getenv("CEO_REPORT_PERIOD", "daily"))
        except Exception:
            logger.exception("[TRADER] CEO report generation failed")

    runtime_state = runtime_state_store.load_state()
    kis: KisAPI | None = None
    balance: dict[str, object] = {"positions": []}
    preferred_strategy: dict[str, object] = {}
    try:
        ledger_entries = load_ledger_entries()
        preferred_strategy = strategy_map_from_ledger(ledger_entries) or {}
    except Exception:
        logger.exception("[TRADER] ledger preload failed")

    try:
        kis = KisAPI()
        balance = kis.get_balance()
        runtime_state = runtime_state_store.reconcile_with_kis_balance(
            balance, preferred_strategy=preferred_strategy, state=runtime_state
        )
        runtime_state_store.save_state_atomic(runtime_state)
        logger.info("[TRADER] runtime state reconciled (positions=%d)", len(runtime_state.get("positions", {})))
    except Exception as e:
        logger.exception("[TRADER] runtime state reconcile failed: %s", e)
        runtime_state.setdefault("lots", [])
        runtime_state["positions"] = {}
        runtime_state.setdefault("meta", {})["reconcile_error"] = str(e)
        _finalize(runtime_state)
        return

    now = now_kst()
    if not is_trading_day(now) or not is_trading_window(now):
        logger.warning("[TRADER] 비거래일/장외(%s) → 단일 실행 후 종료", now.isoformat())
        _finalize(runtime_state)
        return

    try:
        if kis is None:
            kis = KisAPI()
        manager = StrategyManager(kis=kis)
        candidates = _collect_rebalance_candidates()
        cycle_count = 0
        while True:
            now = now_kst()
            if (time.time() - start_ts) > max_runtime_sec:
                logger.warning("[TRADER] max runtime reached (%ss) → 종료", max_runtime_sec)
                break
            if now.time() >= MARKET_CLOSE or not is_trading_window(now):
                logger.info("[TRADER] 장마감/장외(%s) → 거래 루프 종료", now.isoformat())
                break
            cycle_count += 1
            try:
                balance = kis.get_balance()
                runtime_state = runtime_state_store.reconcile_with_kis_balance(
                    balance, preferred_strategy=preferred_strategy, state=runtime_state
                )
            except Exception:
                logger.exception("[TRADER] balance fetch/reconcile failed")
            cycle_candidates = set(candidates)
            cycle_candidates.update(runtime_state.get("positions", {}).keys())
            try:
                result = manager.run_cycle(runtime_state, balance, cycle_candidates)
            except Exception:
                logger.exception("[TRADER] strategy cycle failed")
                result = {"entries": 0, "exits": 0}
            runtime_state_store.save_state_atomic(runtime_state)
            logger.info(
                "[TRADER] strategy cycle %d complete (entries=%s, exits=%s)",
                cycle_count,
                result.get("entries"),
                result.get("exits"),
            )
            time.sleep(15)
    except Exception:
        logger.exception("[TRADER] strategy loop failed")
    except KeyboardInterrupt:
        logger.info("[TRADER] 수동 종료 신호 → 거래 루프 종료")
    finally:
        _finalize(runtime_state)


if __name__ == "__main__":
    main()
