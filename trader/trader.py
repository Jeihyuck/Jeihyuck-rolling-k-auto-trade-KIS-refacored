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
from trader.config import (
    ALLOW_ADOPT_UNMANAGED,
    DIAG_ENABLED,
    DIAGNOSTIC_FORCE_RUN,
    DIAGNOSTIC_MODE,
    DIAGNOSTIC_ONLY,
    resolve_active_strategies,
)

logger = logging.getLogger(__name__)


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


def main() -> None:
    now = now_kst()
    event_name = os.getenv("GITHUB_EVENT_NAME", "")
    trading_day = is_trading_day(now)
    active_strategies = resolve_active_strategies()
    live_trading_enabled_env = _env_flag("LIVE_TRADING_ENABLED", True)
    disable_live_env = _env_flag("DISABLE_LIVE_TRADING", False)
    dry_run_env_raw = os.getenv("DRY_RUN", "")
    dry_run_env = dry_run_env_raw.lower() in {"1", "true", "yes", "on"}
    dry_run_reasons: list[str] = []
    if dry_run_env:
        dry_run_reasons.append(f"DRY_RUN env={dry_run_env_raw}")
    if event_name in {"push", "pull_request"}:
        dry_run_reasons.append(f"event={event_name} forces dry_run")
    if disable_live_env:
        dry_run_reasons.append("DISABLE_LIVE_TRADING env=true")
    if not live_trading_enabled_env:
        dry_run_reasons.append("LIVE_TRADING_ENABLED env=false")
    diag_enabled = bool(DIAG_ENABLED or DIAGNOSTIC_FORCE_RUN)
    if diag_enabled:
        dry_run_reasons.append("diagnostic_mode")
    dry_run = bool(dry_run_reasons)
    dry_run_reason = ",".join(dry_run_reasons) if dry_run_reasons else "live"
    if dry_run:
        os.environ["DISABLE_LIVE_TRADING"] = "true"
    logger.info(
        "[DIAG][TRADER] now=%s trading_day=%s diag_enabled=%s force_run=%s only=%s mode=%s",
        now.isoformat(),
        trading_day,
        DIAG_ENABLED,
        DIAGNOSTIC_FORCE_RUN,
        DIAGNOSTIC_ONLY,
        DIAGNOSTIC_MODE,
    )
    logger.info(
        "[TRADER][STARTUP] event=%s trading_day=%s dry_run=%s dry_run_reason=%s live_trading_enabled=%s active_strategies=%s allow_adopt_unmanaged=%s engine_disabled_reason=%s",
        event_name or "unknown",
        trading_day,
        dry_run,
        dry_run_reason,
        live_trading_enabled_env and not dry_run and not disable_live_env,
        sorted(active_strategies),
        ALLOW_ADOPT_UNMANAGED,
        dry_run_reason if dry_run else ("DISABLE_LIVE_TRADING" if disable_live_env else "enabled"),
    )
    if (not trading_day) and (not (DIAG_ENABLED and DIAGNOSTIC_FORCE_RUN)):
        logger.warning("[TRADER] 비거래일(%s) → 즉시 종료 dry_run=%s reason=%s", now.date(), dry_run, dry_run_reason)
        return
    if (not trading_day) and diag_enabled:
        logger.warning(
            "[DIAG][TRADER] non-trading-day(%s) but running diagnostics (only=%s force_run=%s)",
            now.date(),
            DIAGNOSTIC_ONLY,
            DIAGNOSTIC_FORCE_RUN,
        )
    runtime_state = {}
    kis: KisAPI | None = None
    try:
        runtime_state = runtime_state_store.load_state()
        kis = KisAPI()
        balance = kis.get_balance()
        runtime_state = runtime_state_store.reconcile_with_kis_balance(
            runtime_state,
            balance,
            active_strategies=active_strategies,
        )
        runtime_state_store.save_state(runtime_state)
        logger.info("[TRADER] runtime state reconciled")
    except Exception:
        logger.exception("[TRADER] runtime state reconcile failed")
        runtime_state = runtime_state or runtime_state_store.load_state()

    if DIAGNOSTIC_ONLY:
        from trader.diagnostics_runner import run_diagnostics

        run_diagnostics(kis=kis, runtime_state=runtime_state, selected_by_market=None)
        logger.info("[DIAG][TRADER] diagnostic_only complete")
        return

    diag_result = None
    if DIAGNOSTIC_MODE:
        try:
            from trader.diagnostics_runner import run_diagnostics

            diag_result = run_diagnostics(
                kis=kis, runtime_state=runtime_state, selected_by_market=None
            )
        except Exception:
            logger.exception("[DIAG][TRADER] diagnostics run failed")

    mgr = PortfolioManager(active_strategies=active_strategies)
    result = mgr.run_once()
    if isinstance(result, dict) and diag_result is not None:
        result.setdefault("diagnostics", diag_result)
    if isinstance(result, dict):
        result.setdefault("active_strategies", sorted(active_strategies))
        result.setdefault("dry_run", dry_run)
        result.setdefault("dry_run_reason", dry_run_reason)
    logger.info("[TRADER] cycle complete %s", result)


if __name__ == "__main__":
    main()
