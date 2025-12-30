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
from trader.utils.env import env_bool, parse_env_flag, resolve_mode

logger = logging.getLogger(__name__)


def truthy(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def main() -> None:
    now = now_kst()
    event_name = os.getenv("GITHUB_EVENT_NAME", "") or ""
    event_name_lower = event_name.lower()
    trading_day = is_trading_day(now)
    active_strategies = resolve_active_strategies() or {1}
    dry_run_flag = parse_env_flag("DRY_RUN", default=False)
    disable_live_flag = parse_env_flag("DISABLE_LIVE_TRADING", default=False)
    live_trading_flag = parse_env_flag("LIVE_TRADING_ENABLED", default=False)
    expect_live_flag = env_bool("EXPECT_LIVE_TRADING", False)
    allow_live_on_push = truthy(os.getenv("ALLOW_LIVE_ON_PUSH", "0"))
    mode = resolve_mode(os.getenv("STRATEGY_MODE", ""))
    diag_enabled = bool(DIAG_ENABLED or DIAGNOSTIC_FORCE_RUN)

    dry_run_reasons: list[str] = []
    decision_reasons: list[str] = []
    if mode == "INTENT_ONLY":
        dry_run_reasons.append("STRATEGY_MODE=INTENT_ONLY")
        decision_reasons.append("STRATEGY_MODE=INTENT_ONLY")
    if disable_live_flag.value:
        dry_run_reasons.append("DISABLE_LIVE_TRADING=1")
        decision_reasons.append("DISABLE_LIVE_TRADING=1")
    if diag_enabled:
        dry_run_reasons.append("diagnostic_mode")
        decision_reasons.append("diagnostic_mode")
    if not live_trading_flag.value and mode == "LIVE":
        dry_run_reasons.append("LIVE_TRADING_ENABLED=0")
        decision_reasons.append("LIVE_TRADING_ENABLED=0")
    if dry_run_flag.value:
        dry_run_reasons.append("DRY_RUN=1")
        decision_reasons.append("DRY_RUN=1")
    if mode == "LIVE":
        if event_name_lower == "pull_request":
            dry_run_reasons.append("event=pull_request")
            decision_reasons.append("event=pull_request")
        elif event_name_lower == "push":
            if allow_live_on_push:
                decision_reasons.append("event=push_allowed")
            else:
                dry_run_reasons.append("event=push")
                decision_reasons.append("event=push")
    for flag in (dry_run_flag, disable_live_flag, live_trading_flag):
        if not flag.valid:
            dry_run_reasons.append(f"{flag.name}=invalid({flag.raw})")
            decision_reasons.append(f"{flag.name}=invalid({flag.raw})")

    dry_run = bool(dry_run_reasons)
    dry_run_reason = ",".join(dry_run_reasons) if dry_run_reasons else "live"
    live_trading_enabled = bool(
        mode == "LIVE" and live_trading_flag.value and not disable_live_flag.value and not dry_run
    )
    engine_disabled_reason = dry_run_reason if dry_run else (
        "DISABLE_LIVE_TRADING env=true" if disable_live_flag.value else (
            "LIVE_TRADING_ENABLED env=false" if not live_trading_flag.value else "enabled"
        )
    )

    logger.info(
        "[TRADER][DRY_RUN_RESOLVE] event=%s allow_live_on_push=%s dry_run=%s reasons=%s env_flags=%s",
        event_name_lower or "unknown",
        allow_live_on_push,
        dry_run,
        decision_reasons or ["live"],
        {
            "DRY_RUN": dry_run_flag.raw if dry_run_flag.raw is not None else dry_run_flag.value,
            "DISABLE_LIVE_TRADING": disable_live_flag.raw if disable_live_flag.raw is not None else disable_live_flag.value,
            "LIVE_TRADING_ENABLED": live_trading_flag.raw if live_trading_flag.raw is not None else live_trading_flag.value,
            "STRATEGY_MODE": mode,
            "ALLOW_LIVE_ON_PUSH": allow_live_on_push,
        },
    )

    expect_kis_env = os.getenv("EXPECT_KIS_ENV")
    kis_env_raw = (os.getenv("KIS_ENV") or "").strip()
    kis_env = kis_env_raw.lower()
    api_base_url = (os.getenv("API_BASE_URL") or "").lower()
    if expect_live_flag:
        snapshot = {
            "DRY_RUN": dry_run_flag.raw if dry_run_flag.raw is not None else dry_run_flag.value,
            "DISABLE_LIVE_TRADING": disable_live_flag.raw if disable_live_flag.raw is not None else disable_live_flag.value,
            "LIVE_TRADING_ENABLED": live_trading_flag.raw if live_trading_flag.raw is not None else live_trading_flag.value,
            "STRATEGY_MODE": mode,
            "EXPECT_KIS_ENV": expect_kis_env,
            "KIS_ENV": kis_env_raw,
            "API_BASE_URL": api_base_url,
            "event": event_name_lower or "unknown",
        }
        guard_failures: list[str] = []
        if dry_run_flag.value or not dry_run_flag.valid:
            guard_failures.append("DRY_RUN!=0")
        if disable_live_flag.value or not disable_live_flag.valid:
            guard_failures.append("DISABLE_LIVE_TRADING!=0")
        if (not live_trading_flag.value) or (not live_trading_flag.valid):
            guard_failures.append("LIVE_TRADING_ENABLED!=1")
        if mode != "LIVE":
            guard_failures.append("STRATEGY_MODE!=LIVE")
        if kis_env != "practice":
            guard_failures.append("KIS_ENV!=practice")
        if "openapivts" not in api_base_url:
            guard_failures.append("API_BASE_URL missing openapivts")
        if expect_kis_env and kis_env_raw != expect_kis_env:
            guard_failures.append("EXPECT_KIS_ENV mismatch")
        if guard_failures:
            logger.error("[TRADER][GUARD] EXPECT_LIVE_TRADING=1 but guard failed: %s reasons=%s", snapshot, guard_failures)
            raise SystemExit(2)

    os.environ["DRY_RUN"] = "1" if dry_run else "0"
    os.environ["DISABLE_LIVE_TRADING"] = "1" if (dry_run or disable_live_flag.value) else "0"
    os.environ["LIVE_TRADING_ENABLED"] = "1" if live_trading_flag.value else "0"
    os.environ["STRATEGY_MODE"] = mode

    if expect_live_flag and dry_run:
        resolved_values = (
            f"DRY_RUN={dry_run_flag.raw!r} DISABLE_LIVE_TRADING={disable_live_flag.raw!r} "
            f"LIVE_TRADING_ENABLED={live_trading_flag.raw!r} mode={mode} event={event_name_lower or 'unknown'} "
            f"ALLOW_LIVE_ON_PUSH={allow_live_on_push!r}"
        )
        raise SystemExit(
            f"EXPECT_LIVE_TRADING=1 but dry_run resolved True. reasons={dry_run_reasons} values={resolved_values}"
        )
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
        "[TRADER][STARTUP] event=%s trading_day=%s mode=%s dry_run=%s reasons=%s live_trading_enabled=%s disable_live_flag=%s dry_run_flag=%s live_trading_flag=%s active_strategies=%s allow_adopt_unmanaged=%s engine_disabled_reason=%s",
        event_name or "unknown",
        trading_day,
        mode,
        dry_run,
        dry_run_reasons,
        live_trading_enabled,
        disable_live_flag.raw if disable_live_flag.raw is not None else disable_live_flag.value,
        dry_run_flag.raw if dry_run_flag.raw is not None else dry_run_flag.value,
        live_trading_flag.raw if live_trading_flag.raw is not None else live_trading_flag.value,
        sorted(active_strategies) if active_strategies else [1],
        ALLOW_ADOPT_UNMANAGED,
        engine_disabled_reason,
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
