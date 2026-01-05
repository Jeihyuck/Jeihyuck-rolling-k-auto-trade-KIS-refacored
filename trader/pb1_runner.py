from __future__ import annotations

import argparse
import logging
import os
import time as time_mod
from datetime import datetime, time as dtime

from trader.config import (
    AFTERNOON_WINDOW_END,
    AFTERNOON_WINDOW_START,
    CLOSE_AUCTION_END,
    CLOSE_AUCTION_START,
    DIAGNOSTIC_MODE,
    DIAGNOSTIC_ONLY,
    MARKET_CLOSE_HHMM,
    MARKET_OPEN_HHMM,
    MORNING_EXIT_END,
    MORNING_EXIT_START,
    MORNING_WINDOW_END,
    MORNING_WINDOW_START,
    PB1_FORCE_ENTRY_ON_PUSH,
    PB1_MAX_WAIT_FOR_WINDOW_MIN,
    PB1_WAIT_FOR_WINDOW,
)
from trader.db.engine import make_engine
from trader.db.lock import release_lock, try_acquire_lock
from trader.db.migrate import run_migrations
from trader.db.repos import FillsRepo, OrdersRepo, PositionsRepo, RunsRepo, UniverseRepo
from trader.kis_wrapper import KisAPI
from trader.pb1_engine import PB1Engine
from trader.time_utils import is_trading_day, now_kst
from trader.utils.env import env_bool, parse_env_flag, resolve_mode
from trader.window_router import WindowDecision, decide_window

logger = logging.getLogger(__name__)


def _parse_hhmm_to_time(hhmm: str) -> dtime:
    hh, mm = hhmm.split(":")
    return dtime(hour=int(hh), minute=int(mm))


def _next_window_start(now: datetime, window_starts: list[dtime]) -> datetime | None:
    sorted_starts = sorted(window_starts)
    for start in sorted_starts:
        if now.time() < start:
            return now.replace(hour=start.hour, minute=start.minute, second=0, microsecond=0)
    return None


def _market_session(now: datetime) -> tuple[datetime, datetime]:
    open_t = _parse_hhmm_to_time(MARKET_OPEN_HHMM)
    close_t = _parse_hhmm_to_time(MARKET_CLOSE_HHMM)
    return (
        now.replace(hour=open_t.hour, minute=open_t.minute, second=0, microsecond=0),
        now.replace(hour=close_t.hour, minute=close_t.minute, second=0, microsecond=0),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PB1 close pullback runner")
    parser.add_argument("--window", default="auto", choices=["auto", "morning", "afternoon"], help="Execution window override")
    parser.add_argument("--phase", default="auto", choices=["auto", "entry", "exit", "verify"], help="Phase override")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    engine = make_engine()
    run_migrations(engine)

    now = now_kst()
    event_name = os.getenv("GITHUB_EVENT_NAME", "") or ""
    event_name_lower = event_name.lower()
    trading_day = is_trading_day(now)
    non_trading_day = not trading_day
    diag_env_flag = (
        env_bool("DIAGNOSTIC_FORCE_RUN", False)
        or env_bool("DIAGNOSTIC_ONLY", DIAGNOSTIC_ONLY)
        or env_bool("DIAGNOSTIC_MODE", DIAGNOSTIC_MODE)
    )
    _, market_close_dt = _market_session(now)
    market_close_time = market_close_dt.time()
    wait_enabled = env_bool("PB1_WAIT_FOR_WINDOW", PB1_WAIT_FOR_WINDOW)
    if event_name_lower == "schedule":
        wait_enabled = False
    max_wait_s = int(PB1_MAX_WAIT_FOR_WINDOW_MIN) * 60
    window = decide_window(now=now, override=args.window)
    window_name_for_log = window.name if window else "none"
    phase_for_log = window.phase if window and hasattr(window, "phase") else "none"
    target_start = None

    os.environ.setdefault("MORNING_WINDOW_START", MORNING_WINDOW_START)
    os.environ.setdefault("MORNING_WINDOW_END", MORNING_WINDOW_END)
    os.environ.setdefault("MORNING_EXIT_START", MORNING_EXIT_START)
    os.environ.setdefault("MORNING_EXIT_END", MORNING_EXIT_END)
    os.environ.setdefault("AFTERNOON_WINDOW_START", AFTERNOON_WINDOW_START)
    os.environ.setdefault("AFTERNOON_WINDOW_END", AFTERNOON_WINDOW_END)
    os.environ.setdefault("CLOSE_AUCTION_START", CLOSE_AUCTION_START)
    os.environ.setdefault("CLOSE_AUCTION_END", CLOSE_AUCTION_END)

    force_diag = diag_env_flag or not trading_day or now.time() >= market_close_time
    if not force_diag and window is None and trading_day and now.time() < market_close_time:
        window_starts = [
            _parse_hhmm_to_time(MORNING_WINDOW_START),
            _parse_hhmm_to_time(AFTERNOON_WINDOW_START),
            _parse_hhmm_to_time(CLOSE_AUCTION_START),
        ]
        target_start = _next_window_start(now, window_starts)
        if target_start is None:
            force_diag = True
        elif not wait_enabled:
            logger.info(
                "[PB1][RUN-PLAN] event=%s now_kst=%s trading_day=%s action=skip target_start=%s max_wait_s=%s window=%s phase=%s",
                event_name_lower or "unknown",
                now.isoformat(),
                trading_day,
                target_start.isoformat(),
                max_wait_s,
                window_name_for_log,
                phase_for_log,
            )
            return
        else:
            wait_seconds = int((target_start - now).total_seconds())
            if wait_seconds > max_wait_s:
                logger.info(
                    "[PB1][RUN-PLAN] event=%s now_kst=%s trading_day=%s action=skip target_start=%s max_wait_s=%s window=%s phase=%s",
                    event_name_lower or "unknown",
                    now.isoformat(),
                    trading_day,
                    target_start.isoformat(),
                    max_wait_s,
                    window_name_for_log,
                    phase_for_log,
                )
                return

    plan_window_name = window_name_for_log
    plan_phase_for_log = phase_for_log
    if force_diag and window is None:
        plan_window_name = "diagnostic"
        plan_phase_for_log = "verify"

    action = "diag" if force_diag else "run" if window else "wait"
    logger.info(
        "[PB1][RUN-PLAN] event=%s now_kst=%s trading_day=%s action=%s target_start=%s max_wait_s=%s window=%s phase=%s",
        event_name_lower or "unknown",
        now.isoformat(),
        trading_day,
        action,
        target_start.isoformat() if target_start else "none",
        max_wait_s,
        plan_window_name,
        plan_phase_for_log,
    )

    if action == "wait" and target_start:
        while True:
            now = now_kst()
            remaining = (target_start - now).total_seconds()
            if remaining <= 0:
                break
            if remaining > max_wait_s:
                logger.info(
                    "[PB1][RUN-PLAN] action=skip reason=wait_exceeds_max target_start=%s remaining_s=%.0f max_wait_s=%s",
                    target_start.isoformat(),
                    remaining,
                    max_wait_s,
                )
                return
            sleep_for = min(30, remaining)
            logger.info("[PB1][WAIT] until=%s remaining_s=%.0f sleep=%.0f", target_start.isoformat(), remaining, sleep_for)
            time_mod.sleep(sleep_for)
        now = now_kst()
        trading_day = is_trading_day(now)
        non_trading_day = not trading_day
        force_diag = diag_env_flag or not trading_day or now.time() >= market_close_time
        window = decide_window(now=now, override=args.window)
        window_name_for_log = window.name if window else "none"
        phase_for_log = window.phase if window and hasattr(window, "phase") else "none"
        if window is None and not force_diag:
            logger.info("[PB1][WINDOW] outside active windows override=%s now=%s", args.window, now)
            return
        action = "diag" if force_diag else "run"

    dry_run_flag = parse_env_flag("DRY_RUN", default=False)
    disable_live_flag = parse_env_flag("DISABLE_LIVE_TRADING", default=False)
    live_trading_flag = parse_env_flag("LIVE_TRADING_ENABLED", default=False)
    expect_live_flag = env_bool("EXPECT_LIVE_TRADING", False)
    mode = resolve_mode(os.getenv("STRATEGY_MODE", ""))
    dry_run_reasons: list[str] = []
    if non_trading_day:
        dry_run_reasons.append("non_trading_day")
        os.environ["PB1_ENTRY_ENABLED"] = "0"
        os.environ["DIAGNOSTIC_FORCE_RUN"] = "1"
        os.environ["DISABLE_LIVE_TRADING"] = "1"
        os.environ["DRY_RUN"] = "1"
        os.environ["LIVE_TRADING_ENABLED"] = "0"
    diag_enabled = force_diag or diag_env_flag
    if force_diag and not non_trading_day and now.time() >= market_close_time:
        dry_run_reasons.append("market_closed")
    if diag_enabled:
        dry_run_reasons.append("diagnostic_mode")
    if mode == "INTENT_ONLY":
        dry_run_reasons.append("STRATEGY_MODE=INTENT_ONLY")
    if parse_env_flag("DISABLE_LIVE_TRADING", default=disable_live_flag.value).value:
        dry_run_reasons.append("DISABLE_LIVE_TRADING=1")
    live_trading_flag = parse_env_flag("LIVE_TRADING_ENABLED", default=live_trading_flag.value)
    disable_live_flag = parse_env_flag("DISABLE_LIVE_TRADING", default=disable_live_flag.value)
    dry_run_flag = parse_env_flag("DRY_RUN", default=dry_run_flag.value)
    if not live_trading_flag.value and mode == "LIVE":
        dry_run_reasons.append("LIVE_TRADING_ENABLED=0")
    if dry_run_flag.value:
        dry_run_reasons.append("DRY_RUN=1")
    for flag in (dry_run_flag, disable_live_flag, live_trading_flag):
        if not flag.valid:
            dry_run_reasons.append(f"{flag.name}=invalid({flag.raw})")

    dry_run = bool(dry_run_reasons)
    dry_run_reason = ",".join(dry_run_reasons) if dry_run_reasons else "live"

    logger.info(
        "[PB1][DRY_RUN_RESOLVE] event=%s dry_run=%s reasons=%s",
        event_name_lower or "unknown",
        dry_run,
        dry_run_reasons or ["live"],
    )

    expect_kis_env = os.getenv("EXPECT_KIS_ENV")
    kis_env_raw = (os.getenv("KIS_ENV") or "").strip()
    kis_env = kis_env_raw.lower()
    api_base_url = (os.getenv("API_BASE_URL") or "").lower()
    guard_live = expect_live_flag and not diag_enabled and trading_day and not dry_run
    if guard_live:
        guard_failures: list[str] = []
        if dry_run:
            guard_failures.append("dry_run")
        if not live_trading_flag.value or not live_trading_flag.valid:
            guard_failures.append("LIVE_TRADING_ENABLED!=1")
        if disable_live_flag.value or not disable_live_flag.valid:
            guard_failures.append("DISABLE_LIVE_TRADING!=0")
        if mode != "LIVE":
            guard_failures.append("STRATEGY_MODE!=LIVE")
        if kis_env != "practice":
            guard_failures.append("KIS_ENV!=practice")
        if "openapivts" not in api_base_url:
            guard_failures.append("API_BASE_URL missing openapivts")
        if expect_kis_env and kis_env_raw != expect_kis_env:
            guard_failures.append("EXPECT_KIS_ENV mismatch")
        if guard_failures:
            raise SystemExit(f"EXPECT_LIVE_TRADING=1 guards failed: {guard_failures}")

    def _apply_env_flags(dry: bool) -> None:
        os.environ["DRY_RUN"] = "1" if dry else "0"
        os.environ["DISABLE_LIVE_TRADING"] = "1" if (dry or disable_live_flag.value or non_trading_day) else "0"
        os.environ["LIVE_TRADING_ENABLED"] = "1" if (live_trading_flag.value and not non_trading_day) else "0"
        os.environ["STRATEGY_MODE"] = mode

    _apply_env_flags(dry_run)

    phase_override_arg = args.phase
    if (
        window
        and event_name_lower == "push"
        and phase_override_arg == "auto"
        and window.name == "afternoon"
        and env_bool("PB1_FORCE_ENTRY_ON_PUSH", PB1_FORCE_ENTRY_ON_PUSH)
    ):
        try:
            start = datetime.fromisoformat(f"{now.date()}T{AFTERNOON_WINDOW_START}")
            end = datetime.fromisoformat(f"{now.date()}T{AFTERNOON_WINDOW_END}")
            in_afternoon = start.time() <= now.time() < end.time()
        except Exception:
            in_afternoon = False
        if trading_day and in_afternoon and window.phase == "prep":
            logger.info("[PB1][PHASE_OVERRIDE] event=push from=prep to=entry reason=PB1_FORCE_ENTRY_ON_PUSH")
            phase_override_arg = "entry"

    if action == "diag":
        dry_run = True
        dry_run_reason = dry_run_reason if dry_run_reason else "diagnostic"
        dry_run_reasons = dry_run_reasons or ["diagnostic"]
        diag_enabled = True
        if args.phase == "auto":
            phase_override_arg = "verify"
        window = window or WindowDecision(name="diagnostic", phase=phase_override_arg or "verify")
        _apply_env_flags(dry_run)

    window_name_for_log = window.name if window else "none"
    phase_for_log = window.phase if window and hasattr(window, "phase") else "none"

    logger.info(
        "[PB1][RUN-START] event=%s now_kst=%s trading_day=%s window=%s phase=%s DRY_RUN=%s DISABLE_LIVE_TRADING=%s LIVE_TRADING_ENABLED=%s STRATEGY_MODE=%s PB1_ENTRY_ENABLED=%s reasons=%s",
        event_name_lower or "unknown",
        now.isoformat(),
        trading_day,
        window_name_for_log,
        phase_for_log,
        dry_run,
        os.getenv("DISABLE_LIVE_TRADING"),
        os.getenv("LIVE_TRADING_ENABLED"),
        os.getenv("STRATEGY_MODE"),
        os.getenv("PB1_ENTRY_ENABLED"),
        dry_run_reasons or ["live"],
    )

    if non_trading_day:
        logger.info("[PB1][SKIP] non-trading-day(%s) → diagnostics/dry-run reason=%s", now.date(), dry_run_reason)
        if diag_enabled:
            logger.warning("[PB1][DIAG] non-trading-day(%s) but running diagnostics", now.date())

    owner = os.getenv("GITHUB_ACTOR", "local")
    workflow_run_id = os.getenv("GITHUB_RUN_ID", "local")
    lock_key = f"PB1:{kis_env_raw or 'practice'}"
    lock_acquired = try_acquire_lock(engine, lock_key)
    if not lock_acquired:
        logger.warning("[PB1][LOCKED] key=%s owner=%s run_id=%s", lock_key, owner, workflow_run_id)
        return

    runs_repo = RunsRepo(engine)
    universe_repo = UniverseRepo(engine)
    orders_repo = OrdersRepo(engine)
    fills_repo = FillsRepo(engine)
    positions_repo = PositionsRepo(engine)

    run_record_id = None
    try:
        kis: KisAPI | None = None
        try:
            kis = KisAPI()
            if kis.env != kis_env:
                dry_run_reasons.append("kis_env_mismatch")
                dry_run = True
                _apply_env_flags(dry_run)
        except Exception:
            logger.exception("[PB1] KIS init failed, forcing dry-run")
            dry_run_reasons.append("kis_init_failed")
            dry_run = True
            _apply_env_flags(dry_run)

        run_record_id = runs_repo.start_run(
            env=kis_env or "practice",
            strategy="pb1_pullback_close",
            run_window=window_name_for_log,
            phase=phase_override_arg,
            event_name=event_name_lower,
            dry_run=dry_run,
            git_sha=os.getenv("GITHUB_SHA"),
            workflow=os.getenv("GITHUB_WORKFLOW"),
            workflow_run_id=workflow_run_id,
            workflow_attempt=int(os.getenv("GITHUB_RUN_ATTEMPT", "0") or 0),
            config_json={"dry_run_reasons": dry_run_reasons, "window": window_name_for_log, "phase": phase_for_log},
        )

        engine_runner = PB1Engine(
            universe_repo=universe_repo,
            orders_repo=orders_repo,
            fills_repo=fills_repo,
            positions_repo=positions_repo,
            kis=kis,
            window=window,
            phase_override=phase_override_arg,
            dry_run=dry_run,
            env=kis_env or "practice",
            run_id=run_record_id,
        )
        result = engine_runner.run()
        runs_repo.finish_run(run_record_id, status=result.status, notes=result.notes)
    except Exception as exc:
        logger.exception("[PB1][FAIL] unexpected error")
        if run_record_id:
            runs_repo.finish_run(run_record_id, status="FAILED", notes=str(exc))
        raise
    finally:
        release_lock(engine, lock_key)


if __name__ == "__main__":
    main()
