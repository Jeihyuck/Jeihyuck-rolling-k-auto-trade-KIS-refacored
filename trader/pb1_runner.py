from __future__ import annotations

import argparse
import logging
import os
import time as time_mod
from datetime import datetime, time as dtime
from pathlib import Path

from trader.kis_wrapper import KisAPI
from trader.time_utils import is_trading_day, now_kst
from trader.config import (
    BOTSTATE_LOCK_TTL_SEC,
    DIAGNOSTIC_MODE,
    DIAGNOSTIC_ONLY,
    PB1_DIAG_LEVEL,
    PB1_SHADOW_LIVE,
    MORNING_WINDOW_START,
    MORNING_WINDOW_END,
    MORNING_EXIT_START,
    MORNING_EXIT_END,
    AFTERNOON_WINDOW_START,
    AFTERNOON_WINDOW_END,
    CLOSE_AUCTION_START,
    CLOSE_AUCTION_END,
    PB1_FORCE_ENTRY_ON_PUSH,
    PB1_WAIT_FOR_WINDOW,
    PB1_MAX_WAIT_FOR_WINDOW_MIN,
    MARKET_OPEN_HHMM,
    MARKET_CLOSE_HHMM,
)
from trader.utils.env import env_bool, parse_env_flag, resolve_mode
from trader.botstate_sync import (
    acquire_lock,
    release_lock,
    setup_worktree,
    persist_run_files,
    resolve_botstate_worktree_dir,
)
from trader.pb1_engine import PB1Engine
from trader.window_router import WindowDecision, decide_window

logger = logging.getLogger(__name__)


def truthy(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_hhmm_to_time(hhmm: str) -> time:
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
    parser.add_argument("--window", default="auto", choices=["auto", "morning", "afternoon", "diagnostic"], help="Execution window override")
    parser.add_argument("--phase", default="auto", choices=["auto", "entry", "exit", "verify"], help="Phase override")
    parser.add_argument("--target-branch", default=os.getenv("BOTSTATE_BRANCH", "bot-state"), help="Bot-state target branch")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
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
    diag_level_raw = os.getenv("PB1_DIAG_LEVEL", str(PB1_DIAG_LEVEL))
    try:
        diag_level = int(diag_level_raw)
    except Exception:
        diag_level = 1
    if diag_level not in (1, 2):
        diag_level = 1
    shadow_live_flag = env_bool("PB1_SHADOW_LIVE", PB1_SHADOW_LIVE)
    _, market_close_dt = _market_session(now)
    market_close_time = market_close_dt.time()
    wait_enabled = env_bool("PB1_WAIT_FOR_WINDOW", PB1_WAIT_FOR_WINDOW)
    if event_name_lower == "schedule":
        wait_enabled = False
    max_wait_s = int(PB1_MAX_WAIT_FOR_WINDOW_MIN) * 60
    window = decide_window(now=now, override=args.window if args.window != "diagnostic" else "auto")
    if args.window == "diagnostic":
        window = WindowDecision(name="diagnostic", phase=args.phase if args.phase != "auto" else "verify")
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

    worktree_dir = resolve_botstate_worktree_dir()
    setup_worktree(Path.cwd(), worktree_dir, target_branch=args.target_branch)

    force_diag = diag_env_flag or not trading_day or now.time() >= market_close_time
    if window and window.name == "diagnostic":
        force_diag = True
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

    if shadow_live_flag and action in {"run", "wait"}:
        action = "shadow_live"
    if action == "diag":
        action = "diag_deep" if diag_level == 2 else "diag_verify"

    dry_run_flag = parse_env_flag("DRY_RUN", default=False)
    disable_live_flag = parse_env_flag("DISABLE_LIVE_TRADING", default=False)
    live_trading_flag = parse_env_flag("LIVE_TRADING_ENABLED", default=False)
    expect_live_flag = env_bool("EXPECT_LIVE_TRADING", False)
    mode = resolve_mode(os.getenv("STRATEGY_MODE", ""))
    dry_run_reasons: list[str] = []
    order_mode = "live"
    if action == "diag_verify":
        order_mode = "dry_run"
    elif action in {"diag_deep", "shadow_live"}:
        order_mode = "shadow"
    if non_trading_day:
        dry_run_reasons.append("non_trading_day")
        os.environ["PB1_ENTRY_ENABLED"] = "0"
        os.environ["DIAGNOSTIC_FORCE_RUN"] = "1"
        if order_mode != "shadow":
            os.environ["DISABLE_LIVE_TRADING"] = "1"
            os.environ["DRY_RUN"] = "1"
            os.environ["LIVE_TRADING_ENABLED"] = "0"
            order_mode = "dry_run"
    diag_enabled = force_diag or diag_env_flag or action.startswith("diag")
    if force_diag and not non_trading_day and now.time() >= market_close_time:
        dry_run_reasons.append("market_closed")
    if diag_enabled and order_mode == "dry_run":
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

    dry_run = bool(dry_run_reasons) or order_mode == "dry_run"
    if order_mode == "shadow":
        dry_run = False
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

    def _apply_env_flags(order_mode_value: str) -> None:
        os.environ["DRY_RUN"] = "1" if order_mode_value == "dry_run" else "0"
        if order_mode_value == "shadow":
            os.environ["DISABLE_LIVE_TRADING"] = "1" if disable_live_flag.value else "0"
            os.environ["LIVE_TRADING_ENABLED"] = "1" if (live_trading_flag.value and not disable_live_flag.value) else "0"
        else:
            os.environ["DISABLE_LIVE_TRADING"] = "1" if (order_mode_value == "dry_run" or disable_live_flag.value or non_trading_day) else "0"
            os.environ["LIVE_TRADING_ENABLED"] = "1" if (live_trading_flag.value and not non_trading_day and order_mode_value == "live") else "0"
        os.environ["STRATEGY_MODE"] = mode
        os.environ["ORDER_MODE"] = order_mode_value

    _apply_env_flags(order_mode)

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

    if action in {"diag_verify", "diag_deep"}:
        if action == "diag_verify":
            dry_run = True
            order_mode = "dry_run"
        else:
            dry_run = False
            order_mode = "shadow"
        dry_run_reason = dry_run_reason if dry_run_reason else "diagnostic"
        dry_run_reasons = dry_run_reasons or ["diagnostic"]
        diag_enabled = True
        if args.phase == "auto":
            phase_override_arg = "verify"
        window = window or WindowDecision(name="diagnostic", phase=phase_override_arg or "verify")
        _apply_env_flags(order_mode)

    window_name_for_log = window.name if window else "none"
    phase_for_log = window.phase if window and hasattr(window, "phase") else "none"

    logger.info(
        "[PB1][RUN-START] event=%s now_kst=%s trading_day=%s window=%s phase=%s DRY_RUN=%s DISABLE_LIVE_TRADING=%s LIVE_TRADING_ENABLED=%s STRATEGY_MODE=%s PB1_ENTRY_ENABLED=%s ORDER_MODE=%s reasons=%s",
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
        order_mode,
        dry_run_reasons or ["live"],
    )

    if non_trading_day:
        logger.info("[PB1][SKIP] non-trading-day(%s) → diagnostics/dry-run reason=%s", now.date(), dry_run_reason)
        if diag_enabled:
            logger.warning("[PB1][DIAG] non-trading-day(%s) but running diagnostics", now.date())

    owner = os.getenv("GITHUB_ACTOR", "local")
    run_id = os.getenv("GITHUB_RUN_ID", "local")
    lock_acquired = acquire_lock(worktree_dir, owner=owner, run_id=run_id, ttl_sec=BOTSTATE_LOCK_TTL_SEC)
    if not lock_acquired:
        logger.warning("[BOTSTATE][LOCKED] owner=%s run_id=%s", owner, run_id)
        return

    os.environ["STATE_PATH"] = str(worktree_dir / "trader" / "state" / "state.json")
    from trader import state_store as runtime_state_store
    state_dir = Path(os.environ["STATE_PATH"]).parent
    state_dir.mkdir(parents=True, exist_ok=True)
    state_target_path = Path(os.environ["STATE_PATH"])

    runtime_state = {}
    kis: KisAPI | None = None
    try:
        runtime_state = runtime_state_store.load_state()
        kis = KisAPI()
        balance = kis.get_balance()
        runtime_state = runtime_state_store.reconcile_with_kis_balance(runtime_state, balance, active_strategies={1})
        runtime_state_store.save_state(runtime_state)
    except Exception:
        logger.exception("[PB1] runtime state reconcile failed")
        runtime_state = runtime_state or runtime_state_store.load_state()
        dry_run_reasons.append("kis_init_failed")
        dry_run_reason = ",".join(dry_run_reasons)
        dry_run = True
        _apply_env_flags(dry_run)

    if DIAGNOSTIC_ONLY:
        logger.info("[PB1][DIAG] diagnostic_only mode -> exit")
        release_lock(worktree_dir, run_id=run_id)
        return

    touched: list[Path] = []
    try:
        engine = PB1Engine(
            kis=kis,
            worktree_dir=worktree_dir,
            window=window,
            phase_override=phase_override_arg,
            dry_run=dry_run,
            env="shadow" if order_mode == "shadow" else "paper" if dry_run else kis.env if kis else "paper",
            run_id=run_id,
            order_mode=order_mode,
            diag_level=diag_level,
        )
        touched = engine.run()
        if state_target_path.exists():
            touched.append(state_target_path)
        logger.info("[PB1] run complete touched=%s", touched)
        persist_run_files(
            worktree_dir,
            touched,
            message=f"pb1 ledger run_id={run_id} window={window.name} phase={engine.phase}",
        )
    finally:
        release_lock(worktree_dir, run_id=run_id)


if __name__ == "__main__":
    main()
