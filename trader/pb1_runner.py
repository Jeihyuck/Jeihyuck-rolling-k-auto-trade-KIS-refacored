from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

from trader.kis_wrapper import KisAPI
from trader.time_utils import is_trading_day, now_kst
from trader.config import (
    BOTSTATE_LOCK_TTL_SEC,
    DIAGNOSTIC_MODE,
    DIAGNOSTIC_ONLY,
    MORNING_WINDOW_START,
    MORNING_WINDOW_END,
    MORNING_EXIT_START,
    MORNING_EXIT_END,
    AFTERNOON_WINDOW_START,
    AFTERNOON_WINDOW_END,
    CLOSE_AUCTION_START,
    CLOSE_AUCTION_END,
)
from trader.utils.env import env_bool, parse_env_flag, resolve_mode
from trader.botstate_sync import acquire_lock, release_lock, setup_worktree, persist_run_files
from trader.pb1_engine import PB1Engine
from trader.window_router import decide_window

logger = logging.getLogger(__name__)


def truthy(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PB1 close pullback runner")
    parser.add_argument("--window", default="auto", choices=["auto", "morning", "afternoon"], help="Execution window override")
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
    diag_enabled = (
        env_bool("DIAGNOSTIC_FORCE_RUN", False)
        or env_bool("DIAGNOSTIC_ONLY", DIAGNOSTIC_ONLY)
        or env_bool("DIAGNOSTIC_MODE", DIAGNOSTIC_MODE)
    )
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
    guard_live = expect_live_flag and trading_day and not diag_enabled and not dry_run
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

    def _sleep_until_morning_window() -> bool:
        try:
            hh, mm = MORNING_WINDOW_START.split(":")
            start_dt = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
        except Exception:
            return True
        delta = start_dt - now
        if delta.total_seconds() <= 0:
            return True
        if not trading_day or event_name_lower not in {"push", "workflow_dispatch"}:
            return True
        max_wait_min = int(os.getenv("MAX_WAIT_BEFORE_MORNING_MIN", "120") or "120")
        if delta > timedelta(minutes=max_wait_min):
            logger.info("[PB1][WAIT-SKIP] delta_min=%.1f max_wait_min=%s -> exit early", delta.total_seconds() / 60, max_wait_min)
            return False
        logger.info("[PB1][WAIT] waiting until morning window start delta_sec=%.0f", delta.total_seconds())
        time.sleep(delta.total_seconds())
        return True

    if not _sleep_until_morning_window():
        return
    now = now_kst()

    os.environ.setdefault("MORNING_WINDOW_START", MORNING_WINDOW_START)
    os.environ.setdefault("MORNING_WINDOW_END", MORNING_WINDOW_END)
    os.environ.setdefault("MORNING_EXIT_START", MORNING_EXIT_START)
    os.environ.setdefault("MORNING_EXIT_END", MORNING_EXIT_END)
    os.environ.setdefault("AFTERNOON_WINDOW_START", AFTERNOON_WINDOW_START)
    os.environ.setdefault("AFTERNOON_WINDOW_END", AFTERNOON_WINDOW_END)
    os.environ.setdefault("CLOSE_AUCTION_START", CLOSE_AUCTION_START)
    os.environ.setdefault("CLOSE_AUCTION_END", CLOSE_AUCTION_END)

    worktree_dir = Path("_botstate")
    setup_worktree(Path.cwd(), worktree_dir, target_branch=args.target_branch)

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
        return

    window = decide_window(now=now, override=args.window)
    if window is None and diag_enabled:
        window_name_for_log = "diagnostic"
        phase_for_log = "verify"
    else:
        phase_for_log = window.phase if window and hasattr(window, "phase") else "none"
        window_name_for_log = window.name if window else "none"
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
    if window is None and not diag_enabled:
        logger.info("[PB1][WINDOW] outside active windows override=%s now=%s", args.window, now)
        return
    if window is None and diag_enabled:
        window = window  # keep None, but allow diagnostic flow below

    if non_trading_day:
        logger.info("[PB1][SKIP] non-trading-day(%s) → diagnostics/dry-run reason=%s", now.date(), dry_run_reason)
        if diag_enabled:
            logger.warning("[PB1][DIAG] non-trading-day(%s) but running diagnostics", now.date())

    owner = os.getenv("GITHUB_ACTOR", "local")
    run_id = os.getenv("GITHUB_RUN_ID", "local")
    if not acquire_lock(worktree_dir, owner=owner, run_id=run_id, ttl_sec=BOTSTATE_LOCK_TTL_SEC):
        logger.warning("[BOTSTATE][LOCKED] owner=%s run_id=%s", owner, run_id)
        return

    touched: list[Path] = []
    try:
        engine = PB1Engine(
            kis=kis,
            worktree_dir=worktree_dir,
            window=window,
            phase_override=args.phase,
            dry_run=dry_run,
            env="paper" if dry_run else kis.env if kis else "paper",
            run_id=run_id,
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
