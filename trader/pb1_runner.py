from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from trader.kis_wrapper import KisAPI
from trader import state_store as runtime_state_store
from trader.time_utils import is_trading_day, now_kst
from trader.config import (
    BOTSTATE_LOCK_TTL_SEC,
    DIAG_ENABLED,
    DIAGNOSTIC_FORCE_RUN,
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
    dry_run_flag = parse_env_flag("DRY_RUN", default=False)
    disable_live_flag = parse_env_flag("DISABLE_LIVE_TRADING", default=False)
    live_trading_flag = parse_env_flag("LIVE_TRADING_ENABLED", default=False)
    expect_live_flag = env_bool("EXPECT_LIVE_TRADING", False)
    allow_live_on_push = truthy(os.getenv("ALLOW_LIVE_ON_PUSH", "0"))
    mode = resolve_mode(os.getenv("STRATEGY_MODE", ""))
    diag_enabled = bool(DIAG_ENABLED or DIAGNOSTIC_FORCE_RUN)

    dry_run_reasons: list[str] = []
    if mode == "INTENT_ONLY":
        dry_run_reasons.append("STRATEGY_MODE=INTENT_ONLY")
    if disable_live_flag.value:
        dry_run_reasons.append("DISABLE_LIVE_TRADING=1")
    if diag_enabled:
        dry_run_reasons.append("diagnostic_mode")
    if not live_trading_flag.value and mode == "LIVE":
        dry_run_reasons.append("LIVE_TRADING_ENABLED=0")
    if dry_run_flag.value:
        dry_run_reasons.append("DRY_RUN=1")
    if mode == "LIVE":
        if event_name_lower == "pull_request":
            dry_run_reasons.append("event=pull_request")
        elif event_name_lower == "push" and not allow_live_on_push:
            dry_run_reasons.append("event=push")
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
    if expect_live_flag:
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

    os.environ["DRY_RUN"] = "1" if dry_run else "0"
    os.environ["DISABLE_LIVE_TRADING"] = "1" if (dry_run or disable_live_flag.value) else "0"
    os.environ["LIVE_TRADING_ENABLED"] = "1" if live_trading_flag.value else "0"
    os.environ["STRATEGY_MODE"] = mode

    if (not trading_day) and (not (DIAG_ENABLED and DIAGNOSTIC_FORCE_RUN)):
        logger.warning("[PB1] 비거래일(%s) → 즉시 종료 dry_run=%s reason=%s", now.date(), dry_run, dry_run_reason)
        return
    if (not trading_day) and diag_enabled:
        logger.warning("[PB1][DIAG] non-trading-day(%s) but running diagnostics", now.date())

    os.environ.setdefault("MORNING_WINDOW_START", MORNING_WINDOW_START)
    os.environ.setdefault("MORNING_WINDOW_END", MORNING_WINDOW_END)
    os.environ.setdefault("MORNING_EXIT_START", MORNING_EXIT_START)
    os.environ.setdefault("MORNING_EXIT_END", MORNING_EXIT_END)
    os.environ.setdefault("AFTERNOON_WINDOW_START", AFTERNOON_WINDOW_START)
    os.environ.setdefault("AFTERNOON_WINDOW_END", AFTERNOON_WINDOW_END)
    os.environ.setdefault("CLOSE_AUCTION_START", CLOSE_AUCTION_START)
    os.environ.setdefault("CLOSE_AUCTION_END", CLOSE_AUCTION_END)

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

    if DIAGNOSTIC_ONLY:
        logger.info("[PB1][DIAG] diagnostic_only mode -> exit")
        return

    window = decide_window(now=now, override=args.window)
    if window is None:
        logger.info("[PB1][WINDOW] outside active windows override=%s now=%s", args.window, now)
        return

    worktree_dir = Path("_botstate")
    setup_worktree(Path.cwd(), worktree_dir, target_branch=args.target_branch)
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
