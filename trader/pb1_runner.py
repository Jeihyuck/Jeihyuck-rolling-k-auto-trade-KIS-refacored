from __future__ import annotations

import argparse
import logging
import os
import signal
import time as time_mod
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from trader.config import (
    AFTERNOON_WINDOW_END,
    AFTERNOON_WINDOW_START,
    BOTSTATE_LOCK_TTL_SEC,
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
    resolve_strategy_mode,
)
from trader.botstate_sync import acquire_lock as acquire_botstate_lock
from trader.botstate_sync import persist_run_files, release_lock as release_botstate_lock
from trader.botstate_sync import resolve_botstate_worktree_dir, setup_worktree
from trader.db.engine import make_engine
from trader.db.lock import release_lock, try_acquire_lock
from trader.db.migrate import run_migrations
from trader.db.repos import FillsRepo, OrdersRepo, PositionsRepo, RunsRepo, UniverseRepo
from trader.kis_wrapper import KisAPI
from trader.pb1_engine import PB1Engine, resolve_pb1_phase
from trader.time_utils import now_kst
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


def _parse_now_override(raw: str | None, label: str) -> datetime | None:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("Asia/Seoul"))
        else:
            dt = dt.astimezone(ZoneInfo("Asia/Seoul"))
        return dt
    except Exception:
        logger.warning("[PB1][SMOKE] invalid %s=%s", label, raw)
        return None


def _get_now_kst() -> datetime:
    override = _parse_now_override(os.getenv("NOW_KST_OVERRIDE"), "NOW_KST_OVERRIDE")
    if override:
        return override
    simulated = _parse_now_override(os.getenv("PB1_SIMULATE_NOW_KST"), "PB1_SIMULATE_NOW_KST")
    if simulated:
        return simulated
    return now_kst()


def _decide_action(now: datetime, trading_day: bool, open_dt: datetime, close_dt: datetime, allow_wait: bool, max_wait_s: int, smoke_enabled: bool) -> tuple[str, datetime | None]:
    if smoke_enabled:
        return "smoke", None
    if not trading_day:
        return "smoke", None
    if now < open_dt:
        remaining = (open_dt - now).total_seconds()
        if allow_wait and remaining <= max_wait_s:
            return "wait", open_dt
        return "smoke", open_dt
    if now >= close_dt:
        return "smoke", None
    return "run", None


def _log_balance_cache(force: bool) -> None:
    logger.info("[BALANCE][CACHE] hit=%s", not force)


def _run_smoke(engine, kis_env: str, now: datetime) -> None:
    token_ok = balance_ok = universe_ok = pretrade_ok = False
    kis: KisAPI | None = None
    members: list[dict] = []
    try:
        kis = KisAPI()
        token_ok = True
    except Exception as exc:
        logger.warning("[SMOKE][FAIL] token_init err=%s", exc)

    if kis:
        try:
            _log_balance_cache(force=True)
            snap = kis.get_balance_cached(force=True)
            balance_ok = bool(snap)
        except Exception:
            logger.exception("[SMOKE][FAIL] balance")

    repo = UniverseRepo(engine)
    try:
        members = repo.get_latest_universe_members(kis_env, "best_k_meta")
        if not members:
            from trader.universe import build as universe_build

            universe_build.build_universe(as_of_date=now.date().isoformat(), env=kis_env, strategy="best_k_meta")
            members = repo.get_latest_universe_members(kis_env, "best_k_meta")
        universe_ok = bool(members)
    except Exception:
        logger.exception("[SMOKE][FAIL] universe")

    if kis:
        try:
            code = members[0]["code"] if members else "005930"
            quote = kis.get_price_quote(code, diag_mode=True, attempts=1)
            pretrade_ok = bool(quote)
        except Exception:
            logger.exception("[SMOKE][FAIL] pretrade")

    status = token_ok and balance_ok and universe_ok and pretrade_ok
    if status:
        logger.info(
            "[SMOKE][PASS] token=%s balance=%s universe=%s pretrade=%s",
            token_ok,
            balance_ok,
            universe_ok,
            pretrade_ok,
        )
    else:
        logger.warning(
            "[SMOKE][FAIL] token=%s balance=%s universe=%s pretrade=%s",
            token_ok,
            balance_ok,
            universe_ok,
            pretrade_ok,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PB1 close pullback runner")
    parser.add_argument("--window", default="auto", choices=["auto", "morning", "afternoon"], help="Execution window override")
    parser.add_argument("--phase", default="auto", choices=["auto", "entry", "exit", "verify"], help="Phase override")
    return parser.parse_args()


def _parse_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("[PB1][ENV] invalid %s=%s fallback=%s", name, raw, default)
        return default


def _resolve_lock_ttl(max_seconds: int) -> tuple[int, int]:
    buffer_sec = _parse_int_env("BOTSTATE_LOCK_TTL_BUFFER_SEC", 180)
    base_sec = max_seconds if max_seconds > 0 else BOTSTATE_LOCK_TTL_SEC
    ttl_sec = base_sec + buffer_sec
    return ttl_sec, buffer_sec


def _collect_botstate_files(since_ts: float) -> list[Path]:
    base_dir = Path("bot_state")
    if not base_dir.exists():
        return []
    touched: list[Path] = []
    for path in base_dir.rglob("*"):
        if not path.is_file():
            continue
        try:
            if path.stat().st_mtime >= since_ts:
                touched.append(path)
        except FileNotFoundError:
            continue
    return touched


def _is_urgent_persist(touched: list[Path]) -> bool:
    keywords = {"orders", "fills", "exits_intent"}
    for path in touched:
        parts = {part.lower() for part in path.parts}
        if parts & keywords:
            return True
    return False


def _setup_botstate_session(owner: str, run_id: str, ttl_sec: int) -> Path | None:
    worktree_dir = resolve_botstate_worktree_dir()
    try:
        setup_worktree(Path.cwd(), worktree_dir)
    except Exception:
        logger.exception("[BOTSTATE][SETUP] failed worktree=%s", worktree_dir)
        return None
    if not acquire_botstate_lock(worktree_dir, owner=owner, run_id=run_id, ttl_sec=ttl_sec):
        return None
    return worktree_dir


def run_once(
    *,
    args: argparse.Namespace,
    engine,
    loop_mode: bool = False,
    window: WindowDecision | None = None,
) -> tuple[list[Path], bool, dict[str, int], str]:
    now = _get_now_kst()
    smoke_enabled = os.getenv("PB1_SMOKE_RUN") == "1"
    event_name = os.getenv("GITHUB_EVENT_NAME", "") or ""
    event_name_lower = event_name.lower()
    mode, trading_day, market_window, mode_source = resolve_strategy_mode(
        now_kst=now,
        force_mode_env=os.getenv("FORCE_STRATEGY_MODE"),
    )
    effective_mode = mode
    if smoke_enabled:
        trading_day = True
    logger.info(
        "[MODE_DECISION] source=%s now_kst=%s trading_day=%s window=%s mode=%s",
        mode_source,
        now.isoformat(),
        trading_day,
        market_window,
        mode,
    )
    os.environ["STRATEGY_MODE"] = mode
    diag_env_flag = (
        env_bool("DIAGNOSTIC_FORCE_RUN", False)
        or env_bool("DIAGNOSTIC_ONLY", DIAGNOSTIC_ONLY)
        or env_bool("DIAGNOSTIC_MODE", DIAGNOSTIC_MODE)
    )
    open_dt, close_dt = _market_session(now)
    allow_wait = env_bool("PB1_ALLOW_WAIT", env_bool("PB1_WAIT_FOR_WINDOW", PB1_WAIT_FOR_WINDOW))
    max_wait_s = int(PB1_MAX_WAIT_FOR_WINDOW_MIN) * 60
    window = window or decide_window(now=now, override=args.window)
    window_name_for_log = window.name if window else "none"

    entry_flag = parse_env_flag("PB1_ENTRY_ENABLED", default=True)
    os.environ["PB1_ENTRY_ENABLED"] = "1" if entry_flag.value else "0"
    force_phase_env = os.getenv("FORCE_PB1_PHASE") or ""
    phase_seed = force_phase_env if force_phase_env else (None if args.phase == "auto" else args.phase)
    resolved_phase, phase_reason, phase_window = resolve_pb1_phase(now, trading_day, phase_seed)
    phase_for_log = resolved_phase

    os.environ.setdefault("MORNING_WINDOW_START", MORNING_WINDOW_START)
    os.environ.setdefault("MORNING_WINDOW_END", MORNING_WINDOW_END)
    os.environ.setdefault("MORNING_EXIT_START", MORNING_EXIT_START)
    os.environ.setdefault("MORNING_EXIT_END", MORNING_EXIT_END)
    os.environ.setdefault("AFTERNOON_WINDOW_START", AFTERNOON_WINDOW_START)
    os.environ.setdefault("AFTERNOON_WINDOW_END", AFTERNOON_WINDOW_END)
    os.environ.setdefault("CLOSE_AUCTION_START", CLOSE_AUCTION_START)
    os.environ.setdefault("CLOSE_AUCTION_END", CLOSE_AUCTION_END)

    action = "run"
    target_start = None
    if not loop_mode:
        action, target_start = _decide_action(now, trading_day, open_dt, close_dt, allow_wait, max_wait_s, smoke_enabled)
        logger.info(
            "[PB1][RUN-PLAN] event=%s now_kst=%s trading_day=%s action=%s target_start=%s max_wait_s=%s window=%s phase=%s allow_wait=%s",
            event_name_lower or "unknown",
            now.isoformat(),
            trading_day,
            action,
            target_start.isoformat() if target_start else "none",
            max_wait_s,
            window_name_for_log,
            phase_for_log,
            allow_wait,
        )

        if action == "wait" and target_start:
            while True:
                now = _get_now_kst()
                remaining = (target_start - now).total_seconds()
                if remaining <= 0:
                    break
                if remaining > max_wait_s:
                    logger.info(
                        "[PB1][WAIT] now_kst=%s next_open_kst=%s sleeping_s=0 reason=exceeds_max",
                        now.isoformat(),
                        target_start.isoformat(),
                    )
                    action = "smoke"
                    break
                sleep_for = remaining if remaining < 30 else min(60, remaining)
                logger.info("[PB1][WAIT] now_kst=%s next_open_kst=%s sleeping_s=%.0f", now.isoformat(), target_start.isoformat(), sleep_for)
                time_mod.sleep(sleep_for)
            if action == "wait":
                now = _get_now_kst()
                mode, trading_day, market_window, mode_source = resolve_strategy_mode(
                    now_kst=now,
                    force_mode_env=os.getenv("FORCE_STRATEGY_MODE"),
                )
                effective_mode = mode
                os.environ["STRATEGY_MODE"] = mode
                window = decide_window(now=now, override=args.window)
                window_name_for_log = window.name if window else "none"
                resolved_phase, phase_reason, phase_window = resolve_pb1_phase(now, trading_day, phase_seed)
                phase_for_log = resolved_phase
                logger.info(
                    "[MODE_DECISION] source=%s now_kst=%s trading_day=%s window=%s mode=%s",
                    mode_source,
                    now.isoformat(),
                    trading_day,
                    market_window,
                    mode,
                )
                logger.info(
                    "[PB1][PHASE] now=%s window=%s phase=%s reason=%s entry_enabled=%s",
                    now.isoformat(),
                    phase_window,
                    resolved_phase,
                    phase_reason,
                    entry_flag.value,
                )
                action = "run" if trading_day else "smoke"
                logger.info("[PB1][WAIT][DONE] now_kst=%s window=%s phase=%s", now.isoformat(), window_name_for_log, phase_for_log)
    elif not trading_day:
        logger.info("[PB1][LOOP] non-trading-day -> skip")
        return [], False, {}, phase_for_log

    if action == "smoke":
        _run_smoke(engine, kis_env=(os.getenv("KIS_ENV") or "practice").lower(), now=now)
        return [], False, {}, phase_for_log

    if not window:
        logger.info("[PB1][WINDOW] outside active windows override=%s now=%s", args.window, now)
        return [], False, {}, phase_for_log

    non_trading_day = not trading_day
    force_diag = diag_env_flag

    dry_run_flag = parse_env_flag("DRY_RUN", default=False)
    disable_live_flag = parse_env_flag("DISABLE_LIVE_TRADING", default=False)
    live_trading_flag = parse_env_flag("LIVE_TRADING_ENABLED", default=False)
    expect_live_flag = env_bool("EXPECT_LIVE_TRADING", False)
    mode_resolved = resolve_mode(os.getenv("STRATEGY_MODE", ""))
    dry_run_reasons: list[str] = []
    diag_enabled = force_diag
    if diag_enabled:
        dry_run_reasons.append("diagnostic_mode")
    if mode_resolved == "INTENT_ONLY":
        dry_run_reasons.append("STRATEGY_MODE=INTENT_ONLY")
    if parse_env_flag("DISABLE_LIVE_TRADING", default=disable_live_flag.value).value:
        dry_run_reasons.append("DISABLE_LIVE_TRADING=1")
    live_trading_flag = parse_env_flag("LIVE_TRADING_ENABLED", default=live_trading_flag.value)
    disable_live_flag = parse_env_flag("DISABLE_LIVE_TRADING", default=disable_live_flag.value)
    dry_run_flag = parse_env_flag("DRY_RUN", default=dry_run_flag.value)
    if not live_trading_flag.value and mode_resolved == "LIVE":
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
    if smoke_enabled:
        logger.info(
            "[PB1][SMOKE] enabled=True simulated_now_kst=%s force_dry_run=True",
            now.isoformat(),
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
        if mode_resolved != "LIVE":
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
        os.environ["DISABLE_LIVE_TRADING"] = "1" if disable_live_flag.value else "0"
        os.environ["LIVE_TRADING_ENABLED"] = "1" if live_trading_flag.value else "0"
        os.environ["STRATEGY_MODE"] = effective_mode

    _apply_env_flags(dry_run)

    force_phase_env = os.getenv("FORCE_PB1_PHASE") or ""
    phase_override_arg = resolved_phase
    if (
        window
        and event_name_lower == "push"
        and args.phase == "auto"
        and not force_phase_env
        and window.name == "afternoon"
        and env_bool("PB1_FORCE_ENTRY_ON_PUSH", PB1_FORCE_ENTRY_ON_PUSH)
    ):
        try:
            start = datetime.fromisoformat(f"{now.date()}T{AFTERNOON_WINDOW_START}")
            end = datetime.fromisoformat(f"{now.date()}T{AFTERNOON_WINDOW_END}")
            in_afternoon = start.time() <= now.time() < end.time()
        except Exception:
            in_afternoon = False
        if trading_day and in_afternoon:
            logger.info("[PB1][PHASE_OVERRIDE] event=push from=%s to=entry reason=PB1_FORCE_ENTRY_ON_PUSH", phase_override_arg)
            phase_override_arg = "entry"
            phase_reason = "force_push"

    if diag_enabled:
        dry_run = True
        dry_run_reason = dry_run_reason if dry_run_reason else "diagnostic"
        dry_run_reasons = dry_run_reasons or ["diagnostic"]
        if args.phase == "auto" and not force_phase_env:
            phase_override_arg = "verify"
            phase_reason = "diagnostic"
        window = window or WindowDecision(name="diagnostic", phase=phase_override_arg or "verify")
        _apply_env_flags(dry_run)

    window_name_for_log = window.name if window else "none"
    phase_for_log = phase_override_arg or "none"
    logger.info(
        "[PB1][PHASE] now=%s window=%s phase=%s reason=%s entry_enabled=%s",
        now.isoformat(),
        phase_window,
        phase_for_log,
        phase_reason,
        entry_flag.value,
    )

    logger.info(
        "[PB1][RUN-START] event=%s now_kst=%s trading_day=%s market_window=%s window=%s phase=%s phase_reason=%s DRY_RUN=%s DISABLE_LIVE_TRADING=%s LIVE_TRADING_ENABLED=%s STRATEGY_MODE=%s PB1_ENTRY_ENABLED=%s reasons=%s",
        event_name_lower or "unknown",
        now.isoformat(),
        trading_day,
        market_window,
        window_name_for_log,
        phase_for_log,
        phase_reason,
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
        return [], False, {}, phase_for_log

    runs_repo = RunsRepo(engine)
    universe_repo = UniverseRepo(engine)
    orders_repo = OrdersRepo(engine)
    fills_repo = FillsRepo(engine)
    positions_repo = PositionsRepo(engine)

    run_record_id = None
    did_work = False
    touched_files: list[Path] = []
    result = None
    run_start_ts = time_mod.time()
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
            config_json={
                "dry_run_reasons": dry_run_reasons,
                "run_window": window_name_for_log,
                "phase": phase_override_arg,
                "phase_reason": phase_reason,
            },
        )

        engine_runner = PB1Engine(
            universe_repo=universe_repo,
            orders_repo=orders_repo,
            fills_repo=fills_repo,
            positions_repo=positions_repo,
            kis=kis,
            window=window,
            phase=phase_override_arg,
            dry_run=dry_run,
            env=kis_env or "practice",
            run_id=run_record_id,
        )
        result = engine_runner.run()
        did_work = True
        runs_repo.finish_run(run_record_id, status=result.status, notes=result.notes)
        touched_files = _collect_botstate_files(run_start_ts)
    except Exception as exc:
        logger.exception("[PB1][FAIL] unexpected error")
        if run_record_id:
            runs_repo.finish_run(run_record_id, status="FAILED", notes=str(exc))
        raise
    finally:
        release_lock(engine, lock_key)
    metrics = {
        "balance_api_calls": result.balance_api_calls if result else 0,
        "balance_cache_hits": result.balance_cache_hits if result else 0,
        "balance_tick_cache_hits": result.balance_tick_cache_hits if result else 0,
    }
    return touched_files, did_work, metrics, phase_for_log


def _run_loop(*, args: argparse.Namespace, engine) -> None:
    loop_interval = _parse_int_env("PB1_LOOP_INTERVAL_SEC", 60)
    persist_interval = _parse_int_env("PB1_PERSIST_INTERVAL_SEC", 300)
    loop_max_minutes = _parse_int_env("PB1_LOOP_MAX_MINUTES", 0)
    run_loop_minutes = _parse_int_env("RUN_LOOP_MINUTES", 0)
    now = _get_now_kst()
    _, close_dt = _market_session(now)
    max_seconds = run_loop_minutes * 60 if run_loop_minutes > 0 else 0
    loop_deadline = now + timedelta(seconds=max_seconds) if max_seconds > 0 else None
    if run_loop_minutes > 0:
        loop_max_minutes = run_loop_minutes
        persist_interval = max(120, min(persist_interval, 240))
    logger.info(
        "[PB1][LOOP] enabled interval=%s persist_interval=%s close=%s max_minutes=%s run_loop_minutes=%s",
        loop_interval,
        persist_interval,
        close_dt.isoformat(),
        loop_max_minutes,
        run_loop_minutes,
    )
    logger.info(
        "[PB1][LOOP] start now_kst=%s max_seconds=%s deadline=%s",
        now.isoformat(),
        max_seconds,
        loop_deadline.isoformat() if loop_deadline else "none",
    )

    owner = os.getenv("GITHUB_ACTOR", "local")
    workflow_run_id = os.getenv("GITHUB_RUN_ID", "local")
    ttl_sec, ttl_buffer = _resolve_lock_ttl(max_seconds)
    logger.info(
        "[BOTSTATE][LOCK] ttl_sec=%s max_seconds=%s buffer=%s",
        ttl_sec,
        max_seconds,
        ttl_buffer,
    )
    botstate_worktree = _setup_botstate_session(owner=owner, run_id=workflow_run_id, ttl_sec=ttl_sec)
    if botstate_worktree is None:
        logger.warning("[PB1][LOOP] botstate lock unavailable -> exit")
        return

    pending_touched: dict[Path, Path] = {}
    last_persist_ts = 0.0
    loop_started_ts = time_mod.time()
    exit_reason = "unknown"
    stop_requested = {"value": False}
    balance_api_calls = 0
    balance_cache_hits = 0
    balance_tick_cache_hits = 0
    last_phase = "none"

    def _request_stop(reason: str) -> None:
        if stop_requested["value"]:
            return
        stop_requested["value"] = True
        logger.warning("[PB1][SIGNAL] %s -> stopping loop", reason)
        try:
            release_botstate_lock(botstate_worktree, owner, workflow_run_id)
        except Exception:
            logger.exception("[PB1][SIGNAL] botstate lock release failed")

    signal.signal(signal.SIGTERM, lambda *_args: _request_stop("SIGTERM"))
    try:
        while True:
            if stop_requested["value"]:
                exit_reason = "sigterm"
                break
            now = _get_now_kst()
            if now >= close_dt:
                logger.info("[PB1][LOOP] market closed -> exit")
                exit_reason = "market_closed"
                break
            elapsed_seconds = time_mod.time() - loop_started_ts
            if max_seconds > 0 and elapsed_seconds >= max_seconds:
                logger.info("[PB1][LOOP] max_seconds=%s exiting", max_seconds)
                exit_reason = "loop_timeout"
                break
            if loop_max_minutes > 0:
                elapsed_min = (time_mod.time() - loop_started_ts) / 60
                if elapsed_min >= loop_max_minutes:
                    logger.info("[PB1][LOOP] max minutes reached -> exit elapsed_min=%.1f", elapsed_min)
                    exit_reason = "loop_timeout"
                    break
            window = decide_window(now=now, override=args.window)
            if window is None:
                if max_seconds > 0 and os.getenv("PB1_LOOP_WAIT_OUTSIDE", "0") != "1":
                    next_start = _next_window_start(
                        now,
                        [
                            _parse_hhmm_to_time(MORNING_WINDOW_START),
                            _parse_hhmm_to_time(AFTERNOON_WINDOW_START),
                            _parse_hhmm_to_time(CLOSE_AUCTION_START),
                        ],
                    )
                    logger.info(
                        "[PB1][LOOP] outside window -> exit (tick mode) next=%s",
                        next_start.isoformat() if next_start else "unknown",
                    )
                    exit_reason = "outside_window"
                    break

                next_start = _next_window_start(
                    now,
                    [
                        _parse_hhmm_to_time(MORNING_WINDOW_START),
                        _parse_hhmm_to_time(AFTERNOON_WINDOW_START),
                        _parse_hhmm_to_time(CLOSE_AUCTION_START),
                    ],
                )
                if next_start:
                    sleep_for = (next_start - now).total_seconds()
                else:
                    sleep_for = loop_interval
                sleep_for = max(5.0, min(300.0, sleep_for))

                # FIX: RUN_LOOP_MINUTES(=max_seconds) 예산보다 더 오래 sleep 해야 하면
                # sleep 후 깨어나자마자 loop_timeout으로 끝나 "한 번 돌고 죽는" 것처럼 보인다.
                # 이 경우에는 sleep하지 않고 종료하여 다음 5분 tick(schedule)에 맡긴다.
                if max_seconds > 0:
                    remaining_budget = max_seconds - elapsed_seconds
                    if remaining_budget <= 0:
                        logger.info("[PB1][LOOP] budget exhausted before sleep -> exit")
                        exit_reason = "loop_timeout"
                        break
                    if sleep_for >= remaining_budget:
                        logger.info(
                            "[PB1][LOOP] outside window but insufficient budget -> exit "
                            "sleep=%.0fs remaining_budget=%.0fs next=%s",
                            sleep_for,
                            remaining_budget,
                            next_start.isoformat() if next_start else "unknown",
                        )
                        exit_reason = "outside_window_budget"
                        break
                logger.info(
                    "[PB1][LOOP] outside window -> sleep %.0fs next=%s",
                    sleep_for,
                    next_start.isoformat() if next_start else "unknown",
                )
                time_mod.sleep(sleep_for)
                continue

            logger.info("[PB1][LOOP] tick window=%s phase=%s", window.name, window.phase)
            touched, _did_work, metrics, last_phase = run_once(args=args, engine=engine, loop_mode=True, window=window)
            balance_api_calls += metrics.get("balance_api_calls", 0)
            balance_cache_hits += metrics.get("balance_cache_hits", 0)
            balance_tick_cache_hits += metrics.get("balance_tick_cache_hits", 0)
            for path in touched:
                pending_touched[path] = path
            if pending_touched:
                now_ts = time_mod.time()
                urgent = _is_urgent_persist(list(pending_touched.values()))
                if urgent or (now_ts - last_persist_ts >= persist_interval):
                    persist_run_files(
                        botstate_worktree,
                        list(pending_touched.values()),
                        message=f"pb1 loop {now.isoformat()}",
                    )
                    pending_touched.clear()
                    last_persist_ts = now_ts
            time_mod.sleep(loop_interval)
    finally:
        elapsed = time_mod.time() - loop_started_ts
        if exit_reason == "unknown":
            exit_reason = "shutdown"
        if pending_touched:
            persist_run_files(
                botstate_worktree,
                list(pending_touched.values()),
                message=f"pb1 loop {now_kst().isoformat()}",
            )
        release_botstate_lock(botstate_worktree, owner, workflow_run_id)
        if max_seconds > 0 and elapsed > max_seconds:
            logger.warning(
                "[PB1][EXIT][WARN] reason=%s elapsed=%.1fs max_seconds=%s deadline=%s phase=%s balance_api_calls=%s balance_cache_hits=%s balance_tick_cache_hits=%s",
                exit_reason,
                elapsed,
                max_seconds,
                loop_deadline.isoformat() if loop_deadline else "none",
                last_phase,
                balance_api_calls,
                balance_cache_hits,
                balance_tick_cache_hits,
            )
        else:
            logger.info(
                "[PB1][EXIT] reason=%s elapsed=%.1fs max_seconds=%s deadline=%s phase=%s balance_api_calls=%s balance_cache_hits=%s balance_tick_cache_hits=%s",
                exit_reason,
                elapsed,
                max_seconds,
                loop_deadline.isoformat() if loop_deadline else "none",
                last_phase,
                balance_api_calls,
                balance_cache_hits,
                balance_tick_cache_hits,
            )


def main() -> None:
    args = parse_args()
    engine = make_engine()
    run_migrations(engine)

    smoke_enabled = os.getenv("PB1_SMOKE_RUN") == "1"
    run_loop = os.getenv("PB1_RUN_LOOP", "0") == "1"
    run_loop_minutes = _parse_int_env("RUN_LOOP_MINUTES", 0)
    if run_loop_minutes > 0:
        run_loop = True
    if run_loop and not smoke_enabled:
        _run_loop(args=args, engine=engine)
        return
    if smoke_enabled:
        run_once(args=args, engine=engine, loop_mode=False, window=None)
        return

    owner = os.getenv("GITHUB_ACTOR", "local")
    workflow_run_id = os.getenv("GITHUB_RUN_ID", "local")
    ttl_sec, ttl_buffer = _resolve_lock_ttl(run_loop_minutes * 60)
    logger.info(
        "[BOTSTATE][LOCK] ttl_sec=%s max_seconds=%s buffer=%s",
        ttl_sec,
        run_loop_minutes * 60,
        ttl_buffer,
    )
    botstate_worktree = _setup_botstate_session(owner=owner, run_id=workflow_run_id, ttl_sec=ttl_sec)
    if botstate_worktree is None:
        logger.warning("[PB1][RUN] botstate lock unavailable -> exit")
        return
    signal.signal(
        signal.SIGTERM,
        lambda *_args: release_botstate_lock(botstate_worktree, owner, workflow_run_id),
    )
    metrics: dict[str, int] = {}
    phase_for_log = "none"
    start_ts = time_mod.time()
    try:
        touched, _did_work, metrics, phase_for_log = run_once(args=args, engine=engine, loop_mode=False, window=None)
        if touched:
            persist_run_files(
                botstate_worktree,
                touched,
                message=f"pb1 run {now_kst().isoformat()}",
            )
    finally:
        release_botstate_lock(botstate_worktree, owner, workflow_run_id)
        elapsed = time_mod.time() - start_ts
        logger.info(
            "[PB1][EXIT] reason=single_run elapsed=%.1fs max_seconds=%s deadline=%s phase=%s balance_api_calls=%s balance_cache_hits=%s balance_tick_cache_hits=%s",
            elapsed,
            run_loop_minutes * 60,
            "none",
            phase_for_log,
            metrics.get("balance_api_calls", 0),
            metrics.get("balance_cache_hits", 0),
            metrics.get("balance_tick_cache_hits", 0),
        )


if __name__ == "__main__":
    main()
