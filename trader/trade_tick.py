"""
Backward-compatibility alias module for trade_tick.

This module exists to prevent "No module named trader.trade_tick" errors.
It simply delegates to the actual trade tick implementation in pb1_runner.

Usage:
    python -m trader.trade_tick
"""

from __future__ import annotations

import argparse
import sys
import os
import logging
import time as _time_mod
from datetime import datetime, time as _dtime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_KST = timezone(timedelta(hours=9))


def _apply_prewarm_guard(now_override: datetime | None = None) -> int | None:
    """Prewarm double-defense guard (section 10).

    Reads env vars set by GitHub Actions workflow:
      PB1_PREWARM_ENABLED   – "1" to activate
      PB1_TARGET_START_TIME – "HH:MM"  trade loop target start time
      PB1_START_ALLOW_UNTIL – "HH:MM"  stale-skip threshold
      PB1_SESSION_KIND      – "am" / "pm" / "close"  (for logging)

    Returns:
        None  – proceed normally into trade loop
        0     – exit clean (OK_NO_TRADE: non-trading day or stale skip)
    """
    if os.getenv("PB1_PREWARM_ENABLED", "0") != "1":
        return None

    now: datetime = now_override if now_override is not None else datetime.now(tz=_KST)
    session = os.getenv("PB1_SESSION_KIND", "")

    # 비거래일 (주말) guard
    if now.weekday() >= 5:  # 5=Sat, 6=Sun
        compute_only = os.getenv("PB1_COMPUTE_ONLY", "0") in {"1", "true"}
        if compute_only:
            logger.info(
                "[PB1][SESSION_GUARD][NONTRADING_DAY_COMPUTE_ONLY] trading_day=0 order_allowed=0 reason=NON_TRADING_DAY session=%s",
                session,
            )
            return None  # 로직 계속, 주문만 차단
        logger.info(
            "[PB1][SESSION_GUARD][NONTRADING_DAY_EXIT] session=%s dow=%s",
            session,
            now.weekday(),
        )
        logger.info(
            "[RUN_SUMMARY][RESULT] status=OK_NO_TRADE reason=NON_TRADING_DAY session=%s",
            session,
        )
        logger.info("[PB1][EXIT] reason=non_trading_day")
        return 0

    target_raw = os.getenv("PB1_TARGET_START_TIME", "")
    allow_until_raw = os.getenv("PB1_START_ALLOW_UNTIL", "")
    
    # Select correct session_end based on session_kind
    session = os.getenv("PB1_SESSION_KIND", "").strip().lower()
    if session == "close":
        session_end_raw = os.getenv("PB1_CLOSE_SESSION_END", "")
    elif session in ("afternoon", "pm"):
        session_end_raw = os.getenv("PB1_PM_SESSION_END", "")
    elif session == "am":
        session_end_raw = os.getenv("PB1_AM_SESSION_END", "")
    else:
        # Fallback check all
        session_end_raw = os.getenv("PB1_PM_SESSION_END") or os.getenv("PB1_CLOSE_SESSION_END") or os.getenv("PB1_AM_SESSION_END", "")
    
    if not target_raw:
        return None

    try:
        target_time = _dtime.fromisoformat(target_raw)
    except ValueError:
        logger.warning(
            "[PB1][PREWARM][PARSE_ERROR] target=%s – skipping guard",
            target_raw,
        )
        return None
    
    # Parse allow_until if provided (used for warnings only, not hard skip)
    allow_until_time = None
    if allow_until_raw:
        try:
            allow_until_time = _dtime.fromisoformat(allow_until_raw)
        except ValueError:
            pass
    
    # Parse session_end - this is the real deadline
    session_end_time = None
    if session_end_raw:
        try:
            session_end_time = _dtime.fromisoformat(session_end_raw)
        except ValueError:
            pass

    # minute-precision comparison (seconds dropped)
    now_time = now.time().replace(second=0, microsecond=0)

    # CRITICAL: Only skip if past actual session end, not START_ALLOW_UNTIL
    # START_ALLOW_UNTIL is for warnings/degraded marking, not hard skip
    if session_end_time and now_time >= session_end_time:
        logger.info(
            "[PB1][PREWARM][STALE_SKIP] now=%s session_end=%s session=%s reason=past_session_end",
            now.strftime("%H:%M"),
            session_end_raw,
            session,
        )
        logger.info(
            "[RUN_SUMMARY][RESULT] status=OK_NO_TRADE reason=SKIP_PHASE_WINDOW session=%s",
            session,
        )
        return 0
    
    # Allow late start with warning if past START_ALLOW_UNTIL but before session end
    if allow_until_time and now_time > allow_until_time:
        if session_end_time and now_time < session_end_time:
            logger.warning(
                "[PB1][PREWARM][LATE_START_ALLOWED] now=%s allow_until=%s session_end=%s session=%s action=run_with_warning",
                now.strftime("%H:%M"),
                allow_until_raw,
                session_end_raw,
                session,
            )
            # Continue to run, just mark as late
            os.environ["PB1_LATE_START_WARNING"] = "1"
        else:
            # Past session end
            logger.info(
                "[PB1][PREWARM][STALE_SKIP] now=%s session_end=%s session=%s",
                now.strftime("%H:%M"),
                session_end_raw or "unknown",
                session,
            )
            logger.info(
                "[RUN_SUMMARY][RESULT] status=OK_NO_TRADE reason=SKIP_PHASE_WINDOW session=%s",
                session,
            )
            return 0

    # 목표 시간 전: target까지 대기
    if now_time < target_time:
        target_dt = datetime.combine(now.date(), target_time).replace(tzinfo=_KST)
        wait_sec = (target_dt - now).total_seconds()
        if wait_sec > 0:
            logger.info(
                "[PB1][PREWARM][WAIT] wait_sec=%.0f session=%s now=%s target=%s",
                wait_sec,
                session,
                now.strftime("%H:%M"),
                target_raw,
            )
            _time_mod.sleep(wait_sec)

    return None


def _apply_late_start_exit_only_policy() -> None:
    """Force PM late-start EXIT_ONLY_NO_NEW_BUY policy through launcher env."""
    reason = (os.getenv("FORCE_ENTRY_DISABLED_REASON") or "").strip()
    late_no_buy = os.getenv("PB1_LATE_START_NO_NEW_BUY", "0") in {"1", "true", "TRUE", "yes", "YES"}
    action = (os.getenv("PM_LATE_START_ACTION") or "").strip().upper()
    if reason == "PM_LATE_START_NO_NEW_BUY" or (late_no_buy and action == "EXIT_ONLY_NO_NEW_BUY"):
        os.environ["FORCE_ENTRY_DISABLED_REASON"] = "PM_LATE_START_NO_NEW_BUY"
        os.environ["PB1_ENTRY_ENABLED"] = "0"
        os.environ["ENTRY_ENABLED"] = "0"
        os.environ["ALLOW_NEW_BUY"] = "0"
        os.environ["PB1_PHASE_DEFAULT"] = "exit"
        os.environ["FORCE_PB1_PHASE"] = "exit"
        logger.info("[ENTRY][DISABLED] reason=PM_LATE_START_NO_NEW_BUY action=launcher_force_exit_only")
        logger.info("[EXIT][ENABLED] reason=late_start_exit_only")


def _verify_log_cli(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="python -m trader.trade_tick verify-log")
    parser.add_argument("--session", required=True, choices=["am", "pm", "close"])
    parser.add_argument("--log", required=True)
    args = parser.parse_args(argv)

    from trader.pb1_runner import evaluate_workflow_log_success

    text = Path(args.log).read_text(encoding="utf-8")
    result = evaluate_workflow_log_success(session=args.session, log_text=text)
    prefix = {
        "am": "TRADE_AM",
        "pm": "TRADE_PM",
        "close": "TRADE_CLOSE",
    }[args.session]
    print(
        f"[{prefix}][VERIFY][COUNTS] fatal_count={0 if result['ok'] else 1} "
        f"timeout_count={result['timeout_count']} db_autocommit_count={result['db_autocommit_count']} "
        f"degraded_count={result['degraded_count']}"
    )
    if result["timeout_count"] > 0:
        print(f"[{prefix}][VERIFY][WARN] timeout_count={result['timeout_count']}")
    if result["db_autocommit_count"] > 0:
        print(f"[{prefix}][VERIFY][WARN] db_autocommit_count={result['db_autocommit_count']}")
    if result["degraded_count"] > 0:
        print(f"[{prefix}][VERIFY][WARN] degraded_count={result['degraded_count']}")
    if result.get("exit_pass_timeout_observed"):
        print(f"[{prefix}][VERIFY][WARN] exit_pass_timeout_observed=1")
    print(f"[{prefix}][VERIFY][RESULT] status={result['status']} reason={result['reason']}")
    return 0 if result["ok"] else 1


def main() -> int:
    """Run the PB1 trade tick pipeline."""
    if len(sys.argv) > 1 and sys.argv[1] == "verify-log":
        return _verify_log_cli(sys.argv[2:])
    os.environ.setdefault("PB1_TICK_HARD_TIMEOUT_SEC", "90")
    os.environ.setdefault("PB1_LAST_STAGE", "trade_tick.bootstrap")
    compute_only = os.getenv("PB1_COMPUTE_ONLY", "0") in {"1", "true"}
    order_allowed = os.getenv("ORDER_ALLOWED", "1") in {"1", "true"}
    order_block_reason = os.getenv("PB1_ORDER_BLOCK_REASON", "")
    session = os.getenv("PB1_SESSION_KIND", "")
    logger.info("[TRADE][BOOT][START] module=trade_tick_alias")
    logger.info(
        "[TRADE][BOOT][ENV] MODE=%s STRATEGY_ENV=%s KIS_ENV=%s FAIL_IF_POOL_MISSING=%s CANDIDATE_POOL_STRATEGY=%s PB1_PHASE_DEFAULT=%s PB1_TICK_HARD_TIMEOUT_SEC=%s PB1_LAST_STAGE=%s",
        os.getenv("MODE", "trade"),
        os.getenv("STRATEGY_ENV", ""),
        os.getenv("KIS_ENV", ""),
        os.getenv("FAIL_IF_POOL_MISSING", ""),
        os.getenv("CANDIDATE_POOL_STRATEGY_KEY", ""),
        os.getenv("PB1_PHASE_DEFAULT", ""),
        os.getenv("PB1_TICK_HARD_TIMEOUT_SEC", "90"),
        os.getenv("PB1_LAST_STAGE", "trade_tick.bootstrap"),
    )
    _apply_late_start_exit_only_policy()
    logger.info(
        "[TRADE][BOOT][SESSION_POLICY] force_trade_session=%s forced_trade_session=%s force_entry_window_override=%s session_recovery_continue=%s phase_guard_classification=%s",
        os.getenv("PB1_FORCE_TRADE_SESSION", "auto"),
        os.getenv("PB1_FORCED_TRADE_SESSION", ""),
        os.getenv("PB1_FORCE_ENTRY_WINDOW_OVERRIDE", "0"),
        os.getenv("PB1_SESSION_RECOVERY_CONTINUE", "0"),
        os.getenv("PB1_PHASE_GUARD_CLASSIFICATION", ""),
    )
    guard_result = _apply_prewarm_guard()
    if guard_result is not None:
        return guard_result
    if compute_only:
        logger.info(
            "[PB1][SESSION_GUARD][COMPUTE_ONLY_MODE] compute_only=1 order_allowed=%d reason=%s session=%s",
            int(order_allowed),
            order_block_reason or "NON_TRADING_DAY",
            session,
        )

    from trader.pb1_runner import main as pb1_main
    rc = pb1_main()
    if compute_only and not order_allowed:
        logger.info(
            "[RUN_SUMMARY][RESULT] status=OK_COMPUTE_ONLY reason=NON_TRADING_DAY_COMPUTE_ONLY session=%s",
            session,
        )
    return rc


if __name__ == "__main__":
    sys.exit(main())
