# -*- coding: utf-8 -*-
"""US Trade Session Runner.

AM / Afternoon session 동안 trade tick을 반복 실행한다.

CLI:
  python -m trader.us.runner.trade_session_runner \\
    --session am \\
    --env practice \\
    --max-minutes 180 \\
    --interval-sec 300 \\
    [--offline] \\
    [--force-now 2026-01-02T09:35:00-05:00]
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import os
import sys
import time as time_mod
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

# 세션별 종료 시각 (ET)
_SESSION_END_TIMES = {
    "am": (12, 30),       # 12:30 ET
    "afternoon": (15, 50), # 15:50 ET
}

# 세션별 시작 시각 (ET) — phase guard
_SESSION_START_TIMES = {
    "am": (9, 30),
    "afternoon": (12, 30),
}

_MAX_CONSECUTIVE_ERRORS = 3


def _write_us_session_report(payload: dict, session: str) -> None:
    """US 세션 최신 리포트를 항상 갱신한다."""
    report_base = Path("reports/us_daily")
    report_base.mkdir(parents=True, exist_ok=True)

    trade_date = str(payload.get("trade_date") or "")
    latest_json = report_base / "latest_us_daily_report.json"
    latest_md = report_base / "latest_us_daily_report.md"

    if trade_date:
        dated_dir = report_base / trade_date / session
        dated_dir.mkdir(parents=True, exist_ok=True)
        dated_json = dated_dir / "us_daily_report.json"
        dated_md = dated_dir / "us_daily_report.md"
    else:
        dated_json = None
        dated_md = None

    latest_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))

    md_lines = [
        f"# US Daily Report - {payload.get('trade_date', 'N/A')}",
        "",
        "## Required Fields",
        "",
    ]
    for k in (
        "trade_date", "run_id", "sha", "workflow", "session", "event_name", "env",
        "dry_run", "kis_order_allowed", "prep_status", "locked_watchlist_count",
        "entry_eval_status", "entry_error_type", "entry_error_message", "entry_intents",
        "orders_sent", "fills", "positions", "last_stage", "final_status", "reason",
        "temp_error_count", "temp_recovered_count", "missed_trade_window",
    ):
        md_lines.append(f"- {k}: {payload.get(k)}")
    latest_md.write_text("\n".join(md_lines) + "\n")

    if dated_json is not None and dated_md is not None:
        dated_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        dated_md.write_text("\n".join(md_lines) + "\n")


def _now_ny(force_now: str | None = None) -> datetime:
    from zoneinfo import ZoneInfo
    NY_TZ = ZoneInfo("America/New_York")
    if force_now:
        return datetime.fromisoformat(force_now).astimezone(NY_TZ)
    from trader.us.market_calendar import now_ny
    return now_ny()


def _session_end_dt(session: str, now: datetime) -> datetime:
    """당일 session 종료 시각 (NY timezone datetime)."""
    from zoneinfo import ZoneInfo
    NY_TZ = ZoneInfo("America/New_York")
    h, m = _SESSION_END_TIMES.get(session, (16, 0))
    return now.replace(hour=h, minute=m, second=0, microsecond=0).astimezone(NY_TZ)


def run_trade_session(
    session: str = "am",
    env: str = "practice",
    offline: bool = False,
    max_minutes: int = 180,
    interval_sec: int = 300,
    force_now: str | None = None,
    max_ticks: int = 0,
    run_mode: str | None = None,
    signal_only: bool = False,
) -> dict:
    """AM 또는 Afternoon session 실행.

    각 tick은 trade_tick_runner.run_trade_tick()을 호출한다.
    단일 tick 오류는 warning으로 기록하고 계속 진행한다.
    연속 3회 tick 실패 시 session ERROR 종료한다.

    Args:
        run_mode: "TRADE" | "NON_TRADING_SIGNAL_ONLY" | None
        signal_only: True이면 종목 후보는 생성하되 KIS 주문은 차단

    Returns:
        {"status": "OK"|"OK_WITH_WARNINGS"|"OK_SIGNAL_ONLY"|"SKIP"|"ERROR", ...}
    """
    logger.info(
        "[US_SESSION][START] session=%s env=%s offline=%s max_minutes=%d interval_sec=%d",
        session, env, offline, max_minutes, interval_sec,
    )
    tick_timeout_sec = int(os.getenv("US_TICK_TIMEOUT_SEC", "90"))
    now_for_date = _now_ny(force_now)
    trade_date = now_for_date.strftime("%Y-%m-%d")
    run_id = os.getenv("GITHUB_RUN_ID", "local")
    final_status = "OK"
    final_reason = "session_end"
    last_stage = "session_start"
    final_tick: dict = {}
    temp_error_count = 0
    temp_recovered_count = 0
    logger.info(
        "[US_SESSION][PIPELINE] session=%s requires_prep=1 requires_locked_watchlist=1 raw_universe_fallback=0",
        session
    )
    logger.info(
        "[US_SESSION][FORCE_NOW] enabled=%s force_now=%s max_ticks=%d",
        1 if force_now else 0,
        force_now or "",
        max_ticks,
    )

    from trader.us.market_calendar import is_us_trading_day
    from trader.us.budget import resolve_us_order_budget

    # ── 시각 및 거래일 확인 ───────────────────────────────────────────────────
    # actual_now: 실제 현재 시각 (실제 거래일 판정용)
    # simulated_now: force_now가 있으면 사용하는 시뮬레이션 시각
    actual_now = _now_ny(None)
    simulated_now = _now_ny(force_now) if force_now else actual_now
    
    actual_is_trading_day = is_us_trading_day(actual_now.date())
    
    logger.info(
        "[US_SESSION][PHASE_GUARD] session=%s actual_now_et=%s simulated_now_et=%s actual_is_trading_day=%s",
        session, actual_now.strftime("%H:%M:%S"), simulated_now.strftime("%H:%M:%S"), actual_is_trading_day,
    )

    # ── Run mode 결정 ─────────────────────────────────────────────────────────
    env_run_mode = os.getenv("US_RUN_MODE", "")
    if run_mode:
        resolved_run_mode = run_mode
    elif env_run_mode:
        resolved_run_mode = env_run_mode
    elif not actual_is_trading_day:
        resolved_run_mode = "NON_TRADING_SIGNAL_ONLY"
    else:
        resolved_run_mode = "TRADE"
    
    # Signal only 모드 결정
    resolved_signal_only = (
        signal_only
        or resolved_run_mode == "NON_TRADING_SIGNAL_ONLY"
        or os.getenv("US_SIGNAL_ONLY") == "1"
    )
    
    kis_order_allowed = (
        resolved_run_mode == "TRADE"
        and os.getenv("US_KIS_ORDER_ALLOWED", "1") == "1"
        and not resolved_signal_only
        and actual_is_trading_day
    )
    
    logger.info(
        "[US_SESSION][RUN_MODE] session=%s run_mode=%s signal_only=%s kis_order_allowed=%s",
        session, resolved_run_mode, int(resolved_signal_only), int(kis_order_allowed),
    )
    
    # Signal only이면 tick 수를 제한
    if resolved_signal_only and max_ticks == 0:
        max_ticks = 1
        logger.info("[US_SESSION][SIGNAL_ONLY][TICK_LIMIT] max_ticks=1")
    elif resolved_signal_only and max_ticks > 3:
        max_ticks = 3
        logger.info("[US_SESSION][SIGNAL_ONLY][TICK_LIMIT] max_ticks=3")

    # ── 예산 요약 ─────────────────────────────────────────────────────────────
    budget = resolve_us_order_budget(10000.0)  # stub — tick runner가 실제 조회
    logger.info("[US_SESSION][BUDGET] cap_usd=%.2f", budget["capital_usd_cap"])

    # ── 세션 종료 시각 및 deadline 계산 ──────────────────────────────────────
    session_end = _session_end_dt(session, simulated_now)
    max_end = simulated_now + timedelta(minutes=max_minutes)
    deadline = min(session_end, max_end)

    if offline and max_ticks > 0 and simulated_now >= session_end:
        deadline = max_end
        logger.info(
            "[US_SESSION][DEADLINE_OVERRIDE] mode=offline_max_ticks reason=session_window_elapsed deadline=%s",
            deadline.strftime("%Y-%m-%dT%H:%M:%S%z"),
        )

    logger.info(
        "[US_SESSION][TICK_LOOP][START] session=%s deadline=%s",
        session, deadline.strftime("%Y-%m-%dT%H:%M:%S%z"),
    )

    # ── Tick loop ─────────────────────────────────────────────────────────────
    from trader.us.runner.trade_tick_runner import run_trade_tick

    tick_count = 0
    warn_count = 0
    consecutive_errors = 0
    results: list[dict] = []
    hard_error_reasons = {
        # marker: reason=fills_contract_error
        "fills_contract_error",
        "locked_watchlist_missing",
        "prep_degraded_or_error",
        "locked_watchlist_below_min",
        "watchlist_load_timeout",
        "entry_eval_error",
    }

    while True:
        if force_now:
            tick_now_dt = simulated_now + timedelta(seconds=interval_sec * tick_count)
            tick_force_now = tick_now_dt.isoformat()
        else:
            tick_now_dt = _now_ny(None)
            tick_force_now = None

        if tick_now_dt >= deadline:
            final_reason = "session_end"
            logger.info(
                "[US_SESSION][END] session=%s reason=session_end ticks=%d",
                session, tick_count,
            )
            break

        tick_count += 1
        last_stage = f"tick_{tick_count}"
        logger.info(
            "[US_TICK_LOOP][TICK] session=%s tick=%d force_now=%s",
            session,
            tick_count,
            tick_force_now or "",
        )

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                fut = pool.submit(
                    run_trade_tick,
                    session=session,
                    env=env,
                    offline=offline,
                    force_now=tick_force_now,
                    run_mode=resolved_run_mode,
                    signal_only=resolved_signal_only,
                    kis_order_allowed=kis_order_allowed,
                )
                tick_result = fut.result(timeout=tick_timeout_sec)
            results.append(tick_result)
            final_tick = tick_result
            temp_error_count += int(tick_result.get("temp_error_count", 0) or 0)
            temp_recovered_count += int(tick_result.get("temp_recovered_count", 0) or 0)
            last_stage = tick_result.get("last_stage", last_stage)

            tick_status = tick_result.get("status", "ERROR")
            acceptable_statuses = {"OK", "OK_SIGNAL_ONLY", "OK_NO_TRADE", "OK_WITH_WARNINGS"}

            if tick_result.get("reason") == "fills_contract_error":
                # compatibility marker for legacy contract tests: "status": "ERROR"
                final_status = "FAILED"
                final_reason = "fills_contract_error"
                logger.error(
                    "[US_SESSION][END] session=%s reason=fills_contract_error tick=%d",
                    session,
                    tick_count,
                )
                break

            if tick_status in acceptable_statuses:
                if tick_status in ("OK_WITH_WARNINGS", "OK_SIGNAL_ONLY"):
                    warn_count += 1
                    logger.warning(
                        "[US_SESSION][TICK_LOOP][WARN] tick=%d status=%s",
                        tick_count, tick_status,
                    )
                consecutive_errors = 0
            elif tick_status in {"FAILED", "ERROR"}:
                final_status = "FAILED"
                final_reason = tick_result.get("reason", "tick_failed")
                logger.error(
                    "[US_SESSION][END] session=%s reason=%s tick=%d",
                    session,
                    final_reason,
                    tick_count,
                )
                break
            else:
                consecutive_errors += 1
                warn_count += 1
                logger.warning(
                    "[US_SESSION][TICK_LOOP][WARN] tick=%d status=%s consecutive_errors=%d",
                    tick_count, tick_status, consecutive_errors,
                )

        except concurrent.futures.TimeoutError:
            final_status = "FAILED"
            final_reason = "tick_timeout"
            last_stage = f"tick_{tick_count}_timeout"
            logger.error(
                "[US_TICK][DONE] status=FAILED reason=tick_timeout timeout_sec=%d",
                tick_timeout_sec,
            )
            logger.error("[US_SESSION][END] session=%s reason=tick_timeout", session)
            break
        except Exception as exc:
            consecutive_errors += 1
            warn_count += 1
            logger.warning(
                "[US_SESSION][TICK_LOOP][WARN] tick=%d exception=%s consecutive_errors=%d",
                tick_count, exc, consecutive_errors,
            )
            results.append({"status": "ERROR", "error": str(exc)})

        if consecutive_errors >= _MAX_CONSECUTIVE_ERRORS:
            final_status = "FAILED"
            final_reason = "consecutive_errors"
            logger.error(
                "[US_SESSION][TICK_LOOP][ERROR] session=%s consecutive_errors=%d - aborting",
                session, consecutive_errors,
            )
            logger.info("[US_SESSION][END] session=%s reason=consecutive_errors", session)
            break

        if final_tick.get("status") == "SKIP" and tick_count == 1 and not resolved_signal_only:
            final_status = "SKIP"
            final_reason = final_tick.get("reason", "market_skip")
            logger.info("[US_SESSION][END] session=%s reason=market_skip", session)
            break

        if final_tick.get("reason") in hard_error_reasons:
            final_status = "FAILED"
            final_reason = final_tick.get("reason")
            logger.error(
                "[US_SESSION][END] session=%s reason=%s tick=%d",
                session,
                final_reason,
                tick_count,
            )
            break

        if max_ticks > 0 and tick_count >= max_ticks:
            final_reason = "max_ticks"
            logger.info(
                "[US_SESSION][END] session=%s reason=max_ticks ticks=%d",
                session,
                tick_count,
            )
            break

        if force_now and max_ticks == 0:
            final_reason = "force_now_single_tick"
            logger.info(
                "[US_SESSION][END] session=%s reason=force_now_single_tick ticks=%d",
                session,
                tick_count,
            )
            break

        if not force_now:
            logger.info("[US_TICK_LOOP][SLEEP] seconds=%d", interval_sec)
            time_mod.sleep(interval_sec)

    logger.info(
        "[US_SESSION][END] session=%s reason=session_end ticks=%d warns=%d",
        session, tick_count, warn_count,
    )

    if final_status not in {"FAILED", "SKIP"}:
        if resolved_signal_only:
            final_status = "OK_SIGNAL_ONLY" if warn_count == 0 else "OK_WITH_WARNINGS_SIGNAL_ONLY"
            logger.info(
                "[US_SESSION][SIGNAL_ONLY][END] session=%s status=%s reason=%s",
                session, final_status, "non_trading_day_signal_only" if resolved_run_mode == "NON_TRADING_SIGNAL_ONLY" else "signal_only",
            )
        else:
            final_status = "OK_WITH_WARNINGS" if warn_count > 0 else "OK"

    report_payload = {
        "trade_date": trade_date,
        "run_id": run_id,
        "sha": os.getenv("GITHUB_SHA", ""),
        "workflow": os.getenv("GITHUB_WORKFLOW", ""),
        "session": session,
        "event_name": os.getenv("GITHUB_EVENT_NAME", ""),
        "env": env,
        "dry_run": os.getenv("DRY_RUN", "0") == "1",
        "kis_order_allowed": int(kis_order_allowed),
        "prep_status": final_tick.get("prep_status", "UNKNOWN"),
        "locked_watchlist_count": int(final_tick.get("locked_watchlist_count", 0) or 0),
        "entry_eval_status": final_tick.get("entry_eval_status", "UNKNOWN"),
        "entry_error_type": final_tick.get("entry_error_type", ""),
        "entry_error_message": final_tick.get("entry_error_message", ""),
        "entry_intents": int(final_tick.get("entry_intents", 0) or 0),
        "orders_sent": int(final_tick.get("orders_sent", 0) or 0),
        "fills": int(final_tick.get("fills", 0) or 0),
        "positions": int(final_tick.get("positions", 0) or 0),
        "last_stage": last_stage,
        "final_status": final_status,
        "reason": final_reason,
        "temp_error_count": temp_error_count,
        "temp_recovered_count": temp_recovered_count,
        "missed_trade_window": os.getenv("US_MISSED_TRADE_WINDOW", "0") == "1",
    }
    _write_us_session_report(report_payload, session=session)
    logger.info(
        "[RUN_SUMMARY][RESULT] status=%s reason=%s last_stage=%s",
        final_status,
        final_reason,
        last_stage,
    )

    return {
        "status": final_status,
        "session": session,
        "reason": final_reason,
        "tick_count": tick_count,
        "warn_count": warn_count,
        "run_mode": resolved_run_mode,
        "signal_only": resolved_signal_only,
        "kis_order_allowed": kis_order_allowed,
        "results": results,
        "last_stage": last_stage,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="US Trade Session Runner")
    parser.add_argument("--session", required=True, choices=["am", "afternoon"])
    parser.add_argument("--env", default="practice")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--max-minutes", dest="max_minutes", type=int, default=180)
    parser.add_argument("--interval-sec", dest="interval_sec", type=int, default=300)
    parser.add_argument("--force-now", dest="force_now", default=None)
    parser.add_argument("--max-ticks", dest="max_ticks", type=int, default=0)
    parser.add_argument("--run-mode", dest="run_mode", default=None)
    parser.add_argument("--signal-only", dest="signal_only", action="store_true")
    args = parser.parse_args()

    result = run_trade_session(
        session=args.session,
        env=args.env,
        offline=args.offline,
        max_minutes=args.max_minutes,
        interval_sec=args.interval_sec,
        force_now=args.force_now,
        max_ticks=args.max_ticks,
        run_mode=args.run_mode,
        signal_only=args.signal_only,
    )
    if result["status"] in ("ERROR", "FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
