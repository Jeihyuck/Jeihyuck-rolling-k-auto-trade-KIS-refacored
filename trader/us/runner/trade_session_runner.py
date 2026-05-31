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


def _write_us_schedule_health(payload: dict, session: str) -> None:
    """reports/us_schedule_health/{trade_date}.json 에 세션 결과를 기록한다.

    파일이 이미 있으면 해당 session 키만 덮어쓴다.
    이 파일은 schedule_health 모니터링/watchdog의 입력으로 사용된다.
    """
    import json
    from datetime import datetime as _dt
    from pathlib import Path

    trade_date = str(payload.get("trade_date") or "")
    if not trade_date:
        return

    health_dir = Path("reports/us_schedule_health")
    health_dir.mkdir(parents=True, exist_ok=True)
    health_file = health_dir / f"{trade_date}.json"

    try:
        existing: dict = {}
        if health_file.exists():
            try:
                existing = json.loads(health_file.read_text(encoding="utf-8"))
            except Exception:
                existing = {}

        now_utc = _dt.utcnow().isoformat() + "Z"
        session_entry = {
            "session": session,
            "trade_date": trade_date,
            "final_status": payload.get("final_status", "UNKNOWN"),
            "reason": payload.get("reason", ""),
            "tick_count": int(payload.get("tick_count", 0) or 0),
            "fills_count": int(payload.get("fills_count", 0) or 0),
            "unique_fills_count": int(payload.get("unique_fills_count", 0) or 0),
            "orders_ack": int(payload.get("orders_ack", 0) or 0),
            "orders_rejected": int(payload.get("orders_rejected", 0) or 0),
            "run_id": payload.get("run_id", ""),
            "workflow": payload.get("workflow", ""),
            "wall_elapsed_sec": float(payload.get("wall_elapsed_sec", 0) or 0),
            "missed_trade_window": bool(payload.get("missed_trade_window", False)),
            "recorded_at_utc": now_utc,
        }

        existing.setdefault("trade_date", trade_date)
        existing.setdefault("sessions", {})
        existing["sessions"][session] = session_entry
        existing["updated_at_utc"] = now_utc

        health_file.write_text(
            json.dumps(existing, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        logger.info(
            "[US_SCHEDULE_HEALTH][SAVED] file=%s session=%s status=%s",
            health_file,
            session,
            session_entry["final_status"],
        )
    except Exception as exc:
        logger.warning("[US_SCHEDULE_HEALTH][WARN] write failed: %s", exc)


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
        "buy_decisions", "sell_decisions",
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

    # ── 파일 기반 session guard 체크 ─────────────────────────────────────────
    from trader.us.utils.session_guard import (
        check_us_session_file_guard,
        now_et_iso,
        write_us_session_done_file,
    )
    session_started_at_et = now_et_iso()

    _file_guard = check_us_session_file_guard(trade_date, session)
    if _file_guard["already_ran"]:
        guard_payload = _file_guard["payload"]
        # P7: stale guard 리포트 — 실제 시작 시각과 예상 시각 차이 계산
        _schedule_expected_et = _file_guard.get("schedule_expected_et", "")
        _actual_start_et = session_started_at_et
        _delay_seconds: int = 0
        try:
            if _schedule_expected_et:
                from datetime import datetime
                _exp = datetime.fromisoformat(_schedule_expected_et)
                _act = datetime.fromisoformat(_actual_start_et)
                _delay_seconds = max(0, int((_act - _exp).total_seconds()))
        except Exception:
            pass
        logger.info(
            "[US_SESSION][FILE_GUARD_SKIP] session=%s trade_date=%s prior_status=%s prior_ticks=%s"
            " schedule_expected_et=%s actual_start_et=%s delay_seconds=%d",
            session,
            trade_date,
            guard_payload.get("status"),
            guard_payload.get("ticks"),
            _schedule_expected_et,
            _actual_start_et,
            _delay_seconds,
        )
        return {
            "status": "SKIP",
            "skip_reason": "file_guard_already_ran",
            "session": session,
            "reason": "file_guard_already_ran",
            "guard_status": _file_guard["guard_status"],
            "prior_run": guard_payload,
            "tick_count": 0,
            # P7 stale guard 리포트 필드
            "schedule_expected_et": _schedule_expected_et,
            "actual_start_et": _actual_start_et,
            "delay_seconds": _delay_seconds,
            "missed_trade_window": _delay_seconds > 3600,
        }

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
    actual_now = _now_ny(None)
    simulated_now = _now_ny(force_now) if force_now else actual_now
    
    actual_is_trading_day = is_us_trading_day(actual_now.date())
    
    logger.info(
        "[US_SESSION][PHASE_GUARD] session=%s actual_now_et=%s simulated_now_et=%s actual_is_trading_day=%s",
        session, actual_now.strftime("%H:%M:%S"), simulated_now.strftime("%H:%M:%S"), actual_is_trading_day,
    )

    # ── Prep Guard (am / afternoon session) ──────────────────────────────
    if session in ("am", "afternoon") and not offline:
        try:
            from trader.us.prep_contract import check_us_prep_guard
            guard = check_us_prep_guard(trade_date)
            if guard["ok"]:
                logger.info(
                    "[US_PREP_GUARD][OK] workflow=us-trade-%s session=%s trade_date=%s"
                    " final30=%s score_nonzero=%s source=%s",
                    session, session, trade_date,
                    guard.get("final30_scored_count", "?"),
                    guard.get("score_nonzero_count", "?"),
                    guard.get("source", "runtime"),
                )
            else:
                logger.error(
                    "[US_PREP_GUARD][BLOCK] workflow=us-trade-%s session=%s"
                    " reason=final30_missing_or_contract_fail trade_date=%s detail=%s",
                    session, session, trade_date, guard.get("reason"),
                )
                _write_us_schedule_health(
                    {
                        "trade_date": trade_date,
                        "final_status": "FAILED_PREP_GUARD",
                        "reason": f"prep_guard_block:{guard.get('reason')}",
                        "tick_count": 0,
                        "workflow": f"us-trade-{session}",
                    },
                    session,
                )
                return {
                    "status": "FAILED_PREP_GUARD",
                    "reason": f"prep_guard_block:{guard.get('reason')}",
                    "session": session,
                    "trade_date": trade_date,
                }
        except Exception as _guard_exc:
            logger.warning(
                "[US_PREP_GUARD][WARN] session=%s guard check failed: %s — proceeding with caution",
                session, _guard_exc,
            )
    elif session in ("am", "afternoon") and offline:
        logger.info("[US_PREP_GUARD][BYPASS] session=%s offline=True — skipping prep guard", session)

    # ── Close session: prep contract를 읽어 report에 포함 ─────────────────
    if session == "close":
        try:
            from trader.us.prep_contract import check_us_prep_guard
            guard = check_us_prep_guard(trade_date)
            logger.info(
                "[US_PREP_GUARD][READ] workflow=us-trade-close session=close"
                " trade_date=%s status=%s",
                trade_date,
                guard.get("contract", {}).get("status", "MISSING") if guard.get("contract") else "MISSING",
            )
        except Exception:
            pass
        logger.info("[US_TRADE_CLOSE][ENTRY_DISABLED] reason=close_session")
        logger.info("[US_TRADE_CLOSE][PNL_CONTINUE] reason=close_report_required")

    # ── Afternoon session: exit 우선 + 신규 매수 차단 체크 ────────────────
    if session == "afternoon":
        try:
            from trader.us.db.repos import get_today_buy_orders_count
            buy_count = get_today_buy_orders_count(trade_date=trade_date, env=env)
        except Exception:
            buy_count = 0
        if buy_count > 0:
            logger.info(
                "[US_AFTERNOON][MODE] mode=exit_first entry_allowed=false"
                " reason=already_bought_today buy_orders_count=%d",
                buy_count,
            )
        else:
            logger.info(
                "[US_AFTERNOON][MODE] mode=exit_first entry_allowed=true"
                " reason=no_buy_order_today",
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

    # Graceful shutdown buffer: GitHub Actions hard kill 전에 Python이 먼저 종료되도록
    shutdown_buffer_sec = int(os.getenv("US_SESSION_SHUTDOWN_BUFFER_SEC", "600"))  # 10분 기본값
    
    # force_now + max_ticks > 0 + offline=True인 smoke run에서는 shutdown_buffer를 낮춰서
    # 최소 1 tick은 실행될 수 있도록 보장
    if force_now and max_ticks > 0 and offline:
        shutdown_buffer_sec = 60
        logger.info(
            "[US_SESSION][SHUTDOWN_BUFFER_OVERRIDE] reason=force_now_max_ticks_offline buffer_sec=%d",
            shutdown_buffer_sec,
        )
    
    graceful_deadline = deadline - timedelta(seconds=shutdown_buffer_sec)
    
    logger.info(
        "[US_SESSION][SHUTDOWN_BUFFER] buffer_sec=%d graceful_deadline=%s hard_deadline=%s",
        shutdown_buffer_sec,
        graceful_deadline.strftime("%Y-%m-%dT%H:%M:%S%z"),
        deadline.strftime("%Y-%m-%dT%H:%M:%S%z"),
    )

    if offline and max_ticks > 0 and simulated_now >= session_end:
        deadline = max_end
        graceful_deadline = deadline - timedelta(seconds=shutdown_buffer_sec)
        logger.info(
            "[US_SESSION][DEADLINE_OVERRIDE] mode=offline_max_ticks reason=session_window_elapsed "
            "graceful_deadline=%s hard_deadline=%s",
            graceful_deadline.strftime("%Y-%m-%dT%H:%M:%S%z"),
            deadline.strftime("%Y-%m-%dT%H:%M:%S%z"),
        )

    logger.info(
        "[US_SESSION][TICK_LOOP][START] session=%s graceful_deadline=%s",
        session, graceful_deadline.strftime("%Y-%m-%dT%H:%M:%S%z"),
    )

    # ── Tick loop (try/finally로 감싸서 report를 항상 작성) ───────────────────
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

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Try/Finally 구조: 예외/timeout이 발생해도 최종 report를 항상 작성한다
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    session_start_time = time_mod.time()
    try:
        while True:
            if force_now:
                tick_now_dt = simulated_now + timedelta(seconds=interval_sec * tick_count)
                tick_force_now = tick_now_dt.isoformat()
            else:
                tick_now_dt = _now_ny(None)
                tick_force_now = None

            # Graceful deadline 검사: GitHub hard kill 전에 Python이 먼저 종료
            # 단, max_ticks > 0일 때는 최소 1 tick은 실행되도록 보장
            if tick_now_dt >= graceful_deadline:
                if max_ticks > 0 and tick_count == 0:
                    logger.info(
                        "[US_SESSION][GRACEFUL_DEADLINE] allow_first_tick=1 reason=max_ticks_guarantee tick_count=%d",
                        tick_count,
                    )
                else:
                    final_reason = "graceful_shutdown"
                    logger.info(
                        "[US_SESSION][END] session=%s reason=graceful_shutdown ticks=%d",
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
                acceptable_statuses = {
                    "OK",
                    "OK_SIGNAL_ONLY",
                    "OK_NO_TRADE",
                    "OK_WITH_WARNINGS",
                    "OK_ORDERS_SENT",
                    "NO_ENTRY_INTENTS",
                    "NO_ORDERS_RISK_BLOCKED",
                    "PARTIAL_ORDERS_BLOCKED",
                }

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

    except KeyboardInterrupt:
        final_status = "FAILED"
        final_reason = "keyboard_interrupt"
        logger.error("[US_SESSION][INTERRUPT] session=%s reason=keyboard_interrupt", session)
    except SystemExit:
        final_status = "FAILED"
        final_reason = "system_exit"
        logger.error("[US_SESSION][EXIT] session=%s reason=system_exit", session)
    except Exception as loop_exc:
        final_status = "FAILED"
        final_reason = f"loop_exception: {loop_exc}"
        logger.error(
            "[US_SESSION][EXCEPTION] session=%s exception=%s",
            session, loop_exc, exc_info=True
        )
    finally:
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # Finally block: 어떤 경우든 항상 report를 작성한다
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        logger.info(
            "[US_SESSION][FINALLY] session=%s ticks=%d warns=%d status=%s reason=%s",
            session, tick_count, warn_count, final_status, final_reason,
        )

    # ── Tick loop 종료 후 최종 처리 ────────────────────────────────────────────
    session_wall_elapsed_sec = time_mod.time() - session_start_time
    
    logger.info(
        "[US_SESSION][END] session=%s reason=%s ticks=%d warns=%d wall_elapsed_sec=%.2f",
        session, final_reason, tick_count, warn_count, session_wall_elapsed_sec,
    )
    
    # tick=0 OK 금지: tick이 한 번도 실행되지 않으면 반드시 경고 또는 실패
    if tick_count == 0 and final_status not in {"FAILED", "SKIP"}:
        if max_ticks > 0:
            # 명시적 tick 수 요청인데 0번 실행 → 실패
            final_status = "FAILED"
            final_reason = "no_tick_executed"
            logger.error(
                "[US_SESSION][ERROR] max_ticks=%d but tick_count=0 - setting final_status=FAILED",
                max_ticks,
            )
        else:
            # 무제한 모드인데도 tick=0이면 경고 (graceful_shutdown 등)
            warn_count += 1
            logger.warning(
                "[US_SESSION][WARN] tick_count=0 reason=%s session ended without executing any tick"
                " - status will be OK_WITH_WARNINGS",
                final_reason,
            )
    
    # KIS TEMP_ERROR recovery warning
    if temp_recovered_count > 0:
        warn_count += 1
        logger.warning(
            "[US_SESSION][WARN] KIS TEMP_ERROR recovered temp_error_count=%d temp_recovered_count=%d",
            temp_error_count,
            temp_recovered_count,
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

    # ─────────────────────────────────────────────────────────────────────────
    # Aggregate explanation statistics from tick results
    # ─────────────────────────────────────────────────────────────────────────
    total_buy_decisions = 0
    total_sell_decisions = 0
    total_orders_sent = 0
    total_orders_ack = 0
    total_orders_rejected = 0
    total_orders_error = 0
    total_fills = 0
    total_pending_orders = 0
    total_sold_today = 0
    total_open_positions = 0
    all_sold_today_symbols: list[str] = []
    all_pending_order_symbols: list[str] = []
    all_open_position_symbols: list[str] = []
    # entry skip summary 집계 (P3: afternoon)
    entry_skip_total = 0
    entry_skip_has_kis_position = 0
    entry_skip_pending_order = 0
    entry_skip_sold_today = 0
    entry_skip_scored = 0
    entry_skip_by_symbol: list[dict] = []

    for tick_result in results:
        # buy_decisions = entry_intents count (BUY decision)
        entry_intents = int(tick_result.get("entry_intents", 0) or 0)
        total_buy_decisions += entry_intents

        # sell_decisions = exit_intents count (SELL decision)
        exit_intents = int(tick_result.get("exit_intents", 0) or 0)
        total_sell_decisions += exit_intents

        # 주문 상태 집계
        total_orders_sent += int(tick_result.get("orders_sent", 0) or 0)
        total_orders_ack += int(tick_result.get("orders_ack", 0) or 0)
        total_orders_rejected += int(tick_result.get("orders_rejected", 0) or 0)
        total_orders_error += int(tick_result.get("orders_error", 0) or 0)
        # fills: KIS가 매 tick마다 당일 누적 체결 snapshot을 반환하므로
        # 단순 합산이 아닌 max로 중복 집계를 방지한다 (unique_fills_count와 동일).
        tick_fills = int(tick_result.get("fills_count", tick_result.get("fills", 0)) or 0)
        total_fills = max(total_fills, tick_fills)
        total_pending_orders = int(tick_result.get("pending_order_count", 0) or 0)
        total_sold_today = int(tick_result.get("sold_today_count", 0) or 0)
        total_open_positions = int(tick_result.get("open_position_count", tick_result.get("positions", 0)) or 0)

        # 심볼 배열 (마지막 tick 기준 덮어쓰기)
        if tick_result.get("sold_today_symbols"):
            all_sold_today_symbols = list(tick_result["sold_today_symbols"])
        if tick_result.get("pending_order_symbols"):
            all_pending_order_symbols = list(tick_result["pending_order_symbols"])
        if tick_result.get("open_position_symbols"):
            all_open_position_symbols = list(tick_result["open_position_symbols"])

        # entry skip summary 집계
        skip_summary = tick_result.get("entry_skip_summary", {})
        entry_skip_total = int(skip_summary.get("total", entry_skip_total) or entry_skip_total)
        entry_skip_scored = int(skip_summary.get("scored", entry_skip_scored) or entry_skip_scored)
        entry_skip_has_kis_position += int(skip_summary.get("has_kis_position", 0) or 0)
        entry_skip_pending_order += int(skip_summary.get("pending_order", 0) or 0)
        entry_skip_sold_today += int(skip_summary.get("sold_today", 0) or 0)
        by_sym = tick_result.get("entry_skip_by_symbol", [])
        if by_sym:
            entry_skip_by_symbol = list(by_sym)

    # KIS temp_errors_by_api 세션 집계
    kis_temp_errors_by_api: dict[str, dict] = {}
    for tick_result in results:
        for api_name, api_stats in (tick_result.get("kis_temp_errors_by_api") or {}).items():
            e = kis_temp_errors_by_api.setdefault(
                api_name, {"temp_error": 0, "recovered": 0, "unrecovered": 0}
            )
            e["temp_error"] += int(api_stats.get("temp_error", 0) or 0)
            e["recovered"] += int(api_stats.get("recovered", 0) or 0)
            e["unrecovered"] += int(api_stats.get("unrecovered", 0) or 0)

    # pending = ack - fills (fallback 계산)
    if total_orders_ack > 0 and total_pending_orders == 0:
        total_pending_orders = max(0, total_orders_ack - total_fills)

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
        # ── tick별 최종값 vs session 누적 total 분리 ─────────────────────────
        # last_tick: 마지막 tick의 값 (신호/포지션 상태 파악용)
        "entry_intents_last_tick": int(final_tick.get("entry_intents", 0) or 0),
        "exit_intents_last_tick": int(final_tick.get("exit_intents", 0) or 0),
        # total: 세션 전체 누적 합산 (주문 건수 집계용)
        "entry_intents_total": total_buy_decisions,
        "exit_intents_total": total_sell_decisions,
        # 하위 호환: entry_intents는 total 값으로 유지
        "entry_intents": total_buy_decisions,
        "orders_sent": total_orders_sent if total_orders_sent else int(final_tick.get("orders_sent", 0) or 0),
        "orders_sent_total": total_orders_sent if total_orders_sent else int(final_tick.get("orders_sent", 0) or 0),
        "orders_ack": total_orders_ack,
        "orders_ack_total": total_orders_ack,
        "orders_rejected": total_orders_rejected,
        "orders_reject_total": total_orders_rejected,
        "orders_error": total_orders_error,
        "orders_error_total": total_orders_error,
        "orders_blocked": int(final_tick.get("orders_blocked", 0) or 0),
        "block_reasons": final_tick.get("block_reasons", {}),
        "fills_count": total_fills,
        "fills": total_fills,
        "unique_fills_count": total_fills,
        "pending_order_count": total_pending_orders,
        "sold_today_count": total_sold_today,
        "open_position_count": total_open_positions,
        "sold_today_symbols": all_sold_today_symbols,
        "pending_order_symbols": all_pending_order_symbols,
        "open_position_symbols": all_open_position_symbols,
        "positions": int(final_tick.get("positions", 0) or 0),
        "last_stage": last_stage,
        "final_status": final_status,
        "reason": final_reason,
        "temp_error_count": temp_error_count,
        "temp_recovered_count": temp_recovered_count,
        "missed_trade_window": os.getenv("US_MISSED_TRADE_WINDOW", "0") == "1",
        "kis_temp_errors": {
            "total": sum(v.get("temp_error", 0) for v in kis_temp_errors_by_api.values()),
            "recovered": sum(v.get("recovered", 0) for v in kis_temp_errors_by_api.values()),
            "unrecovered": sum(v.get("unrecovered", 0) for v in kis_temp_errors_by_api.values()),
            "by_api": kis_temp_errors_by_api,
        },
        # 추가 필드
        "ticks_total": tick_count,
        "tick_count": tick_count,
        "ticks": tick_count,
        "max_ticks": max_ticks,
        "force_now": force_now or "",
        "offline": offline,
        "wall_elapsed_sec": round(session_wall_elapsed_sec, 2),
        # Explanation statistics (total 기준)
        "buy_decisions": total_buy_decisions,
        "sell_decisions": total_sell_decisions,
        # P3: entry skip summary (afternoon에서 신규 매수 0건 이유 분해)
        "entry_skip_summary": {
            "total": entry_skip_total,
            "scored": entry_skip_scored,
            "has_kis_position": entry_skip_has_kis_position,
            "pending_order": entry_skip_pending_order,
            "sold_today": entry_skip_sold_today,
        },
        "entry_skip_by_symbol": entry_skip_by_symbol,
    }
    _write_us_session_report(report_payload, session=session)
    _write_us_schedule_health(report_payload, session=session)

    logger.info(
        "[US_DAILY][AGGREGATE] ticks_total=%d entry_intents_total=%d exit_intents_total=%d"
        " orders_sent_total=%d orders_ack_total=%d orders_reject_total=%d buy_orders_count=%d sell_orders_count=%d",
        tick_count,
        total_buy_decisions,
        total_sell_decisions,
        report_payload["orders_sent_total"],
        total_orders_ack,
        total_orders_rejected,
        total_orders_ack,   # buy_orders_count (ACK 기준)
        sum(1 for r in results if r.get("exit_intents", 0) > 0),
    )

    # ── 파일 기반 done 마커 기록 (성공/경고 종료 시만) ────────────────────────
    if final_status not in ("FAILED", "SKIP"):
        write_us_session_done_file(
            trade_date=trade_date,
            session=session,
            run_id=run_id,
            started_at_et=session_started_at_et,
            finished_at_et=now_et_iso(),
            status=final_status,
            ticks=tick_count,
        )

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
    from trader.us.utils.logging_utils import setup_us_logging
    setup_us_logging()
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
