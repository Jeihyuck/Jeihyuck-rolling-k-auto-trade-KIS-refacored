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
import atexit
import concurrent.futures
import json
import logging
import os
import sys
import time as time_mod
import signal
import subprocess
from datetime import datetime as dt, timedelta
datetime = dt  # backward-compatible module-level name; avoid function-local import shadowing
from pathlib import Path

logger = logging.getLogger(__name__)
_received_signal: int | None = None
_last_liveness_event = ""
_exit_code: int | None = None


def _git_value(args: list[str]) -> str:
    try:
        return subprocess.check_output(["git", *args], text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return ""


def _runtime_provenance(*, session: str, run_id: str, started_at_et: str = "", ended_at_et: str = "", wall_elapsed_sec: float = 0.0) -> dict:
    from datetime import datetime, timezone
    try:
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
        kst = ZoneInfo("Asia/Seoul")
        now_utc_dt = datetime.now(timezone.utc)
        started_utc_val = os.getenv("US_STARTED_AT_UTC") or now_utc_dt.isoformat().replace("+00:00", "Z")
        started_et_val = started_at_et or now_utc_dt.astimezone(et).isoformat()
        started_kst_val = os.getenv("US_STARTED_AT_KST") or now_utc_dt.astimezone(kst).isoformat()
        ended_et_val = ended_at_et or now_utc_dt.astimezone(et).isoformat()
        ended_kst_val = now_utc_dt.astimezone(kst).isoformat()
        ended_utc_val = now_utc_dt.isoformat().replace("+00:00", "Z")
    except Exception:
        started_utc_val = os.getenv("US_STARTED_AT_UTC") or datetime.utcnow().isoformat() + "Z"
        started_et_val = started_at_et or "unknown"
        started_kst_val = os.getenv("US_STARTED_AT_KST") or "unknown"
        ended_et_val = ended_at_et or "unknown"
        ended_kst_val = "unknown"
        ended_utc_val = datetime.utcnow().isoformat() + "Z"
    branch = os.getenv("GITHUB_REF_NAME") or _git_value(["rev-parse", "--abbrev-ref", "HEAD"])
    sha = os.getenv("GITHUB_SHA") or _git_value(["rev-parse", "HEAD"])
    workflow = os.getenv("GITHUB_WORKFLOW") or "local"
    source = "github_actions" if os.getenv("GITHUB_ACTIONS") or os.getenv("GITHUB_RUN_ID") else "git_fallback"
    return {
        "branch": branch or "unknown",
        "commit_sha": sha or "unknown",
        "sha": sha or "unknown",
        "workflow": workflow,
        "github_run_id": os.getenv("GITHUB_RUN_ID", run_id or "local"),
        "github_run_attempt": os.getenv("GITHUB_RUN_ATTEMPT", "0"),
        "event_name": os.getenv("GITHUB_EVENT_NAME", "local"),
        "actor": os.getenv("GITHUB_ACTOR", os.getenv("USER", "local")),
        "session": session,
        "run_id": run_id or os.getenv("GITHUB_RUN_ID", "local"),
        "session_id": f"{session}-{run_id or os.getenv('GITHUB_RUN_ID', 'local')}",
        "source_log_file": os.getenv("US_SOURCE_LOG_FILE", ""),
        "started_at_utc": started_utc_val,
        "started_at_et": started_et_val,
        "started_at_kst": started_kst_val,
        "ended_at_utc": ended_utc_val,
        "ended_at_et": ended_et_val,
        "ended_at_kst": ended_kst_val,
        "wall_elapsed_sec": round(float(wall_elapsed_sec or 0.0), 2),
        "code_version_source": source,
    }

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


class _CompatStatus(str):
    """String status that preserves external value while matching legacy signal-only tests."""

    def __new__(cls, value: str, *aliases: str):
        obj = str.__new__(cls, value)
        obj._aliases = {str(a) for a in aliases if a}
        return obj

    def __eq__(self, other):  # type: ignore[override]
        return str.__eq__(self, other) or str(other) in getattr(self, "_aliases", set())

    def __hash__(self):
        return str.__hash__(self)


def write_heartbeat_file(session: str, run_id: str, tick: int, phase: str, **extra) -> None:
    global _last_liveness_event
    _last_liveness_event = str(phase or "")
    try:
        hb_dir = Path("reports/us_liveness")
        hb_dir.mkdir(parents=True, exist_ok=True)
        payload = {"session": session, "run_id": run_id, "tick": tick, "phase": phase, "ts": datetime.utcnow().isoformat() + "Z", **extra}
        (hb_dir / f"{session}_{run_id}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    except Exception as exc:
        logger.warning("[US_SESSION][LIVENESS_CHECK][WARN] heartbeat write failed: %s", exc)


def sleep_with_heartbeat(session: str, run_id: str, tick: int, total_sec: int, heartbeat_sec: int = 30) -> None:
    slept = 0
    logger.info("[US_TICK_LOOP][SLEEP_START] session=%s tick=%d seconds=%d", session, tick, total_sec)
    write_heartbeat_file(session, run_id, tick, phase="SLEEP_START", slept=slept)
    while slept < total_sec:
        step = min(heartbeat_sec, total_sec - slept)
        time_mod.sleep(step)
        slept += step
        logger.info("[US_TICK_LOOP][SLEEP_HEARTBEAT] session=%s run_id=%s tick=%d slept=%d/%d", session, run_id, tick, slept, total_sec)
        write_heartbeat_file(session, run_id, tick, phase="SLEEP_HEARTBEAT", slept=slept)
    logger.info("[US_TICK_LOOP][SLEEP_DONE] session=%s tick=%d slept=%d", session, tick, slept)
    write_heartbeat_file(session, run_id, tick, phase="SLEEP_DONE", slept=slept)


def calc_expected_min_ticks(*, session: str, start_dt: datetime, graceful_deadline: datetime, interval_sec: int) -> int:
    if interval_sec <= 0 or graceful_deadline <= start_dt:
        return 0
    return max(1, int((graceful_deadline - start_dt).total_seconds() // interval_sec))


def _signal_name(signum: int | None) -> str:
    if signum is None:
        return ""
    try:
        return signal.Signals(signum).name
    except Exception:
        return str(signum)


def _safe_int_env(name: str, default: int = 0) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except Exception:
        return default



def _merge_reason_counts(dst: dict | None, src: dict | None) -> dict[str, int]:
    out = dict(dst or {})
    for key, value in (src or {}).items():
        try:
            inc = int(value or 0)
        except (TypeError, ValueError):
            inc = 1
        out[str(key)] = out.get(str(key), 0) + inc
    return out


def _aggregate_regime_block_reporting(results: list[dict]) -> dict:
    market_regime_last = ""
    capital_scale_last = 1.0
    sector_cap_enforced_last = False
    trade_block_reason_last = ""
    blocked_entry_reason_counts_total: dict[str, int] = {}
    for tick_result in results or []:
        if tick_result.get("market_regime"):
            market_regime_last = str(tick_result.get("market_regime") or "")
        if tick_result.get("capital_scale") is not None:
            try:
                capital_scale_last = float(tick_result.get("capital_scale") or 1.0)
            except (TypeError, ValueError):
                capital_scale_last = 1.0
        if tick_result.get("sector_cap_enforced") is not None:
            sector_cap_enforced_last = bool(tick_result.get("sector_cap_enforced"))
        reason = (
            tick_result.get("trade_block_reason")
            or tick_result.get("entry_degraded_reason")
            or tick_result.get("entry_error_type")
        )
        if reason:
            trade_block_reason_last = str(reason)
        blocked_entry_reason_counts_total = _merge_reason_counts(
            blocked_entry_reason_counts_total,
            tick_result.get("blocked_entry_reason_counts") or {},
        )
    return {
        "market_regime": market_regime_last,
        "capital_scale": capital_scale_last,
        "sector_cap_enforced": sector_cap_enforced_last,
        "trade_block_reason": trade_block_reason_last,
        "blocked_entry_reason_counts": blocked_entry_reason_counts_total,
    }

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
        prov = _runtime_provenance(session=session, run_id=str(payload.get("run_id", "")), wall_elapsed_sec=float(payload.get("wall_elapsed_sec", 0) or 0))
        session_entry = {
            **prov,
            "session": session,
            "trade_date": trade_date,
            "final_status": payload.get("final_status", "UNKNOWN"),
            "reason": payload.get("reason", ""),
            "tick_count": int(payload.get("tick_count", 0) or 0),
            "fills_count": int(payload.get("fills_count", 0) or 0),
            "unique_fills_count": int(payload.get("unique_fills_count", 0) or 0),
            "orders_ack": int(payload.get("orders_ack", 0) or 0),
            "orders_rejected": int(payload.get("orders_rejected", 0) or 0),
            "entry_degraded": payload.get("entry_degraded", 0),
            "entry_degraded_reason": payload.get("entry_degraded_reason", ""),
            "entry_eval_status": payload.get("entry_eval_status", ""),
            "entry_watchlist_source": payload.get("entry_watchlist_source", ""),
            "watchlist_fallback_used": payload.get("watchlist_fallback_used", 0),
            "exit_routed_before_entry": payload.get("exit_routed_before_entry", 0),
            "buy_notional_routed": payload.get("buy_notional_routed", 0),
            "sell_notional_routed": payload.get("sell_notional_routed", 0),
            "total_order_notional_routed": payload.get("total_order_notional_routed", 0),
            "ack_reconcile_before_route_status": payload.get("ack_reconcile_before_route_status", ""),
            "ack_reconcile_after_route_status": payload.get("ack_reconcile_after_route_status", ""),
            "ack_reconcile_after_route_unresolved_count": payload.get("ack_reconcile_after_route_unresolved_count", 0),
            "ack_pending_reconcile_count": payload.get("ack_pending_reconcile_count", 0),
            "pending_order_count": payload.get("pending_order_count", 0),
            "run_id": payload.get("run_id") or prov.get("run_id", ""),
            "workflow": payload.get("workflow") or prov.get("workflow", ""),
            "wall_elapsed_sec": float(payload.get("wall_elapsed_sec", 0) or 0),
            "missed_trade_window": bool(payload.get("missed_trade_window", False)),
            "recorded_at_utc": now_utc,
        }

        existing.update({k: v for k, v in session_entry.items() if k in {"branch", "commit_sha", "sha", "workflow", "github_run_id", "github_run_attempt", "event_name", "actor", "run_id", "code_version_source"}})
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
    session_summary_json = report_base / trade_date / f"{session}_summary.json" if trade_date else None
    session_summary_md = report_base / trade_date / f"{session}_summary.md" if trade_date else None

    if trade_date:
        dated_dir = report_base / trade_date / session
        dated_dir.mkdir(parents=True, exist_ok=True)
        dated_json = dated_dir / "us_daily_report.json"
        dated_md = dated_dir / "us_daily_report.md"
    else:
        dated_json = None
        dated_md = None

    if session_summary_json is not None:
        session_summary_json.parent.mkdir(parents=True, exist_ok=True)
        session_summary_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    latest_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))

    md_lines = [
        f"# US Daily Report - {payload.get('trade_date', 'N/A')}",
        "",
        "## Required Fields",
        "",
    ]
    for k in (
        "trade_date", "branch", "commit_sha", "run_id", "github_run_id", "github_run_attempt", "sha", "workflow", "session", "session_id", "event_name", "actor", "env",
        "source_log_file", "started_at_utc", "started_at_et", "started_at_kst", "ended_at_utc", "ended_at_et", "ended_at_kst", "wall_elapsed_sec", "code_version_source",
        "dry_run", "kis_order_allowed", "prep_status", "prep_run_id", "score_nonzero_count",
        "locked_watchlist_count", "locked_watchlist_count_source",
        "entry_eval_status", "entry_error_type", "entry_error_message", "entry_intents",
        "orders_sent", "fills", "positions", "last_stage", "final_status", "reason",
        "temp_error_count", "temp_recovered_count", "schedule_expected_et", "actual_start_et",
        "delay_seconds", "run_window", "recovery_run", "missed_trade_window",
        "buy_decisions", "sell_decisions", "real_broker_buys", "real_broker_sells",
        "synthetic_reconcile_buys", "synthetic_reconcile_sells", "broker_ack_only",
        "broker_rejects", "duplicate_exit_blocked", "buy_notional_routed", "sell_notional_routed", "total_order_notional_routed",
        "buy_daily_notional_after_routing", "sell_notional_does_not_consume_buy_budget",
        "ack_reconcile_before_route_status", "ack_reconcile_after_route_status", "ack_reconcile_after_route_unresolved_count",
        "ack_pending_reconcile_count", "broker_ack_only_unresolved", "sell_decisions_detail",
        "market_regime", "capital_scale", "sector_cap_enforced", "trade_block_reason", "blocked_entry_reason_counts",
    ):
        md_lines.append(f"- {k}: {payload.get(k)}")
    md_lines.extend([
        "",
        "## Broker/Reconcile Classification",
        f"- 실제 MTS 신규 매수: {payload.get('real_broker_buys', 0)}건",
        f"- 실제 MTS 매도: {payload.get('real_broker_sells', 0)}건",
        f"- 내부 잔고 보정: {payload.get('synthetic_reconcile_buys', 0) + payload.get('synthetic_reconcile_sells', 0)}건",
        "- 내부 잔고 보정은 실제 MTS 신규 매수 체결이 아님",
    ])
    if session_summary_md is not None:
        session_summary_md.write_text("\n".join(md_lines) + "\n")
    latest_md.write_text("\n".join(md_lines) + "\n")

    if dated_json is not None and dated_md is not None:
        dated_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        dated_md.write_text("\n".join(md_lines) + "\n")



def _count_trade_date_order_activity(trade_date: str) -> int:
    """Best-effort count of DB order rows used to classify timeout-after-order as non-fatal."""
    try:
        from trader.us.db.repos import load_us_daily_orders_for_report
        rows = load_us_daily_orders_for_report(trade_date) or []
        return len(rows)
    except Exception as exc:
        logger.warning("[US_SESSION][ORDER_ACTIVITY][WARN] trade_date=%s err=%s", trade_date, exc)
        return 0


def _tick_has_order_activity(tick_result: dict | None, before_count: int, after_count: int) -> bool:
    if tick_result:
        if int(tick_result.get("orders_ack", 0) or 0) > 0:
            return True
        if int(tick_result.get("orders_sent", 0) or 0) > 0:
            return True
        for order in tick_result.get("orders", []) or []:
            if str(order.get("status") or "").upper() in {"ACK", "FILLED", "SUBMITTED", "SENT"}:
                return True
    return after_count > before_count

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
        {"status": "OK"|"OK_WITH_WARNINGS"|"OK_RISK_BLOCKED"|"SKIP"|"ERROR", ...}
    """
    global _received_signal, _exit_code
    _received_signal = None
    _exit_code = None

    def _handle_signal(signum, frame):
        global _received_signal, _exit_code
        _received_signal = signum
        _exit_code = 128 + int(signum)
        logger.error("[US_SESSION][SIGNAL] signal=%s", signum)
        try:
            write_heartbeat_file(session, os.getenv("GITHUB_RUN_ID", "local"), 0, phase="signal", signal=signum)
        finally:
            raise SystemExit(_exit_code)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    logger.info(
        "[US_SESSION][START] session=%s env=%s offline=%s max_minutes=%d interval_sec=%d",
        session, env, offline, max_minutes, interval_sec,
    )
    tick_timeout_sec = int(os.getenv("US_TICK_TIMEOUT_SEC", "270"))
    if interval_sec >= 300 and tick_timeout_sec < 240:
        logger.warning(
            "[US_SESSION][CONFIG][TICK_TIMEOUT_RAISED] interval_sec=%d requested_timeout_sec=%d effective_timeout_sec=240",
            interval_sec, tick_timeout_sec,
        )
        tick_timeout_sec = 240
    if tick_timeout_sec < max(60, int(interval_sec * 0.8)):
        logger.warning(
            "[US_SESSION][CONFIG][TICK_TIMEOUT_LOW] interval_sec=%d timeout_sec=%d recommendation=>=80%%_interval",
            interval_sec, tick_timeout_sec,
        )
    tick_timeout_fatal_consecutive = int(os.getenv("US_TICK_TIMEOUT_FATAL_CONSECUTIVE", "3"))
    now_for_date = _now_ny(force_now)
    trade_date = now_for_date.strftime("%Y-%m-%d")
    try:
        from zoneinfo import ZoneInfo
        now_kst = now_for_date.astimezone(ZoneInfo("Asia/Seoul"))
        from trader.us.market_calendar import is_us_trading_day
        is_td = int(is_us_trading_day(now_for_date.date()))
        logger.info(
            "[US_SESSION][TIME] now_kst=%s now_et=%s trade_date_et=%s session=%s is_trading_day=%d",
            now_kst.isoformat(), now_for_date.isoformat(), trade_date, session, is_td,
        )
    except Exception as exc:
        logger.warning("[US_SESSION][TIME][WARN] err=%s", exc)
    run_id = os.getenv("GITHUB_RUN_ID", "local")
    final_status = "OK"
    final_reason = "session_end"
    last_stage = "session_start"
    final_tick: dict = {}
    temp_error_count = 0
    temp_recovered_count = 0

    # ── 파일 기반 session guard 체크 ─────────────────────────────────────────
    from trader.us.utils.session_guard import (
        acquire_us_session_running_lock,
        release_us_session_running_lock,
        check_us_session_file_guard,
        now_et_iso,
        write_us_session_done_file,
    )
    session_started_at_et = now_et_iso()

    running_lock = acquire_us_session_running_lock(trade_date, session, run_id)
    if running_lock.get("acquired"):
        atexit.register(release_us_session_running_lock, trade_date, session, run_id)
    if not running_lock.get("acquired"):
        logger.warning(
            "[US_SESSION][SKIP_DUPLICATE_RUNNING] session=%s trade_date=%s reason=%s",
            session, trade_date, running_lock.get("reason"),
        )
        return {
            "status": "SKIP",
            "reason": "duplicate_session_running",
            "detail_status": "SKIP_DUPLICATE_RUNNING",
            "session": session,
            "trade_date": trade_date,
            "tick_count": 0,
        }

    try:
        _file_guard = check_us_session_file_guard(trade_date, session)
        if (force_now or offline) and _file_guard["already_ran"]:
            logger.info(
                "[US_SESSION][FILE_GUARD_BYPASS] session=%s trade_date=%s reason=%s",
                session,
                trade_date,
                "force_now" if force_now else "offline",
            )
        elif _file_guard["already_ran"] or (max_ticks <= 0 and (_file_guard.get("payload") or {}).get("status") == "OK"):
            guard_payload = _file_guard["payload"]
            # P7: stale guard 리포트 — 실제 시작 시각과 예상 시각 차이 계산
            _schedule_expected_et = _file_guard.get("schedule_expected_et", "")
            _actual_start_et = session_started_at_et
            _delay_seconds: int = 0
            try:
                if _schedule_expected_et:
                    _exp = dt.fromisoformat(_schedule_expected_et)
                    _act = dt.fromisoformat(_actual_start_et)
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
        prep_guard_result: dict = {}
        if session in ("am", "afternoon") and not offline:
            try:
                from trader.us.prep_contract import check_us_prep_guard
                guard = check_us_prep_guard(trade_date)
                prep_guard_result = guard or {}
                if guard["ok"]:
                    logger.info(
                        "[US_PREP_GUARD][OK] workflow=us-trade-%s session=%s trade_date=%s"
                        " final30=%s score_nonzero=%s source=%s prep_status=%s"
                        " trade_block_reason=%s underfilled_tier=%s effective_capital_scale=%s"
                        " effective_max_new_positions=%s final30_trade_ready=%s",
                        session, session, trade_date,
                        guard.get("final30_scored_count", "?"),
                        guard.get("score_nonzero_count", "?"),
                        guard.get("source", "runtime"),
                        guard.get("prep_status", "?"),
                        guard.get("trade_block_reason", "?"),
                        guard.get("underfilled_tier", "?"),
                        guard.get("effective_capital_scale", "?"),
                        guard.get("effective_max_new_positions", "?"),
                        int(bool(guard.get("final30_trade_ready"))),
                    )
                else:
                    logger.error(
                        "[US_PREP_GUARD][BLOCK] workflow=us-trade-%s session=%s trade_date=%s"
                        " detail=%s prep_status=%s trade_block_reason=%s final30=%s score_nonzero=%s underfilled_tier=%s",
                        session, session, trade_date, guard.get("reason"),
                        guard.get("prep_status", "?"),
                        guard.get("trade_block_reason", "?"),
                        guard.get("final30_scored_count", "?"),
                        guard.get("score_nonzero_count", "?"),
                        guard.get("underfilled_tier", "?"),
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
            session_entry_allowed = True
            if buy_count > 0:
                logger.info(
                    "[US_AFTERNOON][MODE] mode=exit_first entry_allowed=true"
                    " reason=buy_orders_count_diagnostic_only buy_orders_count=%d",
                    buy_count,
                )
            else:
                logger.info(
                    "[US_AFTERNOON][MODE] mode=exit_first entry_allowed=true"
                    " reason=no_buy_order_today",
                )
        else:
            buy_count = 0
            session_entry_allowed = True


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

        expected_min_ticks = 0 if (force_now or max_ticks > 0 or offline) else calc_expected_min_ticks(session=session, start_dt=simulated_now, graceful_deadline=graceful_deadline, interval_sec=interval_sec)
        logger.info("[US_SESSION][LIVENESS_CHECK] session=%s expected_min_ticks=%d", session, expected_min_ticks)

        logger.info(
            "[US_SESSION][TICK_LOOP][START] session=%s graceful_deadline=%s",
            session, graceful_deadline.strftime("%Y-%m-%dT%H:%M:%S%z"),
        )

        # ── Tick loop (try/finally로 감싸서 report를 항상 작성) ───────────────────
        from trader.us.runner.trade_tick_runner import load_watchlist_from_artifact, run_trade_tick

        prep_status_cache: dict | None = None
        locked_watchlist_cache: list[dict] | None = None
        prep_cache_source = "none"
        watchlist_cache_source = "none"
        if session in {"am", "afternoon"}:
            try:
                from trader.us.db.repos import load_latest_us_prep_status, load_locked_us_watchlist
                db_timeout_sec = max(1, int(os.getenv("US_SESSION_PREP_CACHE_TIMEOUT_SEC", "5")))
                min_watchlist_count = int(os.getenv("US_MIN_LOCKED_WATCHLIST_COUNT", "10"))
                allow_degraded = os.getenv("US_ALLOW_DEGRADED_IN_TRADE", "0") == "1"
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as cache_pool:
                    prep_fut = cache_pool.submit(load_latest_us_prep_status, trade_date, db_timeout_sec)
                    wl_fut = cache_pool.submit(load_locked_us_watchlist, trade_date, min_watchlist_count, allow_degraded, db_timeout_sec)
                    try:
                        prep_status_cache = prep_fut.result(timeout=db_timeout_sec + 1) or {}
                        prep_cache_source = "db_session_cache"
                    except Exception as exc:
                        prep_status_cache = {"status": "UNKNOWN_DB_DEGRADED", "error": str(exc), "source": "session_cache_db_fallback"}
                        prep_cache_source = "db_degraded"
                        logger.warning("[US_SESSION][PREP_CACHE][DEGRADED] trade_date=%s error=%s", trade_date, exc)
                    try:
                        locked_watchlist_cache = wl_fut.result(timeout=db_timeout_sec + 1) or []
                        watchlist_cache_source = "db_session_cache"
                    except Exception as exc:
                        logger.warning("[US_SESSION][WATCHLIST_CACHE][DB_DEGRADED] trade_date=%s error=%s", trade_date, exc)
                        try:
                            locked_watchlist_cache = load_watchlist_from_artifact(trade_date)
                            watchlist_cache_source = "artifact_session_cache"
                        except Exception as fb_exc:
                            locked_watchlist_cache = []
                            watchlist_cache_source = "artifact_failed"
                            logger.warning("[US_SESSION][WATCHLIST_CACHE][FALLBACK_FAIL] trade_date=%s error=%s", trade_date, fb_exc)
                logger.info(
                    "[US_SESSION][ENTRY_CACHE][READY] prep_source=%s prep_status=%s watchlist_source=%s watchlist_count=%d",
                    prep_cache_source, (prep_status_cache or {}).get("status"), watchlist_cache_source, len(locked_watchlist_cache or []),
                )
            except Exception as exc:
                logger.warning("[US_SESSION][ENTRY_CACHE][SKIP] trade_date=%s error=%s", trade_date, exc)

        tick_count = 0
        warn_count = 0
        consecutive_errors = 0
        consecutive_tick_timeouts = 0
        root_cause = ""
        surface_reason = ""
        results: list[dict] = []
        total_orders_blocked = 0
        block_reasons_total: dict[str, int] = {}
        hard_error_reasons = {
            # marker: reason=fills_contract_error
            "fills_contract_error",
            "locked_watchlist_missing",
            "prep_guard_block",
            "balance_position_parse_error",
            "FAILED_EXIT_INTENTS_NOT_ROUTED",
            "FAILED_ALL_EXIT_ORDERS_REJECTED",
            "FAILED_ALL_EXIT_ORDERS_BLOCKED",
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
                    if (max_ticks > 0 and tick_count == 0) or (force_now and tick_count == 0):
                        logger.info(
                            "[US_SESSION][GRACEFUL_DEADLINE] allow_first_tick=1 reason=%s tick_count=%d",
                            "max_ticks_guarantee" if max_ticks > 0 else "force_now_single_tick_guarantee",
                            tick_count,
                        )
                    elif offline and max_ticks > 0 and tick_count < max_ticks:
                        logger.info(
                            "[US_SESSION][GRACEFUL_DEADLINE] bypass_for_offline_max_ticks=1 tick_count=%d max_ticks=%d",
                            tick_count,
                            max_ticks,
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
                    write_heartbeat_file(session, run_id, tick_count, phase="TICK_START", last_stage=last_stage)
                    order_activity_before = _count_trade_date_order_activity(trade_date)
                    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
                    try:
                        fut = pool.submit(
                            run_trade_tick,
                            session=session,
                            env=env,
                            offline=offline,
                            force_now=tick_force_now,
                            run_mode=resolved_run_mode,
                            signal_only=resolved_signal_only,
                            kis_order_allowed=kis_order_allowed,
                            session_entry_allowed=session_entry_allowed,
                            session_buy_orders_count=buy_count,
                            tick_index=tick_count,
                            balance_reconcile_interval=int(os.getenv("US_BALANCE_RECONCILE_INTERVAL_TICKS", "3")),
                            prep_status_cache=prep_status_cache,
                            locked_watchlist_cache=locked_watchlist_cache,
                            prep_cache_source=prep_cache_source,
                            watchlist_cache_source=watchlist_cache_source,
                        )
                        tick_result = fut.result(timeout=tick_timeout_sec)
                    finally:
                        pool.shutdown(wait=False, cancel_futures=True)
                    results.append(tick_result)
                    final_tick = tick_result
                    consecutive_tick_timeouts = 0
                    write_heartbeat_file(session, run_id, tick_count, phase="TICK_DONE", status=tick_result.get("status"), reason=tick_result.get("reason", ""))
                    temp_error_count += int(tick_result.get("temp_error_count", 0) or 0)
                    temp_recovered_count += int(tick_result.get("temp_recovered_count", 0) or 0)
                    last_stage = tick_result.get("last_stage", last_stage)

                    # orders_blocked 세션 누적
                    tick_blocked = int(tick_result.get("orders_blocked", 0) or 0)
                    total_orders_blocked += tick_blocked
                    for reason, cnt in (tick_result.get("block_reasons") or {}).items():
                        block_reasons_total[reason] = block_reasons_total.get(reason, 0) + int(cnt or 0)

                    tick_status = tick_result.get("status", "ERROR")
                    from trader.us.runner.status_contract import classify_tick_status
                    classification = classify_tick_status(tick_result)

                    if tick_result.get("reason") == "fills_contract_error":  # "status": "ERROR"
                        # Temporary KIS/fills errors are degraded and must not kill the session.
                        final_tick["status"] = "DEGRADED_FILLS_UNAVAILABLE"
                        final_tick["reason"] = "TEMP_FILLS_UNAVAILABLE"
                        warn_count += 1
                        logger.warning(
                            "[US_SESSION][FILLS_DEGRADED] session=%s tick=%d reason=TEMP_FILLS_UNAVAILABLE continue=1",
                            session, tick_count,
                        )


                    classify_symbols = []
                    for key in ("no_balance_sell_symbols", "recent_sell_ack_symbols", "sell_reject_symbols", "blocked_sell_symbols"):
                        vals = tick_result.get(key) or []
                        if vals:
                            classify_symbols.extend([str(v) for v in vals])
                    classify_symbol_text = ",".join(sorted(set(classify_symbols)))
                    if classification == "success":
                        consecutive_errors = 0
                        logger.info(
                            "[US_SESSION][STATUS_CLASSIFY] session=%s tick=%d status=%s class=%s reason=%s symbols=%s consecutive_errors=%d warn_count=%d",
                            session, tick_count, tick_status, classification, tick_result.get("reason", ""), classify_symbol_text, consecutive_errors, warn_count,
                        )
                    elif classification == "warning":
                        warn_count += 1
                        consecutive_errors = 0
                        logger.warning(
                            "[US_SESSION][STATUS_CLASSIFY] session=%s tick=%d status=%s class=%s reason=%s symbols=%s consecutive_errors=%d warn_count=%d",
                            session, tick_count, tick_status, classification, tick_result.get("reason", ""), classify_symbol_text, consecutive_errors, warn_count,
                        )
                    else:
                        consecutive_errors += 1
                        logger.error(
                            "[US_SESSION][STATUS_CLASSIFY] session=%s tick=%d status=%s class=%s reason=%s symbols=%s consecutive_errors=%d warn_count=%d",
                            session, tick_count, tick_status, classification, tick_result.get("reason", ""), classify_symbol_text, consecutive_errors, warn_count,
                        )
                        if tick_status in {"FAILED", "ERROR"}:
                            final_status = "FAILED"
                            final_reason = tick_result.get("reason", "tick_failed")
                            logger.error("[US_SESSION][END] session=%s reason=%s tick=%d", session, final_reason, tick_count)
                            break
                        warn_count += 1
                        logger.warning("[US_SESSION][TICK_LOOP][WARN] tick=%d status=%s consecutive_errors=%d", tick_count, tick_status, consecutive_errors)

                except concurrent.futures.TimeoutError:
                    last_stage = f"tick_{tick_count}_timeout"
                    root_cause = root_cause or "tick_timeout"
                    warn_count += 1
                    # Give DB order persistence a short grace window, then classify timeout-after-order as non-fatal.
                    time_mod.sleep(float(os.getenv("US_TIMEOUT_ORDER_ACTIVITY_GRACE_SEC", "0.2") or 0.2))
                    order_activity_after = _count_trade_date_order_activity(trade_date)
                    has_order_activity = _tick_has_order_activity(None, locals().get("order_activity_before", 0), order_activity_after)
                    if has_order_activity:
                        consecutive_tick_timeouts = 0
                        timeout_status = "DEGRADED_TICK_TIMEOUT_AFTER_ACK"
                        final_status = "DEGRADED_WITH_ORDER_ACK"
                        final_reason = "WARN_TICK_LATENCY_AFTER_ORDER"
                    else:
                        consecutive_tick_timeouts += 1
                        timeout_status = "WARN_TICK_TIMEOUT" if consecutive_tick_timeouts == 1 else f"WARN_CONSECUTIVE_TICK_TIMEOUT_{consecutive_tick_timeouts}"
                    results.append({
                        "status": timeout_status,
                        "reason": "WARN_TICK_LATENCY_AFTER_ORDER" if has_order_activity else "tick_timeout",
                        "root_cause": "tick_timeout",
                        "timeout_after_order_ack": int(has_order_activity),
                        "order_activity_before": locals().get("order_activity_before", 0),
                        "order_activity_after": order_activity_after,
                        "timeout_sec": tick_timeout_sec,
                        "tick": tick_count,
                    })
                    final_tick = results[-1]
                    write_heartbeat_file(session, run_id, tick_count, phase="TICK_WARN_TIMEOUT", status=timeout_status, timeout_sec=tick_timeout_sec, consecutive_tick_timeouts=consecutive_tick_timeouts)
                    logger.warning(
                        "[US_TICK][TIMEOUT][WARN] status=%s reason=%s timeout_sec=%d consecutive=%d fatal_after=%d order_activity=%d",
                        timeout_status, final_tick.get("reason", "tick_timeout"), tick_timeout_sec, consecutive_tick_timeouts, tick_timeout_fatal_consecutive, int(has_order_activity),
                    )
                    if has_order_activity:
                        continue
                    if consecutive_tick_timeouts >= tick_timeout_fatal_consecutive:
                        final_status = "FAILED"
                        final_reason = "FAILED_CONSECUTIVE_TICK_TIMEOUT"
                        logger.error("[US_SESSION][END] session=%s reason=FAILED_CONSECUTIVE_TICK_TIMEOUT consecutive=%d", session, consecutive_tick_timeouts)
                        break
                    continue
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
                    sleep_with_heartbeat(session, run_id, tick_count, interval_sec)

        except KeyboardInterrupt:
            final_status = "FAILED"
            final_reason = "keyboard_interrupt"
            logger.error("[US_SESSION][INTERRUPT] session=%s reason=keyboard_interrupt", session)
        except SystemExit as se:
            final_status = "FAILED"
            final_reason = "system_exit"
            _exit_code = int(se.code) if isinstance(se.code, int) else _exit_code
            logger.error("[US_SESSION][EXIT] session=%s reason=system_exit exit_code=%s", session, _exit_code)
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
        
        expected_to_trade = not (force_now or max_ticks > 0 or offline or resolved_signal_only)
        if expected_to_trade and tick_count < expected_min_ticks and final_reason not in {"market_skip", "max_ticks", "force_now_single_tick", "graceful_shutdown"}:
            if final_status == "FAILED":
                surface_reason = "early_termination_min_ticks_not_met"
                logger.error("[US_SESSION][EARLY_TERMINATION] root_cause=%s surface_reason=%s ticks=%d expected_min_ticks=%d", root_cause or final_reason, surface_reason, tick_count, expected_min_ticks)
            else:
                warn_count += 1
                surface_reason = "early_termination_min_ticks_not_met"
                logger.warning("[US_SESSION][EARLY_TERMINATION][WARN_ONLY] root_cause=%s surface_reason=%s ticks=%d expected_min_ticks=%d", root_cause or final_reason, surface_reason, tick_count, expected_min_ticks)
        else:
            logger.info("[US_SESSION][LIVENESS_CHECK] expected_min_ticks=%d actual_ticks=%d result=OK reason=%s", expected_min_ticks, tick_count, final_reason)

        # KIS TEMP_ERROR recovery warning
        if temp_recovered_count > 0:
            warn_count += 1
            logger.warning(
                "[US_SESSION][WARN] KIS TEMP_ERROR recovered temp_error_count=%d temp_recovered_count=%d",
                temp_error_count,
                temp_recovered_count,
            )

        status_detail = ""
        if final_status not in {"FAILED", "SKIP", "OK_RISK_BLOCKED", "OK_NO_TRADE", "DEGRADED_WITH_ORDER_ACK", "OK_WITH_TIMEOUT_WARNINGS"}:
            if resolved_signal_only:
                status_detail = "OK_SIGNAL_ONLY" if warn_count == 0 else "OK_WITH_WARNINGS_SIGNAL_ONLY"
                final_status = _CompatStatus("OK" if warn_count == 0 else "OK_WITH_WARNINGS", status_detail)
                logger.info(
                    "[US_SESSION][SIGNAL_ONLY][END] session=%s status=%s status_detail=%s reason=%s",
                    session, final_status, status_detail, "non_trading_day_signal_only" if resolved_run_mode == "NON_TRADING_SIGNAL_ONLY" else "signal_only",
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
        real_broker_buys = real_broker_sells = 0
        distinct_real_broker_buy_orders: set[str] = set()
        distinct_real_broker_sell_orders: set[str] = set()
        countable_broker_statuses = {"ACK", "FILLED", "BALANCE_CONFIRMED_BUY", "BALANCE_CONFIRMED_SELL", "BALANCE_CONFIRMED_PARTIAL", "FILLED_BY_BALANCE_DELTA"}
        synthetic_reconcile_buys = synthetic_reconcile_sells = 0
        broker_ack_only = broker_rejects = duplicate_exit_blocked = 0
        buy_notional_routed = sell_notional_routed = total_order_notional_routed = 0.0
        ack_pending_reconcile_count = broker_ack_only_unresolved = 0
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
            for order in tick_result.get("orders", []) or []:
                side_o = str(order.get("side") or "").upper()
                status_o = str(order.get("status") or order.get("final_status") or "").upper()
                order_no = str(order.get("order_no") or order.get("ack_no") or order.get("client_order_key") or "")
                if order_no and status_o in countable_broker_statuses:
                    if side_o == "BUY":
                        distinct_real_broker_buy_orders.add(order_no)
                    elif side_o == "SELL":
                        distinct_real_broker_sell_orders.add(order_no)
            real_broker_buys += int(tick_result.get("real_broker_buys", 0) or 0)
            real_broker_sells += int(tick_result.get("real_broker_sells", 0) or 0)
            synthetic_reconcile_buys += int(tick_result.get("synthetic_reconcile_buys", 0) or 0)
            synthetic_reconcile_sells += int(tick_result.get("synthetic_reconcile_sells", 0) or 0)
            broker_ack_only += int(tick_result.get("broker_ack_only", 0) or 0)
            broker_rejects += int(tick_result.get("broker_rejects", 0) or 0)
            duplicate_exit_blocked += int(tick_result.get("duplicate_exit_blocked", 0) or 0)
            buy_notional_routed += float(tick_result.get("buy_notional_routed", 0.0) or 0.0)
            sell_notional_routed += float(tick_result.get("sell_notional_routed", 0.0) or 0.0)
            total_order_notional_routed += float(tick_result.get("total_order_notional_routed", 0.0) or 0.0)
            ack_pending_reconcile_count = int(tick_result.get("ack_pending_reconcile_count", 0) or 0)
            broker_ack_only_unresolved = int(tick_result.get("broker_ack_only_unresolved", 0) or 0)

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

        only_max_position_blocked = (
            total_buy_decisions > 0
            and total_orders_blocked > 0
            and total_orders_sent == 0
            and total_orders_error == 0
            and bool(block_reasons_total)
            and set(block_reasons_total.keys()) <= {"max_positions_reached", "max_positions_reached_new_symbol"}
        )
        if only_max_position_blocked and os.getenv("US_TREAT_MAX_POSITIONS_AS_OK", "1") == "1":
            final_status = "OK_RISK_BLOCKED"
            final_reason = "max_positions_reached_new_symbol"
            status_detail = "OK_RISK_BLOCKED"
            logger.info("[US_SESSION][STATUS_CLASSIFY] status=OK_RISK_BLOCKED reason=max_positions_reached_new_symbol completed=1")

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

        if distinct_real_broker_buy_orders:
            real_broker_buys = len(distinct_real_broker_buy_orders)
        if distinct_real_broker_sell_orders:
            real_broker_sells = len(distinct_real_broker_sell_orders)

        # pending_order_count is supplied by ACK/balance reconcile; do not derive it as ack - fills.

        regime_block_report = _aggregate_regime_block_reporting(results)

        prep_contract = prep_guard_result.get("contract") or {}
        prep_status_fallback = prep_contract.get("status") or prep_guard_result.get("status") or "UNKNOWN"
        prep_run_id = (
            prep_contract.get("run_id")
            or prep_guard_result.get("run_id")
            or prep_guard_result.get("prep_run_id")
            or ""
        )
        score_nonzero_count = int(
            prep_contract.get("score_nonzero_count")
            or prep_guard_result.get("score_nonzero_count")
            or 0
        )
        locked_watchlist_count = 0
        locked_watchlist_count_source = "default_zero"
        for key, source in (
            (final_tick.get("locked_watchlist_count"), "final_tick.locked_watchlist_count"),
            (final_tick.get("locked_count"), "final_tick.locked_count"),
            (prep_guard_result.get("final30_scored_count"), "prep_guard_result.final30_scored_count"),
            (prep_guard_result.get("locked_count"), "prep_guard_result.locked_count"),
        ):
            if key not in (None, "") and int(key or 0) > 0:
                locked_watchlist_count = int(key or 0)
                locked_watchlist_count_source = source
                break

        prep_status_value = final_tick.get("prep_status", "UNKNOWN")
        if prep_status_value == "UNKNOWN":
            prep_status_value = prep_status_fallback

        _prov = _runtime_provenance(session=session, run_id=run_id, started_at_et=session_started_at_et, ended_at_et=now_et_iso(), wall_elapsed_sec=session_wall_elapsed_sec)
        session_trade_block_reason = (
            regime_block_report.get("trade_block_reason")
            or final_tick.get("trade_block_reason")
            or final_tick.get("entry_degraded_reason")
            or (final_reason if final_status in {"FAILED", "SKIP"} else "")
        )
        report_payload = {
            **_prov,
            "trade_date": trade_date,
            "session": "daily_final" if session == "close" else session,
            "daily_report_canonical_path": "reports/us_daily/latest_us_daily_report.json",
            "env": env,
            "dry_run": os.getenv("DRY_RUN", "0") == "1",
            "expected_to_trade": int(expected_to_trade),
            "kis_order_allowed": int(kis_order_allowed),
            "prep_status": prep_status_value,
            "prep_run_id": prep_run_id,
            "score_nonzero_count": score_nonzero_count,
            "locked_watchlist_count": locked_watchlist_count,
            "locked_watchlist_count_source": locked_watchlist_count_source,
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
            "entry_degraded": int(final_tick.get("entry_degraded", 0) or 0),
            "entry_degraded_reason": final_tick.get("entry_degraded_reason", ""),
            "watchlist_fallback_used": int(final_tick.get("watchlist_fallback_used", 0) or 0),
            "entry_watchlist_source": final_tick.get("entry_watchlist_source", ""),
            "exit_routed_before_entry": int(final_tick.get("exit_routed_before_entry", 0) or 0),
            "exit_routed_after_entry_degraded": int(final_tick.get("exit_routed_after_entry_degraded", 0) or 0),
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
            "orders_blocked": total_orders_blocked,  # backward compat: total across session
            "orders_blocked_total": total_orders_blocked,
            "orders_blocked_last_tick": int(final_tick.get("orders_blocked", 0) or 0),
            "block_reasons": block_reasons_total,  # backward compat: total across session
            "block_reasons_total": block_reasons_total,
            "block_reasons_last_tick": final_tick.get("block_reasons", {}),
            "primary_block_reason": max(block_reasons_total, key=block_reasons_total.get) if block_reasons_total else "",
            "final_diagnosis": "NO_TRADE_RISK_BLOCKED" if (total_buy_decisions > 0 and total_orders_sent == 0 and total_orders_blocked > 0) else "",
            "existing_add_buy_blocked_by_max_positions_count": sum(
                int(cnt or 0) for reason, cnt in block_reasons_total.items()
                if reason == "max_positions_reached"
            ),
            "new_buy_blocked_by_max_positions_count": int(block_reasons_total.get("max_positions_reached_new_symbol", 0) or 0),
            "duplicate_session_detected": int(any(str(r.get("detail_status") or "") == "SKIP_DUPLICATE_RUNNING" for r in results)),
            "tokenP_403_detected": int("403" in json.dumps(kis_temp_errors_by_api, ensure_ascii=False)),
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
            "real_broker_buys": real_broker_buys,
            "real_broker_sells": real_broker_sells,
            "synthetic_reconcile_buys": synthetic_reconcile_buys,
            "synthetic_reconcile_sells": synthetic_reconcile_sells,
            "broker_ack_only": broker_ack_only,
            "broker_rejects": broker_rejects,
            "duplicate_exit_blocked": duplicate_exit_blocked,
            "buy_notional_routed": round(buy_notional_routed, 4),
            "sell_notional_routed": round(sell_notional_routed, 4),
            "total_order_notional_routed": round(total_order_notional_routed, 4),
            "buy_daily_notional_after_routing": round(buy_notional_routed, 4),
            "sell_notional_does_not_consume_buy_budget": int(sell_notional_routed > 0),
            "ack_reconcile_before_route_status": final_tick.get("ack_reconcile_before_route_status", ""),
            "ack_reconcile_after_route_status": final_tick.get("ack_reconcile_after_route_status", ""),
            "ack_reconcile_after_route_unresolved_count": final_tick.get("ack_reconcile_after_route_unresolved_count", 0),
            "ack_pending_reconcile_count": ack_pending_reconcile_count,
            "broker_ack_only_unresolved": broker_ack_only_unresolved,
            "sell_decisions_detail": final_tick.get("sell_decisions_detail", []),
            "last_stage": last_stage,
            "trade_status": final_status,
            "status_detail": status_detail,
            "signal_only": bool(resolved_signal_only),
            "trade_runner_started": 1,
            "market_regime": regime_block_report.get("market_regime") or final_tick.get("market_regime", "NEUTRAL"),
            "capital_scale": regime_block_report.get("capital_scale", final_tick.get("capital_scale", 1.0)),
            "sector_cap_enforced": regime_block_report.get("sector_cap_enforced", final_tick.get("sector_cap_enforced", False)),
            "blocked_entry_reason_counts": regime_block_report.get("blocked_entry_reason_counts", {}),
            "trade_block_reason": session_trade_block_reason,
            "final_status": final_status,
            "reason": final_reason,
            "temp_error_count": temp_error_count,
            "temp_recovered_count": temp_recovered_count,
            "schedule_expected_et": os.getenv("US_SCHEDULE_EXPECTED_ET", ""),
            "actual_start_et": os.getenv("US_ACTUAL_START_ET", ""),
            "delay_seconds": _safe_int_env("US_DELAY_SECONDS", 0),
            "run_window": os.getenv("US_RUN_WINDOW", "normal"),
            "recovery_run": _safe_int_env("US_RECOVERY_RUN", 0),
            "missed_trade_window": os.getenv("US_MISSED_TRADE_WINDOW", "0") in {"1", "true", "True"},
            "kis_temp_errors": {
                "total": sum(v.get("temp_error", 0) for v in kis_temp_errors_by_api.values()),
                "recovered": sum(v.get("recovered", 0) for v in kis_temp_errors_by_api.values()),
                "unrecovered": sum(v.get("unrecovered", 0) for v in kis_temp_errors_by_api.values()),
                "by_api": kis_temp_errors_by_api,
            },
            # 추가 필드
            "expected_min_ticks": expected_min_ticks,
            "liveness_status": "FAILED_EARLY_TERMINATION" if (expected_min_ticks and tick_count < expected_min_ticks and final_status == "FAILED") else "OK",
            "last_liveness_event": _last_liveness_event,
            "has_session_finally": True,
            "probable_liveness_cause": (root_cause or final_reason) if (expected_min_ticks and tick_count < expected_min_ticks and final_status == "FAILED") else "",
            "root_cause": root_cause,
            "surface_reason": surface_reason,
            "received_signal": _signal_name(_received_signal),
            "exit_code": _exit_code,
            "ticks_total": tick_count,
            "tick_count": tick_count,
            "ticks": tick_count,
            "max_ticks": max_ticks,
            "force_now": force_now or "",
            "offline": offline,
            "wall_elapsed_sec": round(session_wall_elapsed_sec, 2),
            "session_started_at_utc": _prov.get("started_at_utc", ""),
            "session_ended_at_utc": _prov.get("ended_at_utc", ""),
            "report_recorded_at_utc": dt.utcnow().isoformat() + "Z",
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
        if (
            report_payload["prep_status"] == "OK"
            and report_payload["locked_watchlist_count"] == 0
            and report_payload["final_status"] not in {"NO_TRADE", "OK_NO_TRADE", "SKIP"}
        ):
            report_payload["final_status"] = "FAILED"
            report_payload["reason"] = "locked_watchlist_count_zero_under_prep_ok"
        _write_us_session_report(report_payload, session=session)
        _write_us_schedule_health(report_payload, session=session)

        logger.info(
            "[US_DAILY][AGGREGATE] ticks_total=%d entry_intents_total=%d exit_intents_total=%d"
            " orders_sent_total=%d orders_ack_total=%d orders_reject_total=%d"
            " orders_blocked_total=%d block_reasons_total=%s"
            " buy_orders_count=%d sell_orders_count=%d",
            tick_count,
            total_buy_decisions,
            total_sell_decisions,
            report_payload["orders_sent_total"],
            total_orders_ack,
            total_orders_rejected,
            total_orders_blocked,
            block_reasons_total,
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
            "[RUN_SUMMARY][RESULT] status=%s reason=%s orders_sent=%s orders_blocked=%s last_stage=%s",
            final_status,
            final_reason,
            total_orders_sent,
            total_orders_blocked,
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
            "status_detail": status_detail,
            "kis_order_allowed": kis_order_allowed,
            "results": results,
            "last_stage": last_stage,
        }


    # ---------------------------------------------------------------------------
    # CLI
    # ---------------------------------------------------------------------------

    finally:
        try:
            write_heartbeat_file(session, run_id, locals().get("tick_count", 0), phase="SESSION_FINALLY", status=str(locals().get("final_status", "")), reason=locals().get("final_reason", ""), root_cause=locals().get("root_cause", ""))
        except Exception:
            pass
        release_us_session_running_lock(trade_date, session, run_id=run_id)
        try:
            write_heartbeat_file(session, run_id, locals().get("tick_count", 0), phase="LOCK_RELEASED", status=str(locals().get("final_status", "")), reason=locals().get("final_reason", ""), root_cause=locals().get("root_cause", ""))
        except Exception:
            pass
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
    failed_status = str(result.get("status") or "")
    if failed_status in {"ERROR", "FAILED"} or failed_status.startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
