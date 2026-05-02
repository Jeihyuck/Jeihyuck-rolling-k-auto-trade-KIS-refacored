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
import logging
import os
import sys
import time as time_mod
from datetime import datetime, timedelta

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
) -> dict:
    """AM 또는 Afternoon session 실행.

    각 tick은 trade_tick_runner.run_trade_tick()을 호출한다.
    단일 tick 오류는 warning으로 기록하고 계속 진행한다.
    연속 3회 tick 실패 시 session ERROR 종료한다.

    Returns:
        {"status": "OK"|"OK_WITH_WARNINGS"|"SKIP"|"ERROR", ...}
    """
    logger.info(
        "[US_SESSION][START] session=%s env=%s offline=%s max_minutes=%d interval_sec=%d",
        session, env, offline, max_minutes, interval_sec,
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
    base_forced_now = _now_ny(force_now) if force_now else None
    now = base_forced_now if base_forced_now is not None else _now_ny(None)
    logger.info("[US_SESSION][PHASE_GUARD] session=%s now_et=%s", session, now.strftime("%H:%M:%S"))

    if not is_us_trading_day(now.date()):
        logger.info("[US_SESSION][SKIP] not_trading_day date=%s", now.date())
        return {"status": "SKIP", "reason": "not_trading_day"}

    # ── 예산 요약 ─────────────────────────────────────────────────────────────
    budget = resolve_us_order_budget(10000.0)  # stub — tick runner가 실제 조회
    logger.info("[US_SESSION][BUDGET] cap_usd=%.2f", budget["capital_usd_cap"])

    # ── 세션 종료 시각 및 deadline 계산 ──────────────────────────────────────
    session_end = _session_end_dt(session, now)
    max_end = now + timedelta(minutes=max_minutes)
    deadline = min(session_end, max_end)

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

    while True:
        if base_forced_now is not None:
            tick_now_dt = base_forced_now + timedelta(seconds=interval_sec * tick_count)
            tick_force_now = tick_now_dt.isoformat()
        else:
            tick_now_dt = _now_ny(None)
            tick_force_now = None

        if tick_now_dt >= deadline:
            logger.info(
                "[US_SESSION][END] session=%s reason=session_end ticks=%d",
                session, tick_count,
            )
            break

        tick_count += 1
        logger.info(
            "[US_TICK_LOOP][TICK] session=%s tick=%d force_now=%s",
            session,
            tick_count,
            tick_force_now or "",
        )

        try:
            tick_result = run_trade_tick(
                session=session,
                env=env,
                offline=offline,
                force_now=tick_force_now,
            )
            results.append(tick_result)

            tick_status = tick_result.get("status", "ERROR")
            if tick_status in ("OK", "OK_WITH_WARNINGS", "SKIP"):
                if tick_status == "OK_WITH_WARNINGS":
                    warn_count += 1
                    logger.warning(
                        "[US_SESSION][TICK_LOOP][WARN] tick=%d status=%s",
                        tick_count, tick_status,
                    )
                consecutive_errors = 0
            else:
                consecutive_errors += 1
                warn_count += 1
                logger.warning(
                    "[US_SESSION][TICK_LOOP][WARN] tick=%d status=%s consecutive_errors=%d",
                    tick_count, tick_status, consecutive_errors,
                )

        except Exception as exc:
            consecutive_errors += 1
            warn_count += 1
            logger.warning(
                "[US_SESSION][TICK_LOOP][WARN] tick=%d exception=%s consecutive_errors=%d",
                tick_count, exc, consecutive_errors,
            )
            results.append({"status": "ERROR", "error": str(exc)})

        if consecutive_errors >= _MAX_CONSECUTIVE_ERRORS:
            logger.error(
                "[US_SESSION][TICK_LOOP][ERROR] session=%s consecutive_errors=%d — aborting",
                session, consecutive_errors,
            )
            logger.info("[US_SESSION][END] session=%s reason=consecutive_errors", session)
            return {
                "status": "ERROR",
                "session": session,
                "reason": "consecutive_errors",
                "tick_count": tick_count,
                "warn_count": warn_count,
                "results": results,
            }

        # SKIP이면 loop 완전 종료 (마켓 미개장 등)
        if tick_result.get("status") == "SKIP" and tick_count == 1:
            logger.info("[US_SESSION][END] session=%s reason=market_skip", session)
            return {
                "status": "SKIP",
                "session": session,
                "reason": tick_result.get("reason", "market_skip"),
            }

        if force_now and max_ticks > 0 and tick_count >= max_ticks:
            logger.info(
                "[US_SESSION][END] session=%s reason=max_ticks ticks=%d",
                session,
                tick_count,
            )
            break

        if force_now and max_ticks == 0:
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

    final_status = "OK_WITH_WARNINGS" if warn_count > 0 else "OK"
    return {
        "status": final_status,
        "session": session,
        "tick_count": tick_count,
        "warn_count": warn_count,
        "results": results,
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
    args = parser.parse_args()

    result = run_trade_session(
        session=args.session,
        env=args.env,
        offline=args.offline,
        max_minutes=args.max_minutes,
        interval_sec=args.interval_sec,
        force_now=args.force_now,
        max_ticks=args.max_ticks,
    )
    if result["status"] == "ERROR":
        sys.exit(1)


if __name__ == "__main__":
    main()
