# -*- coding: utf-8 -*-
"""US Trade Close Runner.

- fills 조회
- positions reconcile
- 미체결 주문 점검
- pnl snapshot
- report 준비
"""
from __future__ import annotations

import argparse
import logging
import sys

logger = logging.getLogger(__name__)


def run_trade_close(env: str = "practice", offline: bool = False, force_now: str | None = None) -> dict:
    logger.info("[US_TRADE_CLOSE][START] env=%s offline=%s", env, offline)
    logger.info(
        "[US_TRADE_CLOSE][FORCE_NOW] enabled=%s force_now=%s",
        1 if force_now else 0,
        force_now or "",
    )

    from trader.us.data_provider import USDataProvider
    from trader.us.db.repos import (
        save_fills, save_position_snapshot, save_reconcile_log,
    )
    from trader.us.market_calendar import now_ny
    from datetime import datetime
    from zoneinfo import ZoneInfo

    provider = USDataProvider(offline=offline)

    # force_now가 있으면 해당 날짜 기준으로 trade_date 설정
    NY_TZ = ZoneInfo("America/New_York")
    if force_now:
        now = datetime.fromisoformat(force_now).astimezone(NY_TZ)
    else:
        now = now_ny()
    trade_date = now.strftime("%Y-%m-%d")

    # 1. Fills 조회
    fills: list[dict] = []
    fills_status = "SKIP" if offline else "UNKNOWN"
    fills_error = ""
    
    if not offline:
        try:
            from trader.us.execution.fills import get_fills_today
            fills_result = get_fills_today(provider=provider, trade_date=trade_date)
            fills_status = fills_result.get("status", "UNKNOWN")
            fills = fills_result["fills"]
            fills_error = fills_result.get("error", "")
            
            if fills_status != "OK":
                logger.error(
                    "[US_TRADE_CLOSE][ERROR] fills failed: status=%s error=%s",
                    fills_status,
                    fills_error,
                )
            logger.info("[US_TRADE_CLOSE][FILLS] count=%d status=%s", len(fills), fills_status)
        except Exception as exc:
            logger.error("[US_TRADE_CLOSE][ERROR] fills exception: %s", exc)
            fills_status = "ERROR"
            fills_error = str(exc)
    else:
        logger.info("[US_TRADE_CLOSE][FILLS] offline — skipping KIS fills")

    # 2. Fills DB 저장
    try:
        save_fills(fills)
        logger.info("[US_FILLS][SAVE] count=%d", len(fills))
    except Exception as exc:
        logger.warning("[US_TRADE_CLOSE][WARN] save_fills failed: %s", exc)

    # 3. Reconcile
    reconcile_result: dict = {"status": "SKIP", "positions": []}
    if not offline:
        try:
            from trader.us.execution.reconcile import reconcile_positions
            reconcile_result = reconcile_positions(provider=provider)
            logger.info("[US_TRADE_CLOSE][RECONCILE] status=%s", reconcile_result.get("status"))
        except Exception as exc:
            logger.error("[US_TRADE_CLOSE][ERROR] reconcile failed: %s", exc)
            reconcile_result = {"status": "ERROR", "error": str(exc), "positions": []}

    # 4. Positions DB 저장
    positions = reconcile_result.get("positions", [])
    try:
        save_position_snapshot(positions)
        logger.info("[US_POSITIONS][SNAPSHOT][SAVE] count=%d", len(positions))
    except Exception as exc:
        logger.warning("[US_TRADE_CLOSE][WARN] save_position_snapshot failed: %s", exc)

    # 5. Reconcile log DB 저장
    try:
        save_reconcile_log({
            "status": reconcile_result.get("status", "OK"),
            "message": reconcile_result.get("error", ""),
            "position_count": len(positions),
            "total_pvs": reconcile_result.get("total_pvs_usd", 0),
            "detail": {"env": env, "runner": "trade_close"},
        })
        logger.info("[US_RECONCILE_LOG][SAVE]")
    except Exception as exc:
        logger.warning("[US_TRADE_CLOSE][WARN] save_reconcile_log failed: %s", exc)

    # 6. Balance snapshot
    balance = {}
    try:
        balance = provider.get_balance()
    except Exception as exc:
        logger.warning("[US_TRADE_CLOSE][WARN] balance fetch failed: %s", exc)

    # 7. Daily report
    try:
        from trader.us.runner.daily_report_runner import run_daily_report
        run_daily_report(env=env, offline=offline)
    except Exception as exc:
        logger.warning("[US_TRADE_CLOSE][WARN] daily report failed: %s", exc)

    # 8. Status 계산
    status = "OK"
    if fills_status == "CONTRACT_ERROR":
        status = "ERROR"
    elif reconcile_result.get("status") == "ERROR":
        status = "ERROR"
    elif fills_status not in ("OK", "SKIP"):
        status = "OK_WITH_WARNINGS"
    elif reconcile_result.get("status") not in ("OK", "SKIP"):
        status = "OK_WITH_WARNINGS"
    
    # 9. Final 로그
    if status == "OK":
        logger.info("[US_TRADE_CLOSE][OK]")
    elif status == "OK_WITH_WARNINGS":
        logger.warning(
            "[US_TRADE_CLOSE][WARNINGS] fills_status=%s reconcile_status=%s",
            fills_status,
            reconcile_result.get("status"),
        )
    else:
        logger.error(
            "[US_TRADE_CLOSE][ERROR] final_status=ERROR fills_status=%s reconcile_status=%s fills_error=%s",
            fills_status,
            reconcile_result.get("status"),
            fills_error,
        )
    
    return {
        "status": status,
        "fills_status": fills_status,
        "fills_error": fills_error,
        "fills_count": len(fills),
        "positions_count": len(positions),
        "reconcile_status": reconcile_result.get("status"),
        "balance": balance,
    }


def main() -> None:
    from trader.us.utils.logging_utils import setup_us_logging
    setup_us_logging()
    parser = argparse.ArgumentParser(description="US Trade Close Runner")
    parser.add_argument("--env", default="practice")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--force-now", dest="force_now", default=None)
    args = parser.parse_args()
    result = run_trade_close(env=args.env, offline=args.offline, force_now=args.force_now)
    if result["status"] == "ERROR":
        sys.exit(1)


if __name__ == "__main__":
    main()
