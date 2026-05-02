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


def run_trade_close(env: str = "practice", offline: bool = False) -> dict:
    logger.info("[US_TRADE_CLOSE][START] env=%s offline=%s", env, offline)

    from trader.us.data_provider import USDataProvider
    from trader.us.db.repos import (
        save_fills, save_position_snapshot, save_reconcile_log,
    )

    provider = USDataProvider(offline=offline)

    # 1. Fills 조회
    fills: list[dict] = []
    if not offline:
        try:
            from trader.us.execution.fills import get_fills_today
            fills = get_fills_today(provider=provider)
            logger.info("[US_TRADE_CLOSE][FILLS] count=%d", len(fills))
        except Exception as exc:
            logger.error("[US_TRADE_CLOSE][ERROR] fills failed: %s", exc)
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

    logger.info("[US_TRADE_CLOSE][OK]")
    return {
        "status": "OK",
        "fills_count": len(fills),
        "positions_count": len(positions),
        "reconcile_status": reconcile_result.get("status"),
        "balance": balance,
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="US Trade Close Runner")
    parser.add_argument("--env", default="practice")
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    result = run_trade_close(env=args.env, offline=args.offline)
    if result["status"] == "ERROR":
        sys.exit(1)


if __name__ == "__main__":
    main()
