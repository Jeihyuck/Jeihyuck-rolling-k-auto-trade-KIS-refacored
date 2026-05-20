# -*- coding: utf-8 -*-
"""US Trade Open Runner.

- market open 확인
- prep에서 생성된 order intent 로드
- risk gate 통과
- DRY_RUN 또는 paper order
- order ack 저장
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def run_trade_open(
    env: str = "practice",
    offline: bool = False,
    force_now: str | None = None,
) -> dict:
    """Trade Open 단계 실행.

    Args:
        force_now: ISO 날짜시간 문자열 (테스트/harness 용). 예: "2026-01-01T10:00:00-05:00"
    """
    logger.info("[US_TRADE_OPEN][START] env=%s offline=%s", env, offline)

    # Market open 확인
    from trader.us.market_calendar import is_us_regular_market_open, market_phase
    if force_now:
        from datetime import datetime as DT
        now = DT.fromisoformat(force_now)
    else:
        from trader.us.market_calendar import now_ny
        now = now_ny()

    phase = market_phase(now)
    if phase not in ("REGULAR_OPEN", "REGULAR_MID", "REGULAR_CLOSE"):
        logger.info("[US_MARKET][CLOSED] phase=%s dt=%s", phase, now)
        logger.info("[US_TRADE_OPEN][SKIP] market not open")
        return {"status": "SKIP", "reason": "market_closed", "phase": phase}

    logger.info("[US_TRADE_OPEN][MARKET_OPEN] phase=%s", phase)

    # Prep 단계에서 intent 생성 (이 runner에서 재실행하는 간소화 버전)
    from trader.us.runner.prep_runner import run_prep
    prep_result = run_prep(env=env, offline=offline)

    if prep_result["status"] != "OK":
        logger.error("[US_TRADE_OPEN][ERROR] prep failed")
        return {"status": "ERROR", "reason": "prep_failed"}

    intents = prep_result.get("intents", [])
    if not intents:
        logger.info("[US_TRADE_OPEN][NO_INTENTS] nothing to trade")
        return {"status": "OK", "orders": [], "reason": "no_intents"}

    # Data Provider for orderable cash
    from trader.us.data_provider import USDataProvider
    provider = USDataProvider(offline=offline)
    available_cash = provider.get_orderable_cash()

    # Order routing
    from trader.us.execution.order_router import route_order
    orders = []
    daily_notional = 0.0
    position_count = 0

    for intent in intents:
        result = route_order(
            intent,
            current_daily_notional_usd=daily_notional,
            current_position_count=position_count,
            total_portfolio_usd=max(available_cash, 1000.0),
            available_cash_usd=available_cash,
        )
        orders.append(result)
        if result["status"] in ("DRY_RUN", "ACK"):
            daily_notional += float(intent.get("notional_usd", 0))
            if intent.get("side") == "BUY":
                position_count += 1

    ack_cnt = sum(1 for o in orders if o["status"] == "ACK")
    dry_cnt = sum(1 for o in orders if o["status"] == "DRY_RUN")
    blocked_cnt = sum(1 for o in orders if o["status"] == "BLOCKED")

    logger.info(
        "[US_TRADE_OPEN][DONE] total=%d ack=%d dry_run=%d blocked=%d",
        len(orders), ack_cnt, dry_cnt, blocked_cnt,
    )
    return {
        "status": "OK",
        "orders": orders,
        "ack": ack_cnt,
        "dry_run": dry_cnt,
        "blocked": blocked_cnt,
    }


def main() -> None:
    from trader.us.utils.logging_utils import setup_us_logging
    setup_us_logging()
    parser = argparse.ArgumentParser(description="US Trade Open Runner")
    parser.add_argument("--env", default="practice")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--force-now", dest="force_now", default=None,
                        help="ISO datetime string for market_phase override")
    args = parser.parse_args()

    result = run_trade_open(env=args.env, offline=args.offline, force_now=args.force_now)
    if result["status"] == "ERROR":
        sys.exit(1)


if __name__ == "__main__":
    main()
