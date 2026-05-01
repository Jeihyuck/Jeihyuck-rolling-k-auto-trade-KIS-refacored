# -*- coding: utf-8 -*-
"""US Daily Report Runner."""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def run_daily_report(env: str = "practice", offline: bool = False) -> dict:
    logger.info("[US_DAILY_REPORT][START] env=%s", env)

    trade_date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    report = {
        "trade_date": trade_date,
        "env": env,
        "orders_sent": 0,
        "fills": 0,
        "positions": 0,
        "errors": [],
        "next_actions": [],
    }

    # TODO: DB에서 당일 주문/체결/포지션 집계
    logger.info("[US_DAILY_REPORT][OK] date=%s", trade_date)
    return {"status": "OK", "report": report}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="US Daily Report Runner")
    parser.add_argument("--env", default="practice")
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    result = run_daily_report(env=args.env, offline=args.offline)
    if result["status"] == "ERROR":
        sys.exit(1)


if __name__ == "__main__":
    main()
