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
    report: dict = {
        "trade_date": trade_date,
        "env": env,
        "orders_sent": 0,
        "orders_dry_run": 0,
        "orders_blocked": 0,
        "orders_rejected": 0,
        "fills": 0,
        "positions": 0,
        "dry_run": True,
        "budget_cap_usd": None,
        "errors": [],
    }

    # DB에서 당일 집계
    try:
        from trader.us.db.repos import load_today_order_keys, load_positions, load_today_symbols_sold
        order_keys = load_today_order_keys(trade_date=trade_date)
        report["orders_sent"] = len(order_keys)
        positions = load_positions(as_of=trade_date)
        report["positions"] = len(positions)
        sold = load_today_symbols_sold(trade_date=trade_date)
        report["fills"] = len(sold)
    except Exception as exc:
        report["errors"].append(f"DB query failed: {exc}")
        logger.warning("[US_DAILY_REPORT][WARN] DB query failed: %s", exc)

    # 예산 cap
    try:
        from trader.us.budget import get_us_capital_usd_cap
        report["budget_cap_usd"] = round(get_us_capital_usd_cap(), 2)
    except Exception:
        pass

    # DRY_RUN 여부
    try:
        from trader.utils.env import env_bool
        report["dry_run"] = env_bool("DRY_RUN", default=True)
    except Exception:
        pass

    # 리포트 파일 저장 (실패해도 workflow 중단 안 함)
    try:
        import os
        report_dir = "reports/us_daily"
        os.makedirs(report_dir, exist_ok=True)
        report_path = f"{report_dir}/latest_us_daily_report.md"
        with open(report_path, "w") as f:
            f.write(f"# US Daily Report — {trade_date}\n\n")
            f.write(f"- env: {env}\n")
            f.write(f"- dry_run: {report['dry_run']}\n")
            f.write(f"- budget_cap_usd: {report['budget_cap_usd']}\n")
            f.write(f"- orders_sent: {report['orders_sent']}\n")
            f.write(f"- fills: {report['fills']}\n")
            f.write(f"- positions: {report['positions']}\n")
            if report["errors"]:
                f.write(f"- errors: {report['errors']}\n")
        logger.info("[US_DAILY_REPORT][SAVED] path=%s", report_path)
    except Exception as exc:
        logger.warning("[US_DAILY_REPORT][WARN] report save failed: %s", exc)

    logger.info("[US_DAILY_REPORT][OK] date=%s orders=%d", trade_date, report["orders_sent"])
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
