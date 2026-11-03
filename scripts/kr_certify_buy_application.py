"""Operator-only original KIS proof certification, never an order submission.

Dry-run (default):
  FORCE_HTTP=1 NO_TRADE=1 DRY_RUN=1 KIS_ENV=practice python -m scripts.kr_certify_buy_application \
    --order-id 1c610884-7952-49f1-8d7d-4e2712240d8c --trade-date 20261103

To *only* save broker-proven application metadata after reviewing the result,
repeat with --apply. Quantity, P&L, fills and order statuses never change.
"""
from __future__ import annotations

import argparse
import json
import os
import sys


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="KR practice existing BUY application certification")
    parser.add_argument("--order-id", required=True)
    parser.add_argument("--trade-date", required=True, help="Original KIS YYYYMMDD")
    parser.add_argument("--apply", action="store_true", help="Metadata-only guarded DB update")
    args = parser.parse_args(argv)

    if (
        os.getenv("KIS_ENV") != "practice"
        or os.getenv("NO_TRADE") != "1"
        or os.getenv("DRY_RUN") != "1"
        or os.getenv("FORCE_HTTP") != "1"
        or not (len(args.trade_date) == 8 and args.trade_date.isdigit())
    ):
        print(json.dumps({"state": "BLOCKED", "reason": "PRACTICE_READONLY_FLAGS_AND_DATE_REQUIRED"}))
        return 3
    for key, value in (
        ("KR_ORDER_ARMED", "0"),
        ("LIVE_TRADING_ENABLED", "0"),
        ("KR_LIVE_TRADING_ENABLED", "0"),
        ("DISABLE_REAL_TRADING", "1"),
    ):
        os.environ[key] = value

    from trader.kis_wrapper import KisAPI
    from trader.db.engine import make_engine
    from trader.kr.verified_buy_watermark import certify_existing_pb1_buy_watermark

    engine = make_engine()
    try:
        kis = KisAPI(kis_env="practice")
        if str(kis.env).lower() != "practice":
            raise RuntimeError("NONPRACTICE_KIS_CLIENT")
        fresh_balance, source = kis.get_balance_cached(force=True, return_source=True)
        original = kis.inquire_daily_ccld(
            start_date=args.trade_date, end_date=args.trade_date
        )
        result = certify_existing_pb1_buy_watermark(
            engine=engine, order_id=args.order_id,
            kis_balance=fresh_balance, balance_source=source,
            original_day_ccld=original, apply=args.apply,
        )
        print(json.dumps(result, ensure_ascii=False, default=str))
        return 0 if result.get("state") in {
            "READY_TO_CERTIFY", "ALREADY_CERTIFIED", "CERTIFIED",
        } else 2
    except Exception as exc:
        # No KIS CCLD proof => NO metadata mutation.
        print(json.dumps({"state": "BLOCKED",
                          "reason": type(exc).__name__}))
        return 2
    finally:
        engine.dispose()


if __name__ == "__main__":
    sys.exit(main())
