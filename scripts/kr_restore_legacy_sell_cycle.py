"""Operator-triggered KR PB1 legacy missing-cycle repair, practice only.

Read-only evidence check:
  FORCE_HTTP=1 NO_TRADE=1 DRY_RUN=1 KIS_ENV=practice python -m scripts.kr_restore_legacy_sell_cycle \
    --code 122630 --buy-day 20261026 --sell-day 20261027

Only after reviewing the READY_TO_BIND evidence, repeat with --apply.
This may update THREE missing order cycle links. It NEVER marks orders
FILLED, writes fills, changes positions/P&L, or submits broker orders.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
import sys


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--code", required=True)
    parser.add_argument("--buy-day", required=True)
    parser.add_argument("--sell-day", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    if (
        os.getenv("NO_TRADE") != "1"
        or os.getenv("DRY_RUN") != "1"
        or os.getenv("KIS_ENV") != "practice"
        or os.getenv("FORCE_HTTP") != "1"
        or len(args.code) != 6 or not args.code.isdigit()
    ):
        print(json.dumps({"state":"BLOCKED","reason":"PRACTICE_NO_TRADE_FORCE_GET_REQUIRED"}))
        return 3
    try:
        dates = {
            datetime.strptime(args.buy_day, "%Y%m%d").date(),
            datetime.strptime(args.sell_day, "%Y%m%d").date(),
        }
        if len(dates) != 2 or args.sell_day <= args.buy_day:
            raise ValueError("dates")
    except ValueError:
        print(json.dumps({"state":"BLOCKED","reason":"ORIGINAL_DATES_INVALID"}))
        return 3
    for key,value in (
        ("LIVE_TRADING_ENABLED","0"),
        ("KR_LIVE_TRADING_ENABLED","0"),
        ("KR_ORDER_ARMED","0"),
        ("DISABLE_REAL_TRADING","1"),
    ):
        os.environ[key]=value

    from trader.kis_wrapper import KisAPI
    from trader.db.engine import make_engine
    from trader.kr.runtime_integrity_20260929 import _daily_ccld_all_pages_for_date
    from trader.kr.legacy_missing_cycle_repair import certify_legacy_pb1_missing_cycle

    engine=make_engine()
    try:
        kis=KisAPI(kis_env="practice")
        if str(kis.env).lower() != "practice":
            raise RuntimeError("PRACTICE_KIS_ACCOUNT_REQUIRED")
        balance, source = kis.get_balance_cached(force=True, return_source=True)
        pages = {
            d.strftime("%Y%m%d"):_daily_ccld_all_pages_for_date(kis,d)
            for d in sorted(dates)
        }
        result=certify_legacy_pb1_missing_cycle(
            engine=engine,code=args.code,original_daily_pages=pages,
            broker_balance=balance,balance_source=source,apply=args.apply,
        )
        print(json.dumps(result,ensure_ascii=False,default=str))
        return 0 if result.get("state") in {"READY_TO_BIND","BOUND"} else 2
    except Exception as exc:
        print(json.dumps({"state":"BLOCKED","reason":type(exc).__name__}))
        return 2
    finally:
        engine.dispose()


if __name__=="__main__":
    sys.exit(main())
