#!/usr/bin/env python3
"""Canonical NULLIM trading-day gate (0=open, 10=closed, 1=error)."""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from datetime import date


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--market", required=True, choices=("kr", "us"))
    parser.add_argument("--date", required=True, dest="trade_date")
    args = parser.parse_args()
    try:
        day = date.fromisoformat(args.trade_date)
        override = os.getenv("NULLIM_TRADING_DAY_OVERRIDE", "").strip().lower()
        if override:
            if override not in {"open", "closed"}:
                raise ValueError("NULLIM_TRADING_DAY_OVERRIDE must be open or closed")
            is_open = override == "open"
            source = "test_override"
        elif args.market == "us":
            from trader.us.market_calendar import is_us_trading_day
            is_open = bool(is_us_trading_day(day)); source = "trader.us.market_calendar.is_us_trading_day"
        else:
            from trader.time_utils import is_krx_trading_day
            is_open = bool(is_krx_trading_day(day)); source = "trader.time_utils.is_krx_trading_day"
        payload = {
            "market": args.market.upper(), "trade_date": day.isoformat(),
            "is_trading_day": is_open, "status": "TRADING_DAY" if is_open else "SKIPPED_NON_TRADING_DAY",
            "reason": "MARKET_OPEN" if is_open else "MARKET_HOLIDAY", "calendar_source": source,
        }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 0 if is_open else 10
    except Exception as exc:
        print(json.dumps({"market": args.market.upper(), "trade_date": args.trade_date,
                          "is_trading_day": None, "status": "CALENDAR_ERROR",
                          "reason": type(exc).__name__, "detail": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
