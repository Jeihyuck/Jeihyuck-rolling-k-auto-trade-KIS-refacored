#!/usr/bin/env python3
"""Canonical NULLIM trading-day gate (0=open, 10=closed, 1=unknown/error)."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def emit(market: str, day: date, *, is_open: bool | None, status: str, reason: str, **extra: object) -> int:
    payload = {"market": market.upper(), "trade_date": day.isoformat(), "is_trading_day": is_open,
               "status": status, "reason": reason, **extra}
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if is_open is True else 10 if is_open is False else 1


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
            return emit(args.market, day, is_open=override == "open",
                        status="TRADING_DAY" if override == "open" else "SKIPPED_NON_TRADING_DAY",
                        reason="MARKET_OPEN" if override == "open" else "MARKET_HOLIDAY",
                        calendar_source="test_override")
        if args.market == "kr":
            from trader.time_utils import resolve_krx_trading_day_strict
            state, source = resolve_krx_trading_day_strict(day)
            if state == "UNKNOWN":
                return emit("kr", day, is_open=True, status="TRADING_DAY",
                            reason="MARKET_OPEN_FAIL_OPEN",
                            calendar_source=source or "KRX_CALENDAR_UNAVAILABLE_FAIL_OPEN")
            return emit("kr", day, is_open=state == "OPEN",
                        status="TRADING_DAY" if state == "OPEN" else "SKIPPED_NON_TRADING_DAY",
                        reason="MARKET_OPEN" if state == "OPEN" else "MARKET_HOLIDAY", calendar_source=source)
        from trader.us.market_calendar import (
            is_us_early_close_day, is_us_market_holiday, is_us_weekend,
            regular_close_time_for_date, us_calendar_load_error, us_calendar_supported_years,
        )
        if is_us_weekend(day):
            return emit("us", day, is_open=False, status="SKIPPED_NON_TRADING_DAY", reason="WEEKEND", calendar_source="WEEKEND")
        load_error = us_calendar_load_error()
        if load_error:
            return emit("us", day, is_open=None, status="CALENDAR_ERROR", reason=load_error)
        if day.year not in us_calendar_supported_years():
            return emit("us", day, is_open=None, status="CALENDAR_ERROR", reason="UNSUPPORTED_CALENDAR_YEAR")
        closed = is_us_market_holiday(day); early = is_us_early_close_day(day)
        return emit("us", day, is_open=not closed,
                    status="SKIPPED_NON_TRADING_DAY" if closed else "TRADING_DAY",
                    reason="MARKET_HOLIDAY" if closed else "MARKET_OPEN",
                    calendar_source="US_CONFIG", early_close=early,
                    regular_close_et=regular_close_time_for_date(day).strftime("%H:%M"))
    except Exception as exc:
        if args.market == "kr":
            return emit("kr", date.fromisoformat(args.trade_date), is_open=True,
                        status="TRADING_DAY", reason="MARKET_OPEN_FAIL_OPEN",
                        calendar_source="KRX_CALENDAR_UNAVAILABLE_FAIL_OPEN",
                        detail=f"{type(exc).__name__}: {exc}")
        return emit("us", date.fromisoformat(args.trade_date), is_open=None,
                    status="CALENDAR_ERROR", reason="US_CALENDAR_UNAVAILABLE",
                    detail=f"{type(exc).__name__}: {exc}")


if __name__ == "__main__":
    raise SystemExit(main())
