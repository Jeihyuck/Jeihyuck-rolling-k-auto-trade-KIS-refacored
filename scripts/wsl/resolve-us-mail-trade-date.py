#!/usr/bin/env python3
"""Resolve the intended completed US exchange session independently of log files.

Never choose yesterday simply because its Close files exist while today's are
missing. Mail only one ET session's evidence; incomplete sessions are DEGRADED.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")


def latest_completed_us_trade_date(reference_et: datetime) -> str:
    from trader.us.market_calendar import (
        is_us_market_holiday, is_us_weekend, regular_close_time_for_date,
        us_calendar_load_error, us_calendar_supported_years,
    )

    if reference_et.tzinfo is None:
        raise ValueError("US_MAIL_REFERENCE_TIME_NEEDS_OFFSET")
    if us_calendar_load_error():
        raise RuntimeError("US_MAIL_CALENDAR_UNAVAILABLE")
    now = reference_et.astimezone(NY)
    for days_back in range(15):
        day = now.date() - timedelta(days=days_back)
        if day.year not in us_calendar_supported_years():
            raise RuntimeError("US_MAIL_CALENDAR_UNSUPPORTED_YEAR")
        if is_us_weekend(day) or is_us_market_holiday(day):
            continue
        # Even half-day sessions must actually have closed. The resolver does
        # not assume a completed session from folder names or a static 16:00.
        close = datetime.combine(day, regular_close_time_for_date(day), NY)
        if now >= close:
            return day.isoformat()
    raise RuntimeError("US_MAIL_NO_COMPLETED_SESSION_WITHIN_15_DAYS")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference-et", default=None,
                        help="ISO8601 aware NY instant for offline tests")
    args = parser.parse_args()
    try:
        when = (datetime.fromisoformat(args.reference_et)
                if args.reference_et else datetime.now(NY))
        print(latest_completed_us_trade_date(when))
        return 0
    except Exception as exc:
        print(f"[US_MAIL][TRADE_DATE_UNRESOLVED] type={type(exc).__name__}",
              file=__import__("sys").stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
