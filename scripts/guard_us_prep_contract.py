#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Guard same-day US prep contract before trading session starts.

Checks:
- prep_status must be OK or OK_WITH_WARNINGS
- locked_watchlist count must be >= 10

Exits with code 1 if guard fails.
"""
from datetime import datetime
from zoneinfo import ZoneInfo
import os
import sys

# Add repo to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from trader.us.db.repos import (
    load_latest_us_prep_status,
    load_locked_us_watchlist,
)

session = os.getenv("SESSION", "unknown")
force_now = os.getenv("FORCE_NOW_INPUT", "").strip()

if force_now:
    trade_date = datetime.fromisoformat(force_now).date().isoformat()
else:
    trade_date = datetime.now(ZoneInfo("America/New_York")).date().isoformat()

prep = load_latest_us_prep_status(trade_date) or {}
status = prep.get("status", "UNKNOWN")

rows = load_locked_us_watchlist(
    trade_date=trade_date,
    min_count=10,
    allow_degraded=True,
)
locked_count = len(rows or [])

print(
    f"[US_PREP_GUARD][CHECK] session={session} trade_date={trade_date} "
    f"prep_status={status} locked_count={locked_count}"
)

if status not in ("OK", "OK_WITH_WARNINGS") or locked_count < 10:
    print(
        f"[US_PREP_GUARD][FAIL] session={session} trade_date={trade_date} "
        f"prep_status={status} locked_count={locked_count}"
    )
    sys.exit(1)

print(
    f"[US_PREP_GUARD][OK] session={session} trade_date={trade_date} "
    f"prep_status={status} locked_count={locked_count}"
)
