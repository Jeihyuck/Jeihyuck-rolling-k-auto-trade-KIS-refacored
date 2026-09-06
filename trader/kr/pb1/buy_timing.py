from __future__ import annotations

from datetime import datetime


def is_buy_allowed_now(*, now: datetime, entry_cutoff_dt: datetime, market_close_dt: datetime) -> tuple[bool, str, datetime, datetime]:
    if now >= market_close_dt:
        return False, "MARKET_CLOSED", entry_cutoff_dt, market_close_dt
    if now >= entry_cutoff_dt:
        return False, "ENTRY_CUTOFF_PASSED", entry_cutoff_dt, market_close_dt
    return True, "TIME_WINDOW_OK", entry_cutoff_dt, market_close_dt
