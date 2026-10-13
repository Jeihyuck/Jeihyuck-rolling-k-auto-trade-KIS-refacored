"""Reconstruct KIS overseas order timestamps across the Korea midnight boundary.

KIS overseas CCNL rows have been observed using `ord_dt` as the US trade
*session* date while `ord_tmd` describes the Korea-local wall clock.
Combining both as one KST calendar date silently moves post-midnight orders
backward by a whole day (e.g. 20261012/002821 is 2026-10-13 00:28:21 KST).
Never use such a shifted time as terminal broker evidence.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

_KST = ZoneInfo("Asia/Seoul")
_ET = ZoneInfo("America/New_York")


def resolve_kis_us_order_datetime(
    us_trade_date: str, korea_order_time: str,
) -> datetime:
    """Unique KST wall-time whose America/New_York date equals KIS ord_dt.

    Return a timezone-aware datetime. Fail closed on an ambiguous/broken
    timestamp rather than silently accepting a wrong chronological proof.
    """
    raw_date = str(us_trade_date or "").strip().replace("-", "")
    raw_time = str(korea_order_time or "").strip().replace(":", "").zfill(6)
    if (len(raw_date) != 8 or not raw_date.isdigit()
            or len(raw_time) != 6 or not raw_time.isdigit()):
        raise ValueError("invalid KIS US trade date / Korea order time")
    session_day = datetime.strptime(raw_date, "%Y%m%d").date()
    clock = datetime.strptime(raw_time, "%H%M%S").time()
    candidates = []
    for shift in (-1, 0, 1, 2):
        candidate = datetime.combine(
            session_day + timedelta(days=shift), clock, tzinfo=_KST,
        )
        if candidate.astimezone(_ET).date() == session_day:
            candidates.append(candidate)
    if len(candidates) != 1:
        raise ValueError("KIS order timestamp ambiguous across KST/ET")
    return candidates[0]


def kis_order_source_calendar_rollover(us_trade_date: str, korea_time: str) -> int:
    """Number of KST calendar days added to the broker's US session date."""
    market_day = date.fromisoformat(
        str(us_trade_date).replace("-", "")[:4] + "-"
        + str(us_trade_date).replace("-", "")[4:6] + "-"
        + str(us_trade_date).replace("-", "")[6:8]
    )
    return (resolve_kis_us_order_datetime(us_trade_date, korea_time).date() - market_day).days
