# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
logger = logging.getLogger(__name__)
logging.getLogger("pykrx").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.WARNING)
KR_CALENDAR_PYKRX_ENABLED = os.getenv("KR_CALENDAR_PYKRX_ENABLED", "0") == "1"


def now_kst() -> datetime:
    return datetime.now(KST)


def _weekday_trade_date(candidate: date) -> date:
    d = candidate
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def _weekday_prev_trade_date(trade_date: date) -> date:
    d = trade_date - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _krx_nearest_business_day(candidate: date, *, prev: bool) -> date | None:
    try:
        from pykrx import stock

        raw = stock.get_nearest_business_day_in_a_week(candidate.strftime("%Y%m%d"), prev=prev)
        if raw:
            return datetime.strptime(str(raw), "%Y%m%d").date()
    except Exception as exc:
        logger.warning("[KR_CALENDAR][FALLBACK_WEEKDAY_ONLY] reason=KRX_CALENDAR_UNAVAILABLE detail=%s", str(exc)[:160])
    return None


def resolve_kr_trade_date(now_kst: datetime | None = None) -> date:
    now = (now_kst or datetime.now(KST)).astimezone(KST)
    d = None
    source = "LOCAL_CALENDAR"
    if KR_CALENDAR_PYKRX_ENABLED:
        d = _krx_nearest_business_day(now.date(), prev=False)
        source = "KRX_CALENDAR"
    if d is None:
        if KR_CALENDAR_PYKRX_ENABLED:
            logger.warning("[KR_CALENDAR][FALLBACK_WEEKDAY_ONLY] reason=KRX_CALENDAR_UNAVAILABLE")
        d = _weekday_trade_date(now.date())
        source = "WEEKDAY_FALLBACK" if KR_CALENDAR_PYKRX_ENABLED else "LOCAL_WEEKDAY"
    logger.info("[KR_CALENDAR][TRADE_DATE] trade_date=%s source=%s", d.isoformat(), source)
    return d


def resolve_kr_expected_as_of(trade_date: date) -> date:
    """Use the same holiday-aware KRX as-of contract as PREP/PM verification.

    In particular, 2026-10-12 must resolve to 2026-10-08, not the
    Friday 2026-10-09 exchange holiday.  No weekday-only silent fallback.
    """
    from trader.time_utils import resolve_prev_krx_trading_day

    resolved = resolve_prev_krx_trading_day(trade_date)
    logger.info(
        "[KR_CALENDAR][EXPECTED_ASOF] trade_date=%s expected_as_of=%s source=KRX_CANONICAL",
        trade_date.isoformat(),
        resolved.isoformat(),
    )
    return resolved
