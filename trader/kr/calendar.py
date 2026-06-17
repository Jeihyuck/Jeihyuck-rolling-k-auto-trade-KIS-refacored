# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
logger = logging.getLogger(__name__)


def now_kst() -> datetime:
    return datetime.now(KST)


def resolve_kr_trade_date(now_kst: datetime | None = None) -> date:
    now = (now_kst or datetime.now(KST)).astimezone(KST)
    d = now.date()
    # Match existing local scheduler policy: weekends roll forward to next Monday.
    while d.weekday() >= 5:
        d += timedelta(days=1)
    logger.info("[KR_CALENDAR][TRADE_DATE] trade_date=%s source=KST/KRX", d.isoformat())
    return d


def resolve_kr_expected_as_of(trade_date: date) -> date:
    d = trade_date - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    logger.info(
        "[KR_CALENDAR][EXPECTED_ASOF] trade_date=%s expected_as_of=%s source=KRX_PREV_TRADING_DAY",
        trade_date.isoformat(),
        d.isoformat(),
    )
    return d
