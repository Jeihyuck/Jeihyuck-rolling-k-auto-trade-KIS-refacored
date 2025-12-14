"""거래일/거래 가능 시간 헬퍼."""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone

KST = timezone(timedelta(hours=9))
MARKET_OPEN = time(9, 0)
MARKET_CLOSE = time(15, 20)


def now_kst() -> datetime:
    """현재 KST 시각을 반환."""

    return datetime.now(tz=KST)


def is_trading_day(ts: datetime | None = None) -> bool:
    """주말을 제외한 기본 거래일 여부를 판정."""

    ts = ts or now_kst()
    return ts.weekday() < 5


def is_trading_window(ts: datetime | None = None) -> bool:
    """당일 장중(09:00~15:20) 여부."""

    ts = ts or now_kst()
    return is_trading_day(ts) and MARKET_OPEN <= ts.time() <= MARKET_CLOSE
