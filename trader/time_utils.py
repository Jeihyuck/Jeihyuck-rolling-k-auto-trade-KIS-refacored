"""거래일/거래 가능 시간 헬퍼."""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")
MARKET_OPEN = time(9, 0)
MARKET_CLOSE = time(15, 20)


def now_kst() -> datetime:
    """현재 KST 시각을 반환."""
    return datetime.now(tz=KST)


def week_monday(d: date) -> date:
    """
    주어진 날짜가 속한 주의 월요일을 반환.
    
    Args:
        d: KST 기준 날짜 (date 객체)
    
    Returns:
        해당 주의 월요일 (date 객체)
    
    Examples:
        >>> week_monday(date(2026, 1, 30))  # 목요일
        date(2026, 1, 27)  # 월요일
    """
    return d - timedelta(days=d.weekday())


def is_trading_weekday(ts: datetime) -> bool:
    # Mon=0 ... Sun=6
    return ts.weekday() < 5


def is_trading_day(ts: datetime | None = None) -> bool:
    """주말을 제외한 기본 거래일 여부를 판정.
    FORCE_TRADING_DAY=1 이면 강제로 True 반환 (테스트용)
    """

    ts = ts or now_kst()

    # 🔥 강제 거래일 테스트 모드
    if os.getenv("FORCE_TRADING_DAY") == "1":
        logger.warning(
            "[TIME_UTILS] FORCE_TRADING_DAY=1 → 비거래일 체크 우회 (%s)",
            ts.date(),
        )
        return True

    return is_trading_weekday(ts)


def is_trading_window(ts: datetime | None = None) -> bool:
    """당일 장중(09:00~15:20) 여부."""

    ts = ts or now_kst()

    # 거래일 여부도 동일하게 FORCE_TRADING_DAY 영향 받음
    if not is_trading_day(ts):
        return False

    return MARKET_OPEN <= ts.time() <= MARKET_CLOSE


def calc_market_window_kst(dt: datetime) -> str:
    """
    Returns one of: preopen, morning, day, close, after
    IMPORTANT: If not trading weekday => 'after' (weekend guard)
    """
    if not is_trading_weekday(dt):
        return "after"

    preopen_start = time.fromisoformat(os.getenv("PB1_PREOPEN_START", "08:45"))
    preopen_end = time.fromisoformat(os.getenv("PB1_PREOPEN_END", "09:00"))

    t = dt.time()
    if preopen_start <= t < preopen_end:
        return "preopen"
    if preopen_end <= t < time(10, 0):
        return "morning"
    if time(10, 0) <= t < time(15, 15):
        return "day"
    if time(15, 15) <= t <= time(15, 30):
        return "close"
    return "after"
