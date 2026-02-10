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


def week_monday(d: date | datetime | str) -> date:
    """
    주어진 날짜가 속한 주의 월요일을 반환.
    
    Args:
        d: KST 기준 날짜 (date/datetime/"YYYY-MM-DD" 형식)
    
    Returns:
        해당 주의 월요일 (date 객체)
    
    Examples:
        >>> week_monday(date(2026, 1, 30))  # 목요일
        date(2026, 1, 27)  # 월요일
        >>> week_monday("2026-01-30")  # 문자열도 지원
        date(2026, 1, 27)
    """
    if d is None:
        raise ValueError("week_monday: d is None")

    # normalize string -> date
    if isinstance(d, str):
        s = d.strip()
        # allow full ISO datetime too
        if "T" in s:
            d = datetime.fromisoformat(s.replace("Z", "+00:00")).date()
        else:
            d = date.fromisoformat(s)

    # normalize datetime -> date
    if isinstance(d, datetime):
        d = d.date()

    if not isinstance(d, date):
        raise TypeError(f"week_monday: unsupported type {type(d)} value={d}")

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


def is_market_open_kst(dt: datetime | None = None) -> bool:
    """
    장중 여부 판단 (AUTO 모드 결정용).
    
    Returns:
        True: 장중 (09:00~15:20, 월~금)
        False: 장외 (주말, 장시작 전, 장마감 후)
    """
    dt = dt or now_kst()
    
    # 거래일 여부 확인
    if not is_trading_weekday(dt):
        return False
    
    # 장중 시간 확인 (09:00~15:20)
    t = dt.time()
    return MARKET_OPEN <= t <= MARKET_CLOSE


def market_close_dt_kst(dt: datetime) -> datetime:
    """
    주어진 날짜의 장 마감 시각(15:15) 반환.
    
    Args:
        dt: KST 기준 datetime
    
    Returns:
        같은 날 15:15:00 KST
    """
    return dt.replace(hour=15, minute=15, second=0, microsecond=0)


def prev_business_day(d: date) -> date:
    """
    주어진 날짜의 이전 영업일(월~금) 반환.
    
    Args:
        d: 기준 날짜
    
    Returns:
        이전 영업일 (date 객체)
    
    Examples:
        >>> prev_business_day(date(2026, 2, 3))  # 화요일
        date(2026, 2, 2)  # 월요일
        >>> prev_business_day(date(2026, 2, 1))  # 일요일
        date(2026, 1, 31)  # 금요일
        >>> prev_business_day(date(2026, 2, 2))  # 월요일
        date(2026, 1, 31)  # 금요일
    """
    prev = d - timedelta(days=1)
    
    # 주말이면 금요일까지 거슬러 올라감
    while prev.weekday() >= 5:  # 토(5), 일(6)
        prev -= timedelta(days=1)
    
    return prev


def resolve_derived_as_of(now: datetime | None = None) -> date:
    """
    Trade에서 사용할 derived 데이터의 as_of 날짜를 결정.
    
    장중 매매는 항상 "전일 종가 기반 derived"를 사용해야 하므로,
    현재 시각과 무관하게 전일 영업일을 반환한다.
    
    Args:
        now: 현재 시각 (없으면 now_kst() 사용)
    
    Returns:
        전일 영업일 (date 객체)
    
    Examples:
        >>> # 2026-02-10 (화) 장중 -> 2026-02-09 (월) derived 사용
        >>> resolve_derived_as_of(datetime(2026, 2, 10, 10, 0, tzinfo=KST))
        date(2026, 2, 9)
        
        >>> # 2026-02-10 (화) 새벽 -> 2026-02-09 (월) derived 사용
        >>> resolve_derived_as_of(datetime(2026, 2, 10, 3, 0, tzinfo=KST))
        date(2026, 2, 9)
    
    Rationale:
        - prep_runner는 전일 종가 기반으로 derived를 생성 (PREV_TRADING_DAY)
        - trade는 장중에 "오늘 종가"가 없으므로 전일 derived를 사용해야 함
        - 일관성: 장중/장외 무관하게 전일 영업일 사용
    """
    now = now or now_kst()
    today = now.date()
    
    # 전일 영업일 계산
    derived_as_of = prev_business_day(today)
    
    logger.debug(
        "[ASOF][RESOLVE] now=%s today=%s derived_as_of=%s reason=INTRADAY_USE_PREV_CLOSE",
        now.isoformat(),
        today.isoformat(),
        derived_as_of.isoformat(),
    )
    
    return derived_as_of

