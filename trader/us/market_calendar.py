# -*- coding: utf-8 -*-
"""미국장 Market Calendar.

- America/New_York timezone 사용 (서머타임 자동 반영)
- 미국 정규장 판단
- premarket/regular/aftermarket/closed phase 구분
- 휴장일 처리
- KST/KRX 기준 함수 재사용 금지
"""
from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

NY_TZ = ZoneInfo("America/New_York")

# 정규장 시간 (ET 기준)
_REGULAR_OPEN = time(9, 30)
_REGULAR_CLOSE = time(16, 0)
_PREMARKET_START = time(4, 0)
_AFTERMARKET_END = time(20, 0)

# 미국장 완전 휴장일 / 조기 폐장일 (config/us_market_holidays.yaml 로드로 보완)
_FULL_CLOSE_DATES_2026: set[date] = {
    date(2026, 1, 1),   # New Year's Day
    date(2026, 1, 19),  # MLK Day
    date(2026, 2, 16),  # Presidents' Day
    date(2026, 4, 3),   # Good Friday
    date(2026, 5, 25),  # Memorial Day
    date(2026, 6, 19),  # Juneteenth
    date(2026, 7, 3),   # Independence Day (observed)
    date(2026, 9, 7),   # Labor Day
    date(2026, 11, 26), # Thanksgiving Day
    date(2026, 12, 25), # Christmas Day
}
_EARLY_CLOSE_DATES_2026: set[date] = {
    date(2026, 11, 27), # Day after Thanksgiving
    date(2026, 12, 24), # Christmas Eve
}

_RUNTIME_HOLIDAYS: set[date] = set(_FULL_CLOSE_DATES_2026)
_RUNTIME_EARLY_CLOSES: set[date] = set(_EARLY_CLOSE_DATES_2026)
_US_CALENDAR_SUPPORTED_YEARS: set[int] = {2026}
_US_CALENDAR_LOAD_ERROR: str | None = None


def _load_yaml_holidays() -> None:
    """Load the configured calendar and expose failures to scheduler gates."""
    global _US_CALENDAR_LOAD_ERROR
    from pathlib import Path
    yaml_path = Path(__file__).resolve().parents[2] / "config" / "us_market_holidays.yaml"
    if not yaml_path.exists():
        _US_CALENDAR_LOAD_ERROR = "US_CALENDAR_FILE_MISSING"
        return
    try:
        import yaml  # type: ignore
        data = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
        values = list(data.get("full_close") or []) + list(data.get("early_close") or [])
        if not values:
            raise ValueError("US calendar contains no dated entries")
        for value in data.get("full_close") or []:
            parsed = date.fromisoformat(str(value)); _RUNTIME_HOLIDAYS.add(parsed); _US_CALENDAR_SUPPORTED_YEARS.add(parsed.year)
        for value in data.get("early_close") or []:
            parsed = date.fromisoformat(str(value)); _RUNTIME_EARLY_CLOSES.add(parsed); _US_CALENDAR_SUPPORTED_YEARS.add(parsed.year)
        _US_CALENDAR_LOAD_ERROR = None
    except Exception as exc:
        _US_CALENDAR_LOAD_ERROR = f"US_CALENDAR_LOAD_FAILED:{type(exc).__name__}"


_load_yaml_holidays()


def us_calendar_supported_years() -> set[int]:
    return set(_US_CALENDAR_SUPPORTED_YEARS)


def us_calendar_load_error() -> str | None:
    return _US_CALENDAR_LOAD_ERROR


def now_ny() -> datetime:
    """현재 NY 시각 반환."""
    return datetime.now(tz=NY_TZ)


def _resolve_now(now: datetime | None) -> datetime:
    if now is None:
        return now_ny()
    if now.tzinfo is None:
        return now.replace(tzinfo=NY_TZ)
    return now.astimezone(NY_TZ)


def is_us_market_holiday(d: date | None = None) -> bool:
    """해당 날짜가 미국 휴장일인지 확인."""
    if d is None:
        d = now_ny().date()
    return d in _RUNTIME_HOLIDAYS


def is_us_early_close_day(d: date | None = None) -> bool:
    if d is None:
        d = now_ny().date()
    return d in _RUNTIME_EARLY_CLOSES


def regular_close_time_for_date(d: date | None = None) -> time:
    if d is None:
        d = now_ny().date()
    return time(13, 0) if is_us_early_close_day(d) else _REGULAR_CLOSE


def is_us_weekend(d: date | None = None) -> bool:
    """토/일이면 True."""
    if d is None:
        d = now_ny().date()
    return d.weekday() >= 5


def is_us_trading_day(d: date | None = None) -> bool:
    """거래일이면 True (주말/휴장일 제외)."""
    if d is None:
        d = now_ny().date()
    return not is_us_weekend(d) and not is_us_market_holiday(d)


def is_us_regular_market_open(now: datetime | None = None) -> bool:
    """현재 정규장 시간인지 확인 (ET 09:30~16:00, 거래일 한정)."""
    now = _resolve_now(now)
    d = now.date()
    if not is_us_trading_day(d):
        return False
    t = now.time()
    return _REGULAR_OPEN <= t < regular_close_time_for_date(d)


def is_us_premarket_window(now: datetime | None = None) -> bool:
    """프리마켓 시간 (ET 04:00~09:30)."""
    now = _resolve_now(now)
    d = now.date()
    if not is_us_trading_day(d):
        return False
    t = now.time()
    return _PREMARKET_START <= t < _REGULAR_OPEN


def is_us_aftermarket_window(now: datetime | None = None) -> bool:
    """애프터마켓 시간 (ET 16:00~20:00)."""
    now = _resolve_now(now)
    d = now.date()
    if not is_us_trading_day(d):
        return False
    t = now.time()
    return regular_close_time_for_date(d) <= t < _AFTERMARKET_END


def resolve_us_trade_date(now: datetime | None = None) -> date:
    """오늘 거래일을 반환. 비거래일이면 직전 거래일 반환."""
    now = _resolve_now(now)
    d = now.date()
    # 미래로 무한 탐색 방지
    for _ in range(10):
        if is_us_trading_day(d):
            return d
        d -= timedelta(days=1)
    return d


def previous_completed_us_session(value: date | datetime | str | None = None) -> date:
    """Return the latest completed US trading session strictly before *value*."""
    if value is None:
        d = now_ny().date()
    elif isinstance(value, datetime):
        d = value.astimezone(NY_TZ).date() if value.tzinfo else value.date()
    elif isinstance(value, date):
        d = value
    else:
        raw = str(value or "").strip()
        if len(raw) == 8 and raw.isdigit():
            d = datetime.strptime(raw, "%Y%m%d").date()
        else:
            d = datetime.fromisoformat(raw[:10]).date()
    d -= timedelta(days=1)
    for _ in range(14):
        if is_us_trading_day(d):
            return d
        d -= timedelta(days=1)
    return d


def market_phase(now: datetime | None = None) -> str:
    """현재 미국장 phase.

    Returns:
        "CLOSED" | "PREMARKET" | "REGULAR_OPEN" | "REGULAR_MID" | "REGULAR_CLOSE" | "AFTERMARKET"
    """
    now = _resolve_now(now)
    d = now.date()

    if not is_us_trading_day(d):
        return "CLOSED"

    t = now.time()
    close_time = regular_close_time_for_date(d)

    if t < _PREMARKET_START:
        return "CLOSED"

    if t < _REGULAR_OPEN:
        return "PREMARKET"

    if t >= _AFTERMARKET_END:
        return "CLOSED"

    if t >= close_time:
        return "AFTERMARKET"

    if t < time(11, 30):
        return "REGULAR_OPEN"

    close_phase_start = time(12, 0) if close_time == time(13, 0) else time(15, 0)
    if t < close_phase_start:
        return "REGULAR_MID"

    return "REGULAR_CLOSE"


def register_holiday(d: date) -> None:
    """런타임에 휴장일 추가 (테스트/override 용)."""
    _RUNTIME_HOLIDAYS.add(d)


def unregister_holiday(d: date) -> None:
    """런타임 휴장일 제거 (테스트 cleanup 용)."""
    _RUNTIME_HOLIDAYS.discard(d)
