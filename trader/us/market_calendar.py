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

# 미국 연방 공휴일 (고정 날짜 기반, 실제 관측일은 _OBSERVED_HOLIDAYS에서 처리)
# config/us_market_holidays.yaml 로드로 보완
_STATIC_MARKET_HOLIDAYS_2026: set[date] = {
    date(2026, 1, 1),   # New Year's Day
    date(2026, 1, 19),  # MLK Day
    date(2026, 2, 16),  # Presidents' Day
    date(2026, 4, 3),   # Good Friday
    date(2026, 5, 25),  # Memorial Day
    date(2026, 6, 19),  # Juneteenth
    date(2026, 7, 3),   # Independence Day (observed)
    date(2026, 9, 7),   # Labor Day
    date(2026, 11, 26), # Thanksgiving Day
    date(2026, 11, 27), # Day after Thanksgiving (early close, not full holiday)
    date(2026, 12, 24), # Christmas Eve (early close)
    date(2026, 12, 25), # Christmas Day
}

_RUNTIME_HOLIDAYS: set[date] = set(_STATIC_MARKET_HOLIDAYS_2026)


def _load_yaml_holidays() -> None:
    """config/us_market_holidays.yaml이 있으면 로드."""
    from pathlib import Path
    yaml_path = Path(__file__).resolve().parents[2] / "config" / "us_market_holidays.yaml"
    if not yaml_path.exists():
        return
    try:
        import yaml  # type: ignore
        with open(yaml_path, "r") as f:
            data = yaml.safe_load(f) or {}
        for d in (data.get("full_close") or []):
            _RUNTIME_HOLIDAYS.add(date.fromisoformat(str(d)))
    except Exception:
        pass


try:
    _load_yaml_holidays()
except Exception:
    pass


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
    return _REGULAR_OPEN <= t < _REGULAR_CLOSE


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
    return _REGULAR_CLOSE <= t < _AFTERMARKET_END


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

    if t < _PREMARKET_START or t >= _AFTERMARKET_END:
        return "CLOSED"

    if t < _REGULAR_OPEN:
        return "PREMARKET"

    if t < time(11, 30):
        return "REGULAR_OPEN"

    if t < time(15, 0):
        return "REGULAR_MID"

    if t < _REGULAR_CLOSE:
        return "REGULAR_CLOSE"

    return "AFTERMARKET"


def register_holiday(d: date) -> None:
    """런타임에 휴장일 추가 (테스트/override 용)."""
    _RUNTIME_HOLIDAYS.add(d)


def unregister_holiday(d: date) -> None:
    """런타임 휴장일 제거 (테스트 cleanup 용)."""
    _RUNTIME_HOLIDAYS.discard(d)
