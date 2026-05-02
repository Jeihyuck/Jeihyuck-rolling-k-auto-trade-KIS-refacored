# -*- coding: utf-8 -*-
"""tests/us/test_us_market_calendar.py

US Market Calendar 단위 테스트.
"""
from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from trader.us.market_calendar import (
    NY_TZ,
    is_us_regular_market_open,
    is_us_premarket_window,
    is_us_aftermarket_window,
    is_us_trading_day,
    is_us_market_holiday,
    is_us_weekend,
    resolve_us_trade_date,
    market_phase,
    register_holiday,
    unregister_holiday,
    now_ny,
)


def ny(dt_str: str) -> datetime:
    """'YYYY-MM-DD HH:MM' 형식 → NY datetime."""
    return datetime.fromisoformat(dt_str).replace(tzinfo=NY_TZ)


class TestNowNy:
    def test_has_ny_tz(self):
        now = now_ny()
        assert now.tzinfo is not None


class TestIsTradingDay:
    def test_weekday_is_trading(self):
        # 2026-04-30 목요일
        assert is_us_trading_day(date(2026, 4, 30)) is True

    def test_saturday_not_trading(self):
        assert is_us_trading_day(date(2026, 5, 2)) is False

    def test_sunday_not_trading(self):
        assert is_us_trading_day(date(2026, 5, 3)) is False

    def test_holiday_not_trading(self):
        # 2026-01-01 New Year's Day
        assert is_us_trading_day(date(2026, 1, 1)) is False


class TestRegularMarketOpen:
    def test_open_during_regular(self):
        # 10:00 ET (regular hours)
        assert is_us_regular_market_open(ny("2026-04-30 10:00")) is True

    def test_closed_before_open(self):
        # 09:00 ET (before open)
        assert is_us_regular_market_open(ny("2026-04-30 09:00")) is False

    def test_closed_after_close(self):
        # 16:30 ET (after close)
        assert is_us_regular_market_open(ny("2026-04-30 16:30")) is False

    def test_closed_on_weekend(self):
        assert is_us_regular_market_open(ny("2026-05-02 10:00")) is False

    def test_closed_on_holiday(self):
        assert is_us_regular_market_open(ny("2026-01-01 10:00")) is False

    def test_open_at_930(self):
        assert is_us_regular_market_open(ny("2026-04-30 09:30")) is True

    def test_closed_at_1600(self):
        assert is_us_regular_market_open(ny("2026-04-30 16:00")) is False


class TestPremarket:
    def test_premarket_window(self):
        assert is_us_premarket_window(ny("2026-04-30 07:00")) is True

    def test_not_premarket_after_open(self):
        assert is_us_premarket_window(ny("2026-04-30 10:00")) is False

    def test_not_premarket_before_4am(self):
        assert is_us_premarket_window(ny("2026-04-30 03:00")) is False


class TestAftermarket:
    def test_aftermarket_window(self):
        assert is_us_aftermarket_window(ny("2026-04-30 17:00")) is True

    def test_not_aftermarket_during_regular(self):
        assert is_us_aftermarket_window(ny("2026-04-30 10:00")) is False

    def test_not_aftermarket_after_8pm(self):
        assert is_us_aftermarket_window(ny("2026-04-30 20:30")) is False


class TestResolveTradeDate:
    def test_trading_day_returns_same(self):
        dt = ny("2026-04-30 09:00")
        assert resolve_us_trade_date(dt) == date(2026, 4, 30)

    def test_weekend_returns_friday(self):
        # 2026-05-02 토요일 → 직전 금요일 = 2026-05-01
        dt = ny("2026-05-02 10:00")
        result = resolve_us_trade_date(dt)
        assert result == date(2026, 5, 1)


class TestMarketPhase:
    def test_closed_night(self):
        assert market_phase(ny("2026-04-30 02:00")) == "CLOSED"

    def test_premarket(self):
        assert market_phase(ny("2026-04-30 06:00")) == "PREMARKET"

    def test_regular_open(self):
        assert market_phase(ny("2026-04-30 09:45")) == "REGULAR_OPEN"

    def test_regular_mid(self):
        assert market_phase(ny("2026-04-30 13:00")) == "REGULAR_MID"

    def test_regular_close(self):
        assert market_phase(ny("2026-04-30 15:30")) == "REGULAR_CLOSE"

    def test_aftermarket(self):
        assert market_phase(ny("2026-04-30 17:00")) == "AFTERMARKET"

    def test_closed_weekend(self):
        assert market_phase(ny("2026-05-02 10:00")) == "CLOSED"


class TestRegisterHoliday:
    def test_register_and_unregister(self):
        d = date(2027, 7, 4)
        register_holiday(d)
        assert is_us_market_holiday(d) is True
        unregister_holiday(d)
        assert is_us_market_holiday(d) is False
