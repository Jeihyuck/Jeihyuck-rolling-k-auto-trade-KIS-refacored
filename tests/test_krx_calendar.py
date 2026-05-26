"""KRX 한국장 휴장일 캘린더 관련 테스트.

검증 항목:
1. 2026-05-25 (부처님오신날)은 한국장 거래일이 아니다
2. 2026-05-26의 직전 거래일은 2026-05-22다 (2026-05-23 토, 2026-05-24 일, 2026-05-25 휴장)
3. 2026-05-22 (금요일)는 거래일이다
4. 주말(토/일)은 거래일이 아니다
5. config/krx_holidays.json 에 명시된 날짜는 거래일이 아니다
"""
from __future__ import annotations

import importlib
import os
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

# 모듈을 직접 임포트하기 전에 _KRX_HOLIDAYS_CACHE를 리셋한다
import trader.time_utils as time_utils


def _reset_cache():
    """테스트 간 캐시 초기화."""
    time_utils._KRX_HOLIDAYS_CACHE = None


@pytest.fixture(autouse=True)
def reset_krx_cache():
    _reset_cache()
    yield
    _reset_cache()


# ---------------------------------------------------------------------------
# 기본 휴장일 판정
# ---------------------------------------------------------------------------

class TestKrxHoliday2026:
    def test_2026_05_25_is_holiday(self):
        """2026-05-25 (부처님오신날)은 한국장 거래일이 아니다."""
        assert time_utils.is_krx_trading_day(date(2026, 5, 25)) is False

    def test_2026_05_25_string_is_holiday(self):
        """문자열 입력도 동일하게 동작한다."""
        assert time_utils.is_krx_trading_day("2026-05-25") is False

    def test_2026_05_26_is_trading_day(self):
        """2026-05-26 (화요일, 휴장 다음날)은 거래일이다."""
        assert time_utils.is_krx_trading_day(date(2026, 5, 26)) is True

    def test_2026_05_22_is_trading_day(self):
        """2026-05-22 (금요일)은 거래일이다."""
        assert time_utils.is_krx_trading_day(date(2026, 5, 22)) is True

    def test_2026_05_23_saturday_not_trading(self):
        """2026-05-23 (토요일)은 거래일이 아니다."""
        assert time_utils.is_krx_trading_day(date(2026, 5, 23)) is False

    def test_2026_05_24_sunday_not_trading(self):
        """2026-05-24 (일요일)은 거래일이 아니다."""
        assert time_utils.is_krx_trading_day(date(2026, 5, 24)) is False

    def test_2026_01_01_is_holiday(self):
        """2026-01-01 (신정)은 한국장 거래일이 아니다."""
        assert time_utils.is_krx_trading_day(date(2026, 1, 1)) is False

    def test_2026_12_31_is_holiday(self):
        """2026-12-31 (연말 휴장)은 한국장 거래일이 아니다."""
        assert time_utils.is_krx_trading_day(date(2026, 12, 31)) is False

    def test_2026_05_04_is_trading_day(self):
        """2026-05-04 (월요일, 평일)은 거래일이다."""
        assert time_utils.is_krx_trading_day(date(2026, 5, 4)) is True


# ---------------------------------------------------------------------------
# 직전 거래일 계산
# ---------------------------------------------------------------------------

class TestResolvePrevKrxTradingDay:
    def test_prev_trading_day_after_2026_05_25_holiday(self):
        """2026-05-26의 직전 KRX 거래일은 2026-05-22다."""
        result = time_utils.resolve_prev_krx_trading_day(date(2026, 5, 26))
        assert result == date(2026, 5, 22), (
            f"Expected 2026-05-22 but got {result}. "
            "2026-05-23 토, 2026-05-24 일, 2026-05-25 부처님오신날 모두 스킵해야 함"
        )

    def test_prev_trading_day_after_2026_05_25_string(self):
        """문자열 입력도 동일하게 동작한다."""
        result = time_utils.resolve_prev_krx_trading_day("2026-05-26")
        assert result == date(2026, 5, 22)

    def test_prev_trading_day_normal_tuesday(self):
        """일반 화요일의 직전 거래일은 월요일이다."""
        result = time_utils.resolve_prev_krx_trading_day(date(2026, 5, 19))  # 화요일
        assert result == date(2026, 5, 18)  # 월요일

    def test_prev_trading_day_monday_skips_weekend(self):
        """월요일의 직전 거래일은 금요일이다."""
        result = time_utils.resolve_prev_krx_trading_day(date(2026, 5, 18))  # 월요일
        assert result == date(2026, 5, 15)  # 금요일

    def test_prev_trading_day_skips_new_year(self):
        """2026-01-02의 직전 거래일은 2025-12-31이 아니라 2025-12-30이다 (2025-12-31 연말 휴장)."""
        result = time_utils.resolve_prev_krx_trading_day(date(2026, 1, 2))
        # 2026-01-01은 휴장, 2025-12-31도 KRX 연말 휴장
        # 그러므로 직전 거래일은 2025-12-30이어야 한다
        assert result == date(2025, 12, 30), (
            f"Expected 2025-12-30 but got {result}. "
            "2026-01-01 신정 + 2025-12-31 연말 휴장 모두 스킵해야 함"
        )


# ---------------------------------------------------------------------------
# resolve_prev_trading_day (기존 API)
# ---------------------------------------------------------------------------

class TestResolvePrevTradingDay:
    def test_uses_krx_calendar(self):
        """resolve_prev_trading_day()도 KRX 캘린더를 사용한다."""
        result = time_utils.resolve_prev_trading_day(date(2026, 5, 26))
        assert result == date(2026, 5, 22)

    def test_2026_05_25_is_not_trading_date(self):
        """is_trading_date()도 KRX 캘린더를 사용한다."""
        assert time_utils.is_trading_date(date(2026, 5, 25), exchange="KRX") is False

    def test_2026_05_26_is_trading_date(self):
        assert time_utils.is_trading_date(date(2026, 5, 26), exchange="KRX") is True


# ---------------------------------------------------------------------------
# pykrx 실패 시 config fallback 동작
# ---------------------------------------------------------------------------

class TestPykrxFailFallback:
    def test_holiday_detected_even_when_pykrx_fails(self):
        """pykrx가 실패해도 config fallback으로 휴장일을 올바르게 판정한다."""
        with patch.object(
            time_utils, "_safe_get_nearest_business_day_in_a_week",
            return_value=(None, "ConnectionError"),
        ):
            assert time_utils.is_krx_trading_day(date(2026, 5, 25)) is False

    def test_trading_day_detected_when_pykrx_fails(self):
        """pykrx가 실패해도 config에 없는 평일은 거래일로 판정한다 (fail-open이 아닌 config 기준)."""
        with patch.object(
            time_utils, "_safe_get_nearest_business_day_in_a_week",
            return_value=(None, "ConnectionError"),
        ):
            # 2026-05-26은 config에 없는 평일 → 거래일
            assert time_utils.is_krx_trading_day(date(2026, 5, 26)) is True

    def test_prev_trading_day_pykrx_fail(self):
        """pykrx 실패 시에도 2026-05-26의 직전 거래일은 2026-05-22다."""
        with patch.object(
            time_utils, "_safe_get_nearest_business_day_in_a_week",
            return_value=(None, "ConnectionError"),
        ):
            result = time_utils.resolve_prev_krx_trading_day(date(2026, 5, 26))
            assert result == date(2026, 5, 22)
