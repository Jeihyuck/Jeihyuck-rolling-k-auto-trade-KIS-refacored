"""
[CONTRACT] time_utils PyKRX 주말 조기 반환 계약.

- 토요일/일요일에 _resolve_pykrx_previous_or_same()을 호출하면
  PyKRX를 호출하지 않고 즉시 반환해야 한다.
- 반환값이 None이 아니어야 한다 (fallback weekday heuristic).
- 주말 에러 로그 [TIME][TRADING_DAY][PYKRX_FAIL]가 찍히지 않아야 한다.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import patch, MagicMock

import pytest


def test_saturday_does_not_call_pykrx() -> None:
    """토요일에 PyKRX get_nearest_business_day_in_a_week 호출이 없어야 한다."""
    saturday = date(2026, 4, 25)  # Saturday

    with patch(
        "trader.time_utils._safe_get_nearest_business_day_in_a_week",
        wraps=None,
    ) as mock_pykrx:
        mock_pykrx.return_value = (None, "NOT_CALLED")
        from trader import time_utils

        # Clear cache to force re-execution
        time_utils._PYKRX_PREV_OR_SAME_CACHE.pop(saturday.isoformat(), None)

        result = time_utils._resolve_pykrx_previous_or_same(saturday)

    mock_pykrx.assert_not_called(), (
        "PyKRX must NOT be called for Saturday input"
    )


def test_sunday_does_not_call_pykrx() -> None:
    """일요일에 PyKRX 호출이 없어야 한다."""
    sunday = date(2026, 4, 26)  # Sunday

    with patch(
        "trader.time_utils._safe_get_nearest_business_day_in_a_week",
        wraps=None,
    ) as mock_pykrx:
        mock_pykrx.return_value = (None, "NOT_CALLED")
        from trader import time_utils

        time_utils._PYKRX_PREV_OR_SAME_CACHE.pop(sunday.isoformat(), None)

        result = time_utils._resolve_pykrx_previous_or_same(sunday)

    mock_pykrx.assert_not_called(), (
        "PyKRX must NOT be called for Sunday input"
    )


def test_saturday_returns_non_none() -> None:
    """토요일 입력 시 None이 아닌 fallback 날짜를 반환해야 한다."""
    saturday = date(2026, 4, 25)
    from trader import time_utils

    time_utils._PYKRX_PREV_OR_SAME_CACHE.pop(saturday.isoformat(), None)

    result = time_utils._resolve_pykrx_previous_or_same(saturday)
    assert result is not None, "Saturday must return a non-None fallback date"
    assert isinstance(result, date), "Result must be a date object"
    assert result.weekday() < 5, "Fallback must be a weekday (Mon-Fri)"


def test_weekday_still_calls_pykrx() -> None:
    """평일에는 PyKRX 호출을 시도해야 한다."""
    monday = date(2026, 4, 28)  # Monday

    pykrx_called = []

    def fake_pykrx(date_str, *, prev=True):
        pykrx_called.append(date_str)
        return "20260425", None  # return a valid prior trading day

    from trader import time_utils

    time_utils._PYKRX_PREV_OR_SAME_CACHE.pop(monday.isoformat(), None)

    with patch.object(time_utils, "_safe_get_nearest_business_day_in_a_week", side_effect=fake_pykrx):
        time_utils._resolve_pykrx_previous_or_same(monday)

    assert len(pykrx_called) > 0, "PyKRX must be called for weekday input"


def test_weekend_early_return_logs_weekend_message(caplog) -> None:
    """토요일 입력 시 [TIME][TRADING_DAY][WEEKEND] 로그가 찍혀야 한다."""
    import logging
    saturday = date(2026, 4, 25)
    from trader import time_utils

    time_utils._PYKRX_PREV_OR_SAME_CACHE.pop(saturday.isoformat(), None)

    with caplog.at_level(logging.INFO, logger="trader.time_utils"):
        time_utils._resolve_pykrx_previous_or_same(saturday)

    assert any("[TIME][TRADING_DAY][WEEKEND]" in r.message for r in caplog.records), (
        "Weekend early-return must log [TIME][TRADING_DAY][WEEKEND]"
    )
