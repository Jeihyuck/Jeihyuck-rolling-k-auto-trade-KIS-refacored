"""
tests/test_non_trading_day_guard.py

주말/비거래일에 매수 주문이 발생하지 않아야 함을 검증.
- is_trading_weekday: 토/일 → False
- pb1_runner non-trading day 분기: OK_NO_TRADE 반환, orders=[]
"""
from __future__ import annotations

from datetime import datetime

import pytest

from trader.time_utils import is_trading_weekday


# ── is_trading_weekday 단위 테스트 ────────────────────────────────────────────

@pytest.mark.parametrize("weekday_offset,expected", [
    (0, True),   # 월요일
    (1, True),   # 화요일
    (2, True),   # 수요일
    (3, True),   # 목요일
    (4, True),   # 금요일
    (5, False),  # 토요일
    (6, False),  # 일요일
])
def test_is_trading_weekday(weekday_offset: int, expected: bool):
    """월~금은 True, 토~일은 False."""
    # 2025-01-06(월)부터 weekday_offset 적용
    from datetime import timedelta
    base_monday = datetime(2025, 1, 6, 9, 0, 0)
    ts = base_monday + timedelta(days=weekday_offset)
    assert is_trading_weekday(ts) == expected


def test_saturday_not_trading():
    sat = datetime(2025, 1, 11, 9, 0, 0)  # 토요일
    assert not is_trading_weekday(sat)


def test_sunday_not_trading():
    sun = datetime(2025, 1, 12, 9, 0, 0)  # 일요일
    assert not is_trading_weekday(sun)


def test_friday_is_trading():
    fri = datetime(2025, 1, 10, 9, 0, 0)  # 금요일
    assert is_trading_weekday(fri)


# ── non-trading day guard 로그 패턴 검증 ──────────────────────────────────────

def test_non_trading_day_guard_log_patterns():
    """
    non-trading day guard가 내보내는 로그 패턴이
    OK_NO_TRADE 경로임을 문자열 수준에서 확인.
    pb1_runner.py 소스에서 직접 패턴 검색.
    """
    import re
    from pathlib import Path

    runner_src = (
        Path(__file__).parent.parent / "trader" / "pb1_runner.py"
    ).read_text()

    assert re.search(r"TRADING_DAY.*CHECK.*is_trading_day=0", runner_src), (
        "[TRADE][TRADING_DAY][CHECK] log must reference is_trading_day=0"
    )
    assert re.search(r"TRADING_DAY.*SKIP.*non_trading_day", runner_src), (
        "[TRADE][TRADING_DAY][SKIP] log must reference non_trading_day"
    )
    assert re.search(r"RUN_SUMMARY.*RESULT.*status=OK_NO_TRADE", runner_src), (
        "[RUN_SUMMARY][RESULT] status=OK_NO_TRADE must be emitted"
    )
    assert re.search(r"PB1.*EXIT.*reason=non_trading_day", runner_src), (
        "[PB1][EXIT] reason=non_trading_day must be emitted"
    )
    assert re.search(r'return \[\], False, \{\}.*"OK_NO_TRADE"', runner_src), (
        "non-trading day path must return OK_NO_TRADE"
    )


def test_non_trading_day_guard_is_before_smoke_execution():
    """guard가 smoke 실행보다 앞에 있는지 코드 순서로 확인."""
    import re
    from pathlib import Path

    runner_src = (
        Path(__file__).parent.parent / "trader" / "pb1_runner.py"
    ).read_text()

    guard_pos = runner_src.find("OK_NO_TRADE")
    smoke_pos = runner_src.find("_run_smoke(")

    assert guard_pos != -1, "OK_NO_TRADE not found in pb1_runner.py"
    assert smoke_pos != -1, "_run_smoke( not found in pb1_runner.py"
    assert guard_pos < smoke_pos, (
        "OK_NO_TRADE guard must appear before _run_smoke() call"
    )
