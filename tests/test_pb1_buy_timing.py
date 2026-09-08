from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from trader.kr.pb1.buy_timing import is_buy_allowed_now
from trader.pb1_engine import PB1Engine


def test_is_buy_allowed_now_blocks_after_market_close() -> None:
    now = datetime(2026, 6, 5, 15, 31, tzinfo=ZoneInfo("Asia/Seoul"))
    entry_cutoff_dt = datetime(2026, 6, 5, 15, 15, tzinfo=ZoneInfo("Asia/Seoul"))
    market_close_dt = datetime(2026, 6, 5, 15, 30, tzinfo=ZoneInfo("Asia/Seoul"))

    result = is_buy_allowed_now(now=now, entry_cutoff_dt=entry_cutoff_dt, market_close_dt=market_close_dt)

    assert result[0] is False
    assert result[1] == "MARKET_CLOSED"


def test_is_buy_allowed_now_blocks_after_entry_cutoff() -> None:
    now = datetime(2026, 6, 5, 15, 16, tzinfo=ZoneInfo("Asia/Seoul"))
    entry_cutoff_dt = datetime(2026, 6, 5, 15, 15, tzinfo=ZoneInfo("Asia/Seoul"))
    market_close_dt = datetime(2026, 6, 5, 15, 30, tzinfo=ZoneInfo("Asia/Seoul"))

    result = is_buy_allowed_now(now=now, entry_cutoff_dt=entry_cutoff_dt, market_close_dt=market_close_dt)

    assert result[0] is False
    assert result[1] == "ENTRY_CUTOFF_PASSED"


def test_pb1engine_is_buy_allowed_now_matches_helper(monkeypatch) -> None:
    monkeypatch.setenv("ENTRY_CUTOFF_TIME", "15:15")
    monkeypatch.setenv("MARKET_CLOSE_TIME", "15:30")
    now = datetime(2026, 6, 5, 14, 11, tzinfo=ZoneInfo("Asia/Seoul"))
    engine = PB1Engine.__new__(PB1Engine)
    engine._now_kst = now

    helper_result = is_buy_allowed_now(
        now=now,
        entry_cutoff_dt=engine._resolve_entry_cutoff()[0],
        market_close_dt=engine._resolve_market_close()[0],
    )
    wrapper_result = engine._is_buy_allowed_now(now)

    assert helper_result == wrapper_result
