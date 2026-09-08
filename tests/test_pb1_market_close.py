from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from trader.kr.pb1.market_close import resolve_market_close
from trader.pb1_engine import PB1Engine


def test_resolve_market_close_prefers_explicit_market_close_time() -> None:
    now_kst = datetime(2026, 6, 5, 14, 11, tzinfo=ZoneInfo("Asia/Seoul"))
    close_dt, raw = resolve_market_close(
        now_kst=now_kst,
        market_close_time="15:29",
        close_auction_end="15:25",
    )

    assert close_dt.isoformat() == "2026-06-05T15:29:00+09:00"
    assert raw == "15:29"


def test_resolve_market_close_falls_back_to_auction_end() -> None:
    now_kst = datetime(2026, 6, 5, 14, 11, tzinfo=ZoneInfo("Asia/Seoul"))
    close_dt, raw = resolve_market_close(
        now_kst=now_kst,
        market_close_time="",
        close_auction_end="15:25",
    )

    assert close_dt.isoformat() == "2026-06-05T15:25:00+09:00"
    assert raw == "15:25"


def test_resolve_market_close_falls_back_to_default_on_invalid_value() -> None:
    now_kst = datetime(2026, 6, 5, 14, 11, tzinfo=ZoneInfo("Asia/Seoul"))
    close_dt, raw = resolve_market_close(
        now_kst=now_kst,
        market_close_time="bad-value",
        close_auction_end=None,
    )

    assert close_dt.isoformat() == "2026-06-05T15:30:00+09:00"
    assert raw == "15:30"


def test_pb1engine_resolve_market_close_matches_helper(monkeypatch) -> None:
    monkeypatch.setenv("MARKET_CLOSE_TIME", "15:29")
    now_kst = datetime(2026, 6, 5, 14, 11, tzinfo=ZoneInfo("Asia/Seoul"))
    engine = PB1Engine.__new__(PB1Engine)
    engine._now_kst = now_kst

    helper_result = resolve_market_close(
        now_kst=now_kst,
        market_close_time="15:29",
        close_auction_end=None,
    )
    wrapper_result = engine._resolve_market_close()

    assert helper_result == wrapper_result
