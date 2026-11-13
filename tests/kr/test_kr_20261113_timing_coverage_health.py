"""PR-B: 1m observer coverage is diagnostics, never an order authorization.

These are synthetic bars; a KIS original OHLCV+VWAP operational capture is
still required before declaring the live timing gate healthy.
"""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from trader.kr.opening_bar_contract import inspect_completed_bar_coverage

KST = ZoneInfo("Asia/Seoul")
NOW = datetime(2026, 11, 13, 9, 35, tzinfo=KST)


def bars(minutes=35, *, day="20261113"):
    base = datetime(2026, 11, 13, 9, 0, tzinfo=KST)
    result = []
    for i in range(minutes):
        stamp = base + timedelta(minutes=i)
        result.append({
            "date": day, "time": stamp.strftime("%H%M%S"),
            "open": 100, "high": 101, "low": 99, "close": 100,
            "volume": 500,
        })
    return result


def test_complete_native_bars_have_structural_coverage_not_ready_signal():
    decision = inspect_completed_bar_coverage(rows=bars(), now=NOW)
    assert decision["state"] == "COVERED"
    assert decision["reason"] == "STRUCTURAL_1M_BARS_AVAILABLE"
    assert decision["opening_count"] == 30
    assert decision["post_open_count"] == 5
    assert decision["latest_bar_age_sec"] == 60
    assert "READY" not in decision["state"]


def test_missing_exchange_day_does_not_create_native_coverage():
    rows = bars()
    for row in rows:
        del row["date"]
    decision = inspect_completed_bar_coverage(rows=rows, now=NOW)
    assert decision["state"] == "UNKNOWN"
    assert decision["opening_count"] == 0


def test_previous_day_bars_do_not_count_as_todays_opening():
    decision = inspect_completed_bar_coverage(rows=bars(day="20261112"), now=NOW)
    assert decision["state"] == "UNKNOWN"


def test_missing_post_opening_bars_are_not_reported_ready():
    decision = inspect_completed_bar_coverage(rows=bars(30), now=NOW)
    assert decision["state"] == "UNKNOWN"  # latest completed bar is stale
    assert decision["post_open_count"] == 0


def test_recent_minute_gap_is_reported_not_interpolated():
    rows = bars()
    rows.pop(-2)
    decision = inspect_completed_bar_coverage(rows=rows, now=NOW)
    assert decision["state"] == "UNKNOWN"
    assert decision["reason"] == "RECENT_MINUTE_GAP"


def test_future_incomplete_candle_is_excluded():
    rows = bars(36)  # 09:35 minute not completed at 09:35
    decision = inspect_completed_bar_coverage(rows=rows, now=NOW)
    assert decision["state"] == "COVERED"
    assert decision["latest_bar_ts"].endswith("09:34:00+09:00")
