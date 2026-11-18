"""Verified KR PB1 native minute policy replay, no KIS orders or DB mutation."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from trader.kr.opening_bar_contract import (
    evaluate_price_aware_entry, inspect_completed_bar_coverage,
)

KST = ZoneInfo("Asia/Seoul")
DAY = "20261118"
BASE = datetime(2026, 11, 18, 9, 0, tzinfo=KST)
NOW = datetime(2026, 11, 18, 9, 38, 20, tzinfo=KST)


def native_bar(minute, *, high, low, close, volume=1000):
    instant = BASE + timedelta(minutes=minute)
    return {
        "date": DAY, "time": instant.strftime("%H%M%S"),
        "open": close, "high": high, "low": low, "close": close,
        "volume": volume,
    }


def sparse_opening_and_native_rebound():
    opening = [native_bar(i, high=101, low=99, close=100)
               for i in range(16)]
    previous = [
        (110, 108, 109), (109, 106, 108), (107, 104, 106),
        (106, 102, 104), (104, 100, 102),
    ]
    recent = [
        (102, 100, 101.5), (103, 101, 102.5), (104, 102, 103.5),
    ]
    post = [
        native_bar(30 + i, high=h, low=l, close=c,
                   volume=1200 if i >= 5 else 1000)
        for i, (h, l, c) in enumerate(previous + recent)
    ]
    return opening + post


def signal(rows, now=NOW, **kwargs):
    return evaluate_price_aware_entry(
        style=kwargs.pop("style", "ENTRY_PULLBACK"),
        rows=rows, now=now, vwap=kwargs.pop("vwap", None),
        vwap_verified=kwargs.pop("vwap_verified", False),
        setup_ok=kwargs.pop("setup_ok", True),
        trigger_ok=kwargs.pop("trigger_ok", False),
        pivot=kwargs.pop("pivot", None),
        order_price=kwargs.pop("order_price", 103.5),
    )


def test_observer_can_report_partial_opening_while_pb1_verified_intraday_signal_is_ready():
    rows = sparse_opening_and_native_rebound()
    health = inspect_completed_bar_coverage(rows=rows, now=NOW)
    assert health["state"] == "UNKNOWN"
    assert health["opening_count"] == 16
    assert health["native_post_open_window_ready"] is True
    verdict = signal(rows)
    assert verdict["state"] == "READY_SHADOW"
    assert verdict["opening_range_complete"] is False
    assert verdict["reference_source"] == "KIS_NATIVE_POST_OPENING_5_PLUS_3"
    assert "opening_high" not in verdict
    assert verdict["max_entry_price"] >= 103.5


def test_sparse_native_window_can_use_zero_opening_bars_without_synthetic_open():
    rows = sparse_opening_and_native_rebound()[16:]
    verdict = signal(rows)
    assert verdict["state"] == "READY_SHADOW"
    assert verdict["opening_count"] == 0


def test_no_entry_before_0930_even_with_apparent_rebound():
    rows = sparse_opening_and_native_rebound()
    decision = signal(rows, now=datetime(2026, 11, 18, 9, 25, tzinfo=KST))
    assert decision["state"] == "WAIT"
    assert decision["reason"] == "OPENING_OBSERVATION"


def test_missing_a_recent_native_minute_cannot_be_interpolated_or_approved():
    rows = [row for row in sparse_opening_and_native_rebound()
            if row["time"] != "093400"]
    decision = signal(rows)
    assert decision["state"] == "UNKNOWN"
    assert decision["reason"] == "POST_OPENING_NATIVE_WINDOW_GAP"


def test_stale_postopening_bars_fail_even_if_previous_signal_was_ready():
    decision = signal(
        sparse_opening_and_native_rebound(),
        now=datetime(2026, 11, 18, 9, 42, tzinfo=KST),
    )
    assert decision["state"] == "UNKNOWN"
    assert decision["reason"] == "COMPLETED_BAR_STALE"


def test_incomplete_native_window_cannot_approve_early_subsequent_entry():
    rows = sparse_opening_and_native_rebound()[:-3]
    decision = signal(rows, now=datetime(2026, 11, 18, 9, 35, tzinfo=KST))
    assert decision["state"] != "READY_SHADOW"
    assert decision["reason"] == "POST_OPENING_NATIVE_WINDOW_PENDING"


def test_real_price_move_beyond_limit_protects_rebound_entry():
    decision = signal(sparse_opening_and_native_rebound(), order_price=105)
    assert decision["state"] == "WAIT"
    assert decision["reason"] == "ORDER_PRICE_MOVED_FROM_CONFIRMED_BAR"


def test_no_vwap_never_authorizes_sparse_momentum():
    decision = signal(sparse_opening_and_native_rebound(),
                      style="ENTRY_MOMENTUM")
    assert decision["state"] == "UNKNOWN"
    assert decision["reason"] == "NO_VERIFIED_INTRADAY_VWAP"


def test_breakout_without_owner_pivot_never_becomes_native_buy():
    decision = signal(sparse_opening_and_native_rebound(),
                      style="ENTRY_BREAKOUT", trigger_ok=False, pivot=102)
    assert decision["state"] == "WAIT"
    assert decision["reason"] == "ORIGINAL_PIVOT_TRIGGER_UNCONFIRMED"


def test_policy_will_not_use_fake_previous_date_native_minutes():
    rows = sparse_opening_and_native_rebound()
    for row in rows:
        row["date"] = "20261117"
    decision = signal(rows)
    assert decision["state"] != "READY_SHADOW"
