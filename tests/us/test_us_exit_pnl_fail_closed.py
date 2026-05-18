# -*- coding: utf-8 -*-
"""Tests for US exit PnL fail-closed behavior.

entry_price 없을 때 pnl_pct=0.0000으로 위장하지 않고,
fail-closed SELL intent를 생성하거나 None을 반환하는지 검증한다.
"""
import pytest
from trader.us.pb1.us_exit_engine import evaluate_exit


# ─────────────────────────────────────────────────────────────────────────────
# Test 1: avg_price_usd 기반 hard_stop
# ─────────────────────────────────────────────────────────────────────────────

def test_us_exit_hard_stop_uses_avg_price_usd():
    """avg_price_usd=100, current=89 → hard_stop (-11% < -7%)"""
    pos = {
        "symbol": "CRDO",
        "exchange": "NASDAQ",
        "qty": 10,
        "avg_price_usd": 100.0,
    }

    intent = evaluate_exit(pos, current_price=89.0)

    assert intent is not None
    assert intent["side"] == "SELL"
    assert intent["exit_type"] == "hard_stop"
    assert intent["unrealized_pnl_pct"] <= -0.07


# ─────────────────────────────────────────────────────────────────────────────
# Test 2: entry_price missing fail-closed (기본값 US_EXIT_FAIL_CLOSED_ON_PNL_MISSING=1)
# ─────────────────────────────────────────────────────────────────────────────

def test_us_exit_missing_entry_price_fail_closed(monkeypatch):
    """entry_price 없으면 기본값 fail-closed → SELL intent 생성"""
    monkeypatch.setenv("US_EXIT_FAIL_CLOSED_ON_PNL_MISSING", "1")

    pos = {
        "symbol": "AAPL",
        "exchange": "NASDAQ",
        "qty": 10,
    }

    intent = evaluate_exit(pos, current_price=89.0)

    assert intent is not None
    assert intent["side"] == "SELL"
    assert intent["exit_type"] == "pnl_missing_fail_closed"
    assert intent["reason"] == "PNL_MISSING_ENTRY_PRICE_FAIL_CLOSED"


# ─────────────────────────────────────────────────────────────────────────────
# Test 3: fail-closed off → intent None, pnl_pct=0.0000이 아님
# ─────────────────────────────────────────────────────────────────────────────

def test_us_exit_missing_entry_price_can_be_hold_when_fail_closed_off(monkeypatch):
    """US_EXIT_FAIL_CLOSED_ON_PNL_MISSING=0 → None 반환"""
    monkeypatch.setenv("US_EXIT_FAIL_CLOSED_ON_PNL_MISSING", "0")

    pos = {
        "symbol": "AAPL",
        "exchange": "NASDAQ",
        "qty": 10,
    }

    intent = evaluate_exit(pos, current_price=89.0)

    assert intent is None


# ─────────────────────────────────────────────────────────────────────────────
# Test 4: buy_amount_usd / qty 계산으로 hard_stop 작동
# ─────────────────────────────────────────────────────────────────────────────

def test_us_exit_hard_stop_uses_buy_amount_fallback():
    """buy_amount_usd=1000, qty=10 → entry_price=100, current=89 → hard_stop"""
    pos = {
        "symbol": "LITE",
        "exchange": "NASDAQ",
        "qty": 10,
        "buy_amount_usd": 1000.0,
    }

    intent = evaluate_exit(pos, current_price=89.0)

    assert intent is not None
    assert intent["side"] == "SELL"
    assert intent["exit_type"] == "hard_stop"


# ─────────────────────────────────────────────────────────────────────────────
# Test 5: qty=0 → None 반환
# ─────────────────────────────────────────────────────────────────────────────

def test_us_exit_qty_zero_returns_none():
    pos = {
        "symbol": "AAPL",
        "qty": 0,
        "entry_price": 100.0,
    }
    intent = evaluate_exit(pos, current_price=90.0)
    assert intent is None


# ─────────────────────────────────────────────────────────────────────────────
# Test 6: trailing stop 작동
# ─────────────────────────────────────────────────────────────────────────────

def test_us_exit_trailing_stop():
    """max_price=120, current=112 (trailing 6.7% > 5%) → trailing_stop"""
    pos = {
        "symbol": "NVDA",
        "qty": 5,
        "entry_price": 100.0,
        "max_price": 120.0,
    }

    intent = evaluate_exit(pos, current_price=112.0)

    assert intent is not None
    assert intent["exit_type"] == "trailing_stop"
    assert intent["side"] == "SELL"


# ─────────────────────────────────────────────────────────────────────────────
# Test 7: max_price 없을 때 crash 없음
# ─────────────────────────────────────────────────────────────────────────────

def test_us_exit_no_crash_without_max_price():
    """max_price 없어도 crash 없이 동작"""
    pos = {
        "symbol": "TSLA",
        "qty": 3,
        "entry_price": 100.0,
        # max_price 없음
    }

    # current_price=98 → hard_stop 아님, trailing_stop 아님 → None
    intent = evaluate_exit(pos, current_price=98.0)
    # hard_stop (-2%) 이하 아니므로 None or not hard_stop
    assert intent is None or intent["exit_type"] != "hard_stop"


# ─────────────────────────────────────────────────────────────────────────────
# Test 8: avg_cost fallback으로 entry_price 사용
# ─────────────────────────────────────────────────────────────────────────────

def test_us_exit_uses_avg_cost_as_entry_price():
    """avg_cost=100 → entry_price로 사용, current=89 → hard_stop"""
    pos = {
        "symbol": "GOOG",
        "qty": 2,
        "avg_cost": 100.0,
    }

    intent = evaluate_exit(pos, current_price=89.0)

    assert intent is not None
    assert intent["exit_type"] == "hard_stop"


# ─────────────────────────────────────────────────────────────────────────────
# Test 9: pnl_missing_fail_closed 의 unrealized_pnl_pct 확인
# ─────────────────────────────────────────────────────────────────────────────

def test_us_exit_pnl_missing_fail_closed_has_minus_999_pnl_pct(monkeypatch):
    """fail-closed intent는 pnl_pct=-999로 명확히 표시"""
    monkeypatch.setenv("US_EXIT_FAIL_CLOSED_ON_PNL_MISSING", "1")

    pos = {"symbol": "ZZZ", "qty": 5}
    intent = evaluate_exit(pos, current_price=50.0)

    assert intent is not None
    assert intent["unrealized_pnl_pct"] == -999.0
