# -*- coding: utf-8 -*-
"""Phase 5-7: exit router book/horizon 라우팅 검증.

검증 항목:
- SWING_BOOK → SWING_EXIT_ROUTER
- DAY_BOOK → DAY_EXIT_ROUTER
- meta 없으면 SWING_BOOK fallback
- same-day soft exit 차단 (SWING_BOOK)
- hard stop은 same-day라도 허용
"""
from __future__ import annotations

import os
import pytest
from unittest.mock import patch


def _swing_position(symbol: str = "CRDO") -> dict:
    return {
        "symbol": symbol,
        "exchange": "NASDAQ",
        "qty": 10,
        "holding_qty": 10,
        "orderable_qty": 10,
        "avg_price_usd": 68.0,
        "entry_price": 68.0,
        "book": "SWING_BOOK",
        "horizon": "SWING_CARRY",
        "exit_policy": "US_SWING_DEFAULT",
    }


def _day_position(symbol: str = "NVTS") -> dict:
    return {
        "symbol": symbol,
        "exchange": "NASDAQ",
        "qty": 10,
        "holding_qty": 10,
        "orderable_qty": 10,
        "avg_price_usd": 10.0,
        "entry_price": 10.0,
        "book": "DAY_BOOK",
        "horizon": "DAY_TRADE",
        "exit_policy": "US_DAY_DEFAULT",
    }


# ── normalize_book / normalize_horizon ────────────────────────────────────

def test_normalize_book_swing():
    from trader.us.pb1.us_exit_router import normalize_book
    assert normalize_book("SWING_BOOK") == "SWING_BOOK"
    assert normalize_book("swing") == "SWING_BOOK"
    assert normalize_book("swing_carry") == "SWING_BOOK"
    assert normalize_book(None) == "SWING_BOOK"


def test_normalize_book_day():
    from trader.us.pb1.us_exit_router import normalize_book
    assert normalize_book("DAY_BOOK") == "DAY_BOOK"
    assert normalize_book("day") == "DAY_BOOK"
    assert normalize_book("intraday") == "DAY_BOOK"


def test_normalize_horizon_swing():
    from trader.us.pb1.us_exit_router import normalize_horizon
    assert normalize_horizon("SWING_CARRY") == "SWING_CARRY"
    assert normalize_horizon("swing") == "SWING_CARRY"
    assert normalize_horizon(None) == "SWING_CARRY"


def test_normalize_horizon_day():
    from trader.us.pb1.us_exit_router import normalize_horizon
    assert normalize_horizon("DAY_TRADE") == "DAY_TRADE"
    assert normalize_horizon("intraday") == "DAY_TRADE"


# ── route_exit_by_book_horizon 라우팅 확인 ─────────────────────────────────

def test_swing_book_routes_to_swing_router(capsys, monkeypatch):
    """SWING_BOOK 포지션은 SWING_EXIT_ROUTER를 통한다."""
    from trader.us.pb1.us_exit_router import route_exit_by_book_horizon, evaluate_swing_exit

    pos = _swing_position()
    # hard stop 조건 (entry 68, current 61 → -10%)
    current_price = 61.0

    called = []
    original_swing = evaluate_swing_exit
    def mock_swing(position, current_price, now=None):
        called.append("SWING")
        return original_swing(position, current_price, now)

    monkeypatch.setattr(
        "trader.us.pb1.us_exit_router.evaluate_swing_exit", mock_swing
    )
    route_exit_by_book_horizon(pos, current_price, now=None)
    assert "SWING" in called


def test_day_book_routes_to_day_router(monkeypatch):
    """DAY_BOOK 포지션은 DAY_EXIT_ROUTER를 통한다."""
    from trader.us.pb1.us_exit_router import route_exit_by_book_horizon, evaluate_day_exit

    pos = _day_position()
    current_price = 12.0  # +20% → day_profit_take 트리거

    called = []
    original_day = evaluate_day_exit
    def mock_day(position, current_price, now=None):
        called.append("DAY")
        return original_day(position, current_price, now)

    monkeypatch.setattr(
        "trader.us.pb1.us_exit_router.evaluate_day_exit", mock_day
    )
    route_exit_by_book_horizon(pos, current_price, now=None)
    assert "DAY" in called


def test_no_meta_fallback_swing(monkeypatch):
    """meta/book 없으면 SWING_BOOK fallback."""
    from trader.us.pb1.us_exit_router import route_exit_by_book_horizon, evaluate_swing_exit

    pos = {
        "symbol": "LITE",
        "exchange": "NASDAQ",
        "qty": 5,
        "holding_qty": 5,
        "avg_price_usd": 50.0,
        "entry_price": 50.0,
        # book/horizon/meta 없음
    }
    current_price = 45.0  # -10% hard stop

    called = []
    original = evaluate_swing_exit
    def mock_sw(position, current_price, now=None):
        called.append("SWING")
        return original(position, current_price, now)

    monkeypatch.setattr("trader.us.pb1.us_exit_router.evaluate_swing_exit", mock_sw)
    route_exit_by_book_horizon(pos, current_price, now=None)
    assert "SWING" in called


def test_swing_hard_stop_allowed_same_day(monkeypatch):
    """SWING_BOOK 당일 포지션도 hard_stop이면 SELL intent 생성."""
    from datetime import datetime, timezone
    from trader.us.pb1.us_exit_router import evaluate_swing_exit

    now = datetime.now(timezone.utc)
    pos = {
        "symbol": "CRDO",
        "exchange": "NASDAQ",
        "qty": 10,
        "holding_qty": 10,
        "orderable_qty": 10,
        "avg_price_usd": 68.0,
        "entry_price": 68.0,
        "book": "SWING_BOOK",
        "horizon": "SWING_CARRY",
        "entry_time": now.isoformat(),  # 방금 매수
    }
    current_price = 63.0  # -7.35% → hard_stop 초과

    with patch.dict(os.environ, {
        "US_SWING_BLOCK_SAME_DAY_SOFT_EXIT": "1",
        "US_SWING_MIN_HOLD_MINUTES": "390",
        "US_HARD_STOP_PCT": "0.07",
    }):
        intent = evaluate_swing_exit(pos, current_price, now=now)

    # hard stop이므로 same-day라도 허용
    assert intent is not None
    assert intent.get("exit_type") == "hard_stop"


def test_swing_soft_exit_blocked_same_day(monkeypatch):
    """SWING_BOOK 당일 포지션은 profit_protect/trailing 등 soft exit가 차단된다."""
    from datetime import datetime, timezone
    from trader.us.pb1.us_exit_router import evaluate_swing_exit

    now = datetime.now(timezone.utc)
    pos = {
        "symbol": "CRDO",
        "exchange": "NASDAQ",
        "qty": 10,
        "holding_qty": 10,
        "orderable_qty": 10,
        "avg_price_usd": 68.0,
        "entry_price": 68.0,
        "book": "SWING_BOOK",
        "horizon": "SWING_CARRY",
        "entry_time": now.isoformat(),  # 방금 매수
    }
    # +2% — profit_protect 범위 아래지만, trailing 등 soft exit 시뮬레이션
    current_price = 69.36  # +2%

    with patch.dict(os.environ, {
        "US_SWING_BLOCK_SAME_DAY_SOFT_EXIT": "1",
        "US_SWING_MIN_HOLD_MINUTES": "390",
        "US_PROFIT_PROTECT_PCT": "0.01",  # 낮게 설정해 profit_protect 유도
        "US_TRAILING_STOP_PCT": "0.005",
    }):
        intent = evaluate_swing_exit(pos, current_price, now=now)

    # soft exit이므로 same-day min-hold 이전 차단
    if intent is not None:
        # hard exit이면 허용
        from trader.us.pb1.us_exit_router import _HARD_EXIT_TYPES
        assert intent.get("exit_type") in _HARD_EXIT_TYPES, (
            f"same-day soft exit가 허용되면 안 됨: {intent.get('exit_type')}"
        )


def test_swing_soft_exit_blocks_without_entry_time(monkeypatch):
    """Unknown entry time must fail closed for SWING soft exits."""
    from trader.us.pb1.us_exit_router import evaluate_swing_exit

    position = _swing_position("TEST")
    with patch.dict(os.environ, {
        "US_SWING_BLOCK_SAME_DAY_SOFT_EXIT": "1",
        "US_PROFIT_PROTECT_PCT": "0.01",
    }):
        intent = evaluate_swing_exit(position, 69.36)

    assert intent is None


def test_day_profit_take_triggers():
    """DAY_BOOK은 day_profit_take가 작동해야 함."""
    from trader.us.pb1.us_exit_router import evaluate_day_exit

    pos = _day_position()
    current_price = 10.26  # +2.6%

    with patch.dict(os.environ, {"US_DAY_PROFIT_TAKE_PCT": "0.025"}):
        intent = evaluate_day_exit(pos, current_price)

    assert intent is not None
    assert intent.get("exit_type") == "day_profit_take"


def test_day_policy_not_applied_to_swing():
    """DAY_BOOK 정책(day_profit_take)이 SWING_BOOK에 적용되면 안 됨."""
    from trader.us.pb1.us_exit_router import route_exit_by_book_horizon

    pos = _swing_position()
    current_price = 69.36  # +2%

    with patch.dict(os.environ, {
        "US_DAY_PROFIT_TAKE_PCT": "0.025",
        "US_SWING_BLOCK_SAME_DAY_SOFT_EXIT": "0",  # guard 끄기
        "US_SWING_MIN_HOLD_MINUTES": "0",
    }):
        intent = route_exit_by_book_horizon(pos, current_price)

    if intent is not None:
        assert intent.get("exit_type") != "day_profit_take", (
            "SWING_BOOK에 day_profit_take가 적용되면 안 됨"
        )
