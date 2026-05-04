# -*- coding: utf-8 -*-
"""Tests: trade_session_runner force_now + max_ticks loop 동작 검증."""
from __future__ import annotations

import importlib
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

FORCE_NOW_FRIDAY = "2026-05-01T09:35:00-04:00"   # Friday — trading day
FORCE_NOW_SATURDAY = "2026-05-02T09:35:00-04:00"  # Saturday — not trading day
FORCE_NOW_AFTERNOON = "2026-05-01T13:00:00-04:00"  # Friday afternoon


def _make_ok_tick(*args, **kwargs):
    return {"status": "OK", "orders": []}


def _make_skip_tick(*args, **kwargs):
    return {"status": "SKIP", "reason": "not_trading_day"}


# ---------------------------------------------------------------------------
# Test 1: force_now + max_ticks=3 + offline → AM tick 3회 실행
# ---------------------------------------------------------------------------

def test_force_now_am_max_ticks_3():
    from trader.us.runner import trade_session_runner as mod

    tick_calls = []

    def _fake_tick(session, env, offline, force_now, **kwargs):
        tick_calls.append(force_now)
        return {"status": "OK", "orders": []}

    with (
        patch("trader.us.runner.trade_tick_runner.run_trade_tick", side_effect=_fake_tick),
        patch("trader.us.market_calendar.is_us_trading_day", return_value=True),
        patch("trader.us.budget.resolve_us_order_budget", return_value={"capital_usd_cap": 10000.0}),
    ):
        result = mod.run_trade_session(
            session="am",
            env="practice",
            offline=True,
            force_now=FORCE_NOW_FRIDAY,
            max_ticks=3,
            interval_sec=1,
            max_minutes=60,
        )

    assert result["status"] in ("OK", "OK_WITH_WARNINGS"), f"Unexpected status: {result}"
    assert result["tick_count"] == 3, f"Expected 3 ticks, got {result['tick_count']}"
    assert len(tick_calls) == 3
    # force_now가 각 tick마다 전달되어야 함
    assert all(t is not None for t in tick_calls)


# ---------------------------------------------------------------------------
# Test 2: force_now + max_ticks=3 + offline → Afternoon tick 3회 실행
# ---------------------------------------------------------------------------

def test_force_now_afternoon_max_ticks_3():
    from trader.us.runner import trade_session_runner as mod

    tick_calls = []

    def _fake_tick(session, env, offline, force_now, **kwargs):
        tick_calls.append((session, force_now))
        return {"status": "OK", "orders": []}

    with (
        patch("trader.us.runner.trade_tick_runner.run_trade_tick", side_effect=_fake_tick),
        patch("trader.us.market_calendar.is_us_trading_day", return_value=True),
        patch("trader.us.budget.resolve_us_order_budget", return_value={"capital_usd_cap": 10000.0}),
    ):
        result = mod.run_trade_session(
            session="afternoon",
            env="practice",
            offline=True,
            force_now=FORCE_NOW_AFTERNOON,
            max_ticks=3,
            interval_sec=1,
            max_minutes=60,
        )

    assert result["status"] in ("OK", "OK_WITH_WARNINGS"), f"Unexpected status: {result}"
    assert result["tick_count"] == 3
    assert all(s == "afternoon" for s, _ in tick_calls)


# ---------------------------------------------------------------------------
# Test 3: force_now만 있고 max_ticks=0 → single tick
# ---------------------------------------------------------------------------

def test_force_now_single_tick_when_max_ticks_zero():
    from trader.us.runner import trade_session_runner as mod

    tick_calls = []

    def _fake_tick(session, env, offline, force_now, **kwargs):
        tick_calls.append(force_now)
        return {"status": "OK", "orders": []}

    with (
        patch("trader.us.runner.trade_tick_runner.run_trade_tick", side_effect=_fake_tick),
        patch("trader.us.market_calendar.is_us_trading_day", return_value=True),
        patch("trader.us.budget.resolve_us_order_budget", return_value={"capital_usd_cap": 10000.0}),
    ):
        result = mod.run_trade_session(
            session="am",
            env="practice",
            offline=True,
            force_now=FORCE_NOW_FRIDAY,
            max_ticks=0,
            interval_sec=1,
            max_minutes=60,
        )

    assert result["tick_count"] == 1, f"Expected 1 tick, got {result['tick_count']}"


# ---------------------------------------------------------------------------
# Test 4: force_now=금요일 장중 → 실제 실행일이 주말이어도 not_trading_day SKIP 없음
# ---------------------------------------------------------------------------

def test_force_now_friday_no_trading_day_skip():
    """force_now가 거래일이면 실제 현재시간(주말)에 상관없이 SKIP되지 않아야 한다."""
    from trader.us.runner import trade_session_runner as mod

    tick_calls = []

    def _fake_tick(session, env, offline, force_now, **kwargs):
        tick_calls.append(force_now)
        return {"status": "OK", "orders": []}

    # is_us_trading_day는 force_now 기준 날짜(금요일)에 True 반환
    def _trading_day_check(date):
        from datetime import date as dt_date
        import datetime
        friday = datetime.date(2026, 5, 1)
        return date == friday

    with (
        patch("trader.us.runner.trade_tick_runner.run_trade_tick", side_effect=_fake_tick),
        patch("trader.us.market_calendar.is_us_trading_day", side_effect=_trading_day_check),
        patch("trader.us.budget.resolve_us_order_budget", return_value={"capital_usd_cap": 10000.0}),
    ):
        result = mod.run_trade_session(
            session="am",
            env="practice",
            offline=True,
            force_now=FORCE_NOW_FRIDAY,
            max_ticks=1,
            interval_sec=1,
            max_minutes=60,
        )

    assert result["status"] != "SKIP", "Should not be SKIP for a trading day force_now"
    assert len(tick_calls) >= 1


# ---------------------------------------------------------------------------
# Test 5: force_now=토요일 → not_trading_day SKIP 유지
# ---------------------------------------------------------------------------

def test_force_now_saturday_trading_day_skip():
    """force_now가 토요일이면 not_trading_day SKIP이 발생해야 한다."""
    from trader.us.runner import trade_session_runner as mod

    # is_us_trading_day: 토요일(2026-05-02)은 False
    def _trading_day_check(date):
        from datetime import date as dt_date
        import datetime
        saturday = datetime.date(2026, 5, 2)
        return date != saturday

    tick_calls = []

    def _fake_tick(session, env, offline, force_now, **kwargs):
        tick_calls.append(force_now)
        return {"status": "SKIP", "reason": "not_trading_day"}

    with (
        patch("trader.us.runner.trade_tick_runner.run_trade_tick", side_effect=_fake_tick),
        patch("trader.us.market_calendar.is_us_trading_day", side_effect=_trading_day_check),
        patch("trader.us.budget.resolve_us_order_budget", return_value={"capital_usd_cap": 10000.0}),
    ):
        result = mod.run_trade_session(
            session="am",
            env="practice",
            offline=True,
            force_now=FORCE_NOW_SATURDAY,
            max_ticks=1,
            interval_sec=1,
            max_minutes=60,
        )

    assert result["status"] == "SKIP", f"Expected SKIP for Saturday, got {result['status']}"
    assert "not_trading_day" in result.get("reason", "")
