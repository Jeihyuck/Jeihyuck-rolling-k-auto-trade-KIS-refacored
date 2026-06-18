# -*- coding: utf-8 -*-
"""risk_gate BUY/SELL 분기 검증 테스트.

SELL 주문은 budget/cash/cutoff 체크를 건너뛰어야 한다.
BUY 주문은 모든 체크를 수행해야 한다.
"""
import os
import pytest
from unittest.mock import patch


_SAFE_ENV = {
    "US_AGENT_ENABLED": "true",
    "TRADING_REGION": "US",
    "KIS_ENV": "practice",
    "STRATEGY_ENV": "practice",
    "RUN_MODE": "TRADE",
    "STRATEGY_MODE": "TRADE",
    "SIGNAL_ONLY": "0",
    # Keep these side-split unit tests focused on BUY/SELL risk branch
    # behavior instead of CI-level order-permission guards.
    "DRY_RUN": "0",
    "DISABLE_LIVE_TRADING": "0",
    "DISABLE_REAL_TRADING": "0",
    "LIVE_TRADING_ENABLED": "1",
    "US_LIVE_TRADING_ENABLED": "1",
    "US_ORDER_ARMED": "1",
    "US_PAPER_TRADING_ENABLED": "1",
    "ALLOW_REAL_ORDER": "1",
    "US_MAX_ORDER_USD": "9999",
    "US_MAX_DAILY_NOTIONAL_USD": "99999",
    "US_MAX_POSITIONS": "100",
    "US_MAX_POSITION_WEIGHT": "1.0",
    "US_MIN_CASH_BUFFER_USD": "0",
    "US_PAPER_MAX_CAPITAL_KRW": "999999999",
    "US_BLOCK_NEW_ENTRY_AFTER_ET": "",
    "US_BLOCK_REBUY_AFTER_SELL_SAME_DAY": "false",
    "US_ORDER_ACCEPTED_IS_NOT_FILLED": "false",
}


def _make_buy_intent(symbol="AAPL", qty=1, notional=100.0):
    return {
        "symbol": symbol, "exchange": "NASDAQ", "side": "BUY",
        "qty": qty, "notional_usd": notional, "client_order_key": "k1",
    }


def _make_sell_intent(symbol="AAPL", qty=1, notional=100.0, available_qty=10):
    return {
        "symbol": symbol, "exchange": "NASDAQ", "side": "SELL",
        "qty": qty, "notional_usd": notional, "client_order_key": "k2",
        "available_qty": available_qty,
    }


def test_sell_skips_budget_check():
    """SELL 주문은 check_us_capital_budget를 호출하지 않아야 한다."""
    from trader.us.execution import risk_gate

    called = []

    def mock_budget_check(*args, **kwargs):
        called.append("budget_check")

    with patch.dict(os.environ, _SAFE_ENV, clear=True):
        with patch.object(risk_gate, "check_us_capital_budget", mock_budget_check):
            risk_gate.assert_order_allowed(
                _make_sell_intent(),
                available_cash_usd=10000.0,
                total_portfolio_usd=10000.0,
                allowed_symbols={"AAPL"},
                current_position_symbols={"AAPL"},
            )

    assert "budget_check" not in called, (
        "SELL must not call check_us_capital_budget"
    )


def test_sell_skips_entry_cutoff():
    """SELL 주문은 check_entry_cutoff를 통과해야 한다 (cutoff 후라도)."""
    from trader.us.execution import risk_gate
    from datetime import datetime
    import zoneinfo

    with patch.dict(os.environ, {**_SAFE_ENV, "US_BLOCK_NEW_ENTRY_AFTER_ET": "15:45"}, clear=True):
        # 15:50 ET — cutoff 이후여도 SELL은 통과해야 한다
        ny_tz = zoneinfo.ZoneInfo("America/New_York")
        now = datetime(2026, 5, 1, 15, 50, 0, tzinfo=ny_tz)
        # assert_order_allowed에서 check_entry_cutoff(side='SELL', ...)는 즉시 return
        risk_gate.assert_order_allowed(
            _make_sell_intent(),
            available_cash_usd=10000.0,
            total_portfolio_usd=10000.0,
            allowed_symbols={"AAPL"},
            current_position_symbols={"AAPL"},
            now=now,
        )
        # 예외 없이 통과 = pass


def test_buy_fails_after_cutoff():
    """BUY 주문은 15:45 ET 이후 차단되어야 한다."""
    from trader.us.execution import risk_gate
    from trader.us.execution.risk_gate import RiskGateBlocked
    from datetime import datetime
    import zoneinfo

    with patch.dict(os.environ, {**_SAFE_ENV, "US_BLOCK_NEW_ENTRY_AFTER_ET": "15:45"}, clear=True):
        ny_tz = zoneinfo.ZoneInfo("America/New_York")
        now = datetime(2026, 5, 1, 15, 50, 0, tzinfo=ny_tz)
        with pytest.raises(RiskGateBlocked, match="after_entry_cutoff"):
            risk_gate.assert_order_allowed(
                _make_buy_intent(),
                available_cash_usd=10000.0,
                total_portfolio_usd=10000.0,
                allowed_symbols={"AAPL"},
                current_position_symbols=set(),
                now=now,
            )


def test_sell_skips_cash_buffer_check():
    """SELL 주문은 check_cash_buffer를 호출하지 않아야 한다."""
    from trader.us.execution import risk_gate

    called = []

    def mock_cash_buffer(*args, **kwargs):
        called.append("cash_buffer")

    with patch.dict(os.environ, _SAFE_ENV, clear=True):
        with patch.object(risk_gate, "check_cash_buffer", mock_cash_buffer):
            risk_gate.assert_order_allowed(
                _make_sell_intent(),
                available_cash_usd=10000.0,
                total_portfolio_usd=10000.0,
                allowed_symbols={"AAPL"},
                current_position_symbols={"AAPL"},
            )

    assert "cash_buffer" not in called, (
        "SELL must not call check_cash_buffer"
    )


def test_sell_skips_daily_notional():
    """SELL 주문은 check_daily_notional을 호출하지 않아야 한다."""
    from trader.us.execution import risk_gate

    called = []

    def mock_daily(*args, **kwargs):
        called.append("daily_notional")

    with patch.dict(os.environ, _SAFE_ENV, clear=True):
        with patch.object(risk_gate, "check_daily_notional", mock_daily):
            risk_gate.assert_order_allowed(
                _make_sell_intent(),
                available_cash_usd=10000.0,
                total_portfolio_usd=10000.0,
                allowed_symbols={"AAPL"},
                current_position_symbols={"AAPL"},
            )

    assert "daily_notional" not in called, (
        "SELL must not call check_daily_notional"
    )


def test_sell_blocks_qty_exceeds_position():
    """SELL qty > available_qty일 때 차단되어야 한다."""
    from trader.us.execution.risk_gate import RiskGateBlocked

    with patch.dict(os.environ, _SAFE_ENV, clear=True):
        from trader.us.execution import risk_gate
        with pytest.raises(RiskGateBlocked, match="sell_qty_exceeds_position"):
            risk_gate.assert_order_allowed(
                _make_sell_intent(qty=20, available_qty=10),
                available_cash_usd=10000.0,
                total_portfolio_usd=10000.0,
                allowed_symbols={"AAPL"},
                current_position_symbols={"AAPL"},
            )


def test_buy_calls_budget_check():
    """BUY 주문은 check_us_capital_budget를 호출해야 한다."""
    from trader.us.execution import risk_gate

    called = []

    def mock_budget_check(*args, **kwargs):
        called.append("budget_check")

    with patch.dict(os.environ, _SAFE_ENV, clear=True):
        with patch.object(risk_gate, "check_us_capital_budget", mock_budget_check):
            risk_gate.assert_order_allowed(
                _make_buy_intent(),
                available_cash_usd=10000.0,
                total_portfolio_usd=10000.0,
                allowed_symbols={"AAPL"},
                current_position_symbols=set(),
            )

    assert "budget_check" in called, (
        "BUY must call check_us_capital_budget"
    )
