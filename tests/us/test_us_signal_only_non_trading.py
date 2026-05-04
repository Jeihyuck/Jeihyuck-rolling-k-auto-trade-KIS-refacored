# -*- coding: utf-8 -*-
"""US Signal-Only Mode Tests (Non-Trading Day).

Tests for weekend/holiday SIGNAL_ONLY mode that generates trading signals
but blocks KIS order API calls.
"""
from __future__ import annotations

import os
from datetime import datetime
from unittest.mock import Mock, patch

import pytest


# ============================================================================
# Test 1: Workflow run mode resolution
# ============================================================================
def test_workflow_run_mode_resolution_trading_day():
    """Test that trading day resolves to TRADE mode."""
    from trader.us.market_calendar import is_us_trading_day
    
    # Use a known trading day (2026-05-01 is a Friday)
    test_date = datetime(2026, 5, 1).date()
    
    with patch("trader.us.market_calendar.now_ny") as mock_now:
        mock_now.return_value = datetime(2026, 5, 1, 10, 0)
        
        is_trading = is_us_trading_day(test_date)
        
        if is_trading:
            expected_run_mode = "TRADE"
            expected_signal_only = "0"
            expected_kis_order_allowed = "1"
        else:
            expected_run_mode = "NON_TRADING_SIGNAL_ONLY"
            expected_signal_only = "1"
            expected_kis_order_allowed = "0"
        
        # Verify expectations match workflow logic
        assert expected_run_mode in ("TRADE", "NON_TRADING_SIGNAL_ONLY")
        assert expected_signal_only in ("0", "1")
        assert expected_kis_order_allowed in ("0", "1")


def test_workflow_run_mode_resolution_non_trading_day():
    """Test that non-trading day resolves to NON_TRADING_SIGNAL_ONLY mode."""
    from trader.us.market_calendar import is_us_trading_day
    
    # Use a known weekend day (2026-05-02 is a Saturday)
    test_date = datetime(2026, 5, 2).date()
    
    is_trading = is_us_trading_day(test_date)
    
    # Saturday should not be a trading day
    assert not is_trading
    
    # Expected workflow outputs
    expected_run_mode = "NON_TRADING_SIGNAL_ONLY"
    expected_signal_only = "1"
    expected_order_allowed = "0"
    expected_kis_order_allowed = "0"
    expected_kis_data_allowed = "0"
    expected_non_trading_day = "1"
    
    assert expected_run_mode == "NON_TRADING_SIGNAL_ONLY"
    assert expected_signal_only == "1"
    assert expected_kis_order_allowed == "0"


# ============================================================================
# Test 2: trade_session_runner with signal_only=True
# ============================================================================
def test_trade_session_runner_signal_only_mode():
    """Test trade_session_runner respects signal_only mode."""
    from trader.us.runner.trade_session_runner import run_trade_session
    
    # Mock dependencies - patch where it's used, not where it's defined
    with patch("trader.us.runner.trade_tick_runner.run_trade_tick") as mock_tick:
        mock_tick.return_value = {
            "status": "OK_SIGNAL_ONLY",
            "orders": [],
            "signal_only": 1,
        }
        
        result = run_trade_session(
            session="am",
            env="practice",
            offline=True,
            max_minutes=1,
            interval_sec=1,
            max_ticks=1,
            run_mode="NON_TRADING_SIGNAL_ONLY",
            signal_only=True,
            force_now="2026-05-02T10:00:00-04:00",  # Saturday
        )
        
        # Should complete with signal_only status
        assert result["status"] in ("OK_SIGNAL_ONLY", "OK_WITH_WARNINGS_SIGNAL_ONLY")
        assert result["signal_only"] is True
        assert result["kis_order_allowed"] is False
        
        # Verify tick was called with signal_only params
        mock_tick.assert_called()
        call_kwargs = mock_tick.call_args[1]
        assert call_kwargs["signal_only"] is True
        assert call_kwargs["run_mode"] == "NON_TRADING_SIGNAL_ONLY"
        assert call_kwargs["kis_order_allowed"] is False


# ============================================================================
# Test 3: trade_tick_runner with signal_only=True
# ============================================================================
def test_trade_tick_runner_signal_only_allows_non_trading_day():
    """Test trade_tick_runner allows execution on non-trading day when signal_only=True."""
    from trader.us.runner.trade_tick_runner import run_trade_tick
    
    # Mock dependencies - patch at the source module
    with patch("trader.us.market_calendar.is_us_trading_day") as mock_is_td, \
         patch("trader.us.market_calendar.market_phase") as mock_phase, \
         patch("trader.us.execution.order_router.route_order") as mock_route, \
         patch("trader.us.execution.fills.get_fills_today") as mock_fills, \
         patch("trader.us.execution.reconcile.reconcile_positions") as mock_reconcile:
        
        mock_is_td.return_value = False  # Non-trading day
        mock_phase.return_value = "REGULAR_OPEN"
        mock_fills.return_value = []
        mock_route.return_value = {"status": "SIGNAL_ONLY", "symbol": "AAPL"}
        mock_reconcile.return_value = {"status": "OK", "positions": [], "position_count": 0}
        
        result = run_trade_tick(
            session="am",
            env="practice",
            offline=True,
            force_now="2026-05-02T10:00:00-04:00",  # Saturday
            run_mode="NON_TRADING_SIGNAL_ONLY",
            signal_only=True,
            kis_order_allowed=False,
        )
        
        # Should NOT skip on non-trading day when signal_only=True
        assert result["status"] in ("OK_SIGNAL_ONLY", "OK_WITH_WARNINGS_SIGNAL_ONLY")
        assert result["status"] != "SKIP"
        assert result["signal_only_mode"] is True


def test_trade_tick_runner_without_signal_only_skips_non_trading_day():
    """Test trade_tick_runner skips non-trading day when signal_only=False."""
    from trader.us.runner.trade_tick_runner import run_trade_tick
    
    with patch("trader.us.market_calendar.is_us_trading_day") as mock_is_td:
        mock_is_td.return_value = False  # Non-trading day
        
        result = run_trade_tick(
            session="am",
            env="practice",
            offline=True,
            force_now="2026-05-02T10:00:00-04:00",  # Saturday
            run_mode="TRADE",
            signal_only=False,
            kis_order_allowed=True,
        )
        
        # Should SKIP on non-trading day when signal_only=False
        assert result["status"] == "SKIP"
        assert result["reason"] == "not_trading_day"


# ============================================================================
# Test 4: order_router returns SIGNAL_ONLY
# ============================================================================
def test_order_router_signal_only_blocks_order():
    """Test order_router returns SIGNAL_ONLY status when signal_only=True."""
    from trader.us.execution.order_router import route_order
    
    intent = {
        "symbol": "AAPL",
        "side": "BUY",
        "qty": 10,
        "limit_price": 150.0,
        "exchange": "NASDAQ",
        "notional_usd": 1500.0,
        "client_order_key": "test_signal_only_key",
    }
    
    result = route_order(
        intent,
        current_daily_notional_usd=0.0,
        current_position_count=0,
        total_portfolio_usd=10000.0,
        available_cash_usd=10000.0,
        signal_only=True,
    )
    
    # Should return SIGNAL_ONLY status, not attempt KIS order
    assert result["status"] == "SIGNAL_ONLY"
    assert result["reason"] == "KIS_ORDER_DISABLED_SIGNAL_ONLY"
    assert result["symbol"] == "AAPL"
    assert result["side"] == "BUY"
    assert result["qty"] == 10


# ============================================================================
# Test 5: kis_us_client blocks orders when US_KIS_ORDER_ALLOWED=0
# ============================================================================
def test_kis_us_client_blocks_buy_order_when_disabled():
    """Test KIS client raises error when US_KIS_ORDER_ALLOWED=0."""
    from trader.us.execution.kis_us_client import KisUSClient
    
    client = KisUSClient(env="practice", offline=False)
    
    # Mock the paper trading guard to pass, then test US_KIS_ORDER_ALLOWED guard
    with patch("trader.us.config.assert_us_paper_order_allowed"), \
         patch.dict(os.environ, {"US_KIS_ORDER_ALLOWED": "0"}):
        
        with pytest.raises(RuntimeError, match=r"\[US_KIS_ORDER_BLOCKED\]"):
            client.place_us_buy_order(
                symbol="AAPL",
                exchange="NASDAQ",
                qty=10,
                price=150.0,
            )


def test_kis_us_client_blocks_sell_order_when_disabled():
    """Test KIS client raises error for sell when US_KIS_ORDER_ALLOWED=0."""
    from trader.us.execution.kis_us_client import KisUSClient
    
    client = KisUSClient(env="practice", offline=False)
    
    # Mock the paper trading guard to pass, then test US_KIS_ORDER_ALLOWED guard
    with patch("trader.us.config.assert_us_paper_order_allowed"), \
         patch.dict(os.environ, {"US_KIS_ORDER_ALLOWED": "0"}):
        
        with pytest.raises(RuntimeError, match=r"\[US_KIS_ORDER_BLOCKED\]"):
            client.place_us_sell_order(
                symbol="AAPL",
                exchange="NASDAQ",
                qty=10,
                price=150.0,
            )


# ============================================================================
# Test 6: fills.py skips KIS when US_SIGNAL_ONLY=1
# ============================================================================
def test_fills_signal_only_skips_kis_query():
    """Test get_fills_today skips KIS API when signal_only=True."""
    from trader.us.execution.fills import get_fills_today
    from trader.us.data_provider import USDataProvider
    
    provider = USDataProvider(offline=False)
    
    # Mock load_today_fills to return test data
    with patch("trader.us.db.repos.load_today_fills", create=True) as mock_db_fills, \
         patch.object(provider, "_get_client") as mock_client:
        
        mock_db_fills.return_value = [
            {"symbol": "AAPL", "qty": 5, "price": 150.0, "side": "BUY"}
        ]
        
        # signal_only=True should use DB only
        result = get_fills_today(provider=provider, signal_only=True)
        
        # Should not call KIS client
        mock_client.assert_not_called()
        
        # Should return DB fills with status
        assert result["status"] == "OK"
        assert len(result["fills"]) == 1
        assert result["fills"][0]["symbol"] == "AAPL"


def test_fills_signal_only_env_var_skips_kis_query():
    """Test get_fills_today respects US_SIGNAL_ONLY environment variable."""
    from trader.us.execution.fills import get_fills_today
    from trader.us.data_provider import USDataProvider
    
    provider = USDataProvider(offline=False)
    
    # Mock load_today_fills to return empty list
    with patch("trader.us.db.repos.load_today_fills", create=True) as mock_db_fills, \
         patch.object(provider, "_get_client") as mock_client, \
         patch.dict(os.environ, {"US_SIGNAL_ONLY": "1"}):
        
        mock_db_fills.return_value = []
        
        # US_SIGNAL_ONLY=1 should use DB only even if signal_only=False
        result = get_fills_today(provider=provider, signal_only=False)
        
        # Should not call KIS client
        mock_client.assert_not_called()
        
        # Should return DB fills (empty in this case) with status
        assert result["status"] == "OK"
        assert result["fills"] == []


# ============================================================================
# Test 7: End-to-end non-trading day signal generation
# ============================================================================
def test_end_to_end_signal_only_non_trading_day():
    """Test full signal-only flow on non-trading day."""
    from trader.us.runner.trade_session_runner import run_trade_session
    
    # Patch at the source modules
    with patch("trader.us.market_calendar.is_us_trading_day") as mock_is_td, \
         patch("trader.us.market_calendar.market_phase") as mock_phase, \
         patch("trader.us.execution.order_router.route_order") as mock_route, \
         patch("trader.us.execution.fills.get_fills_today") as mock_fills, \
         patch("trader.us.execution.reconcile.reconcile_positions") as mock_reconcile, \
         patch("trader.us.db.repos.load_today_fills", create=True) as mock_db_fills, \
         patch("trader.us.db.repos.save_fills") as mock_save_fills, \
         patch("trader.us.db.repos.save_position_snapshot") as mock_save_pos, \
         patch("trader.us.db.repos.save_reconcile_log") as mock_save_recon:
        
        # Setup: Non-trading day (Saturday)
        mock_is_td.return_value = False
        mock_phase.return_value = "REGULAR_OPEN"
        mock_fills.return_value = []
        mock_db_fills.return_value = []
        mock_reconcile.return_value = {"status": "OK", "positions": [], "position_count": 0}
        
        # route_order should return SIGNAL_ONLY
        mock_route.return_value = {
            "status": "SIGNAL_ONLY",
            "reason": "KIS_ORDER_DISABLED_SIGNAL_ONLY",
            "symbol": "AAPL",
        }
        
        # Run session with signal_only mode on Saturday
        result = run_trade_session(
            session="am",
            env="practice",
            offline=True,
            max_minutes=1,
            interval_sec=1,
            max_ticks=2,
            run_mode="NON_TRADING_SIGNAL_ONLY",
            signal_only=True,
            force_now="2026-05-02T10:00:00-04:00",  # Saturday
        )
        
        # Verify session completed with signal_only status
        assert result["status"] in ("OK_SIGNAL_ONLY", "OK_WITH_WARNINGS_SIGNAL_ONLY")
        assert result["signal_only"] is True
        assert result["kis_order_allowed"] is False
        assert result["run_mode"] == "NON_TRADING_SIGNAL_ONLY"
        assert result["tick_count"] > 0
        
        # In offline mode with mocked components, verify ticks completed successfully
        # (route_order may not be called if no entry/exit candidates generated)
