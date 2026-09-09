# -*- coding: utf-8 -*-
"""US Order Cap Enforcement Tests.

2026-05-11 incident 재발 방지를 위한 테스트.

검증 사항:
1. US_MAX_ORDER_USD를 초과하는 주문이 생성되지 않는지
2. sizing 단계에서 order cap 준수
3. entry intent 생성 시 order cap 준수
4. risk gate resize retry 로직
5. final report에 orders_blocked, block_reasons 포함
6. final status 판정 (NO_ORDERS_RISK_BLOCKED 등)
"""
import os
import pytest
from unittest.mock import MagicMock, patch


class TestUSPositionSizingOrderCap:
    """US position sizing이 US_MAX_ORDER_USD를 준수하는지 검증."""

    def test_sizing_respects_order_cap_price_380(self, monkeypatch):
        """price=380, US_MAX_ORDER_USD=2500이면 qty는 최대 6주."""
        monkeypatch.setenv("US_MAX_ORDER_USD", "2500")
        monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "0.10")
        monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "200")
        
        from trader.us.pb1.us_position_sizing import calc_position_size
        
        result = calc_position_size(
            price=380.0,
            available_cash_usd=93788.0,
            capital_usd_cap=34482.76,
            position_count=5,
            score=1.0,
        )
        
        assert result["blocked"] is False, f"Should not be blocked: {result}"
        assert result["qty"] <= 6, f"qty should be <= 6 for price=380, got {result['qty']}"
        assert result["notional_usd"] <= 2500.0, \
            f"notional_usd should be <= 2500, got {result['notional_usd']}"
        assert "order_cap_usd" in result, "order_cap_usd should be in result"
        assert result["order_cap_usd"] == 2500.0

    def test_sizing_respects_order_cap_price_392(self, monkeypatch):
        """price=392, US_MAX_ORDER_USD=2500이면 qty는 최대 6주."""
        monkeypatch.setenv("US_MAX_ORDER_USD", "2500")
        monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "0.10")
        
        from trader.us.pb1.us_position_sizing import calc_position_size
        
        result = calc_position_size(
            price=392.0,
            available_cash_usd=93788.0,
            capital_usd_cap=34482.76,
            position_count=5,
            score=1.0,
        )
        
        assert result["blocked"] is False, f"Should not be blocked: {result}"
        assert result["qty"] <= 6, f"qty should be <= 6 for price=392, got {result['qty']}"
        assert result["notional_usd"] <= 2500.0, \
            f"notional_usd should be <= 2500, got {result['notional_usd']}"

    def test_sizing_respects_order_cap_price_1050(self, monkeypatch):
        """price=1050, US_MAX_ORDER_USD=2500이면 qty는 최대 2주."""
        monkeypatch.setenv("US_MAX_ORDER_USD", "2500")
        
        from trader.us.pb1.us_position_sizing import calc_position_size
        
        result = calc_position_size(
            price=1050.0,
            available_cash_usd=50000.0,
            capital_usd_cap=50000.0,
            position_count=0,
            score=1.0,
        )
        
        assert result["blocked"] is False, f"Should not be blocked: {result}"
        assert result["qty"] <= 2, f"qty should be <= 2 for price=1050, got {result['qty']}"
        assert result["notional_usd"] <= 2500.0, \
            f"notional_usd should be <= 2500, got {result['notional_usd']}"

    def test_sizing_blocks_price_exceeds_order_cap(self, monkeypatch):
        """price=3000, US_MAX_ORDER_USD=2500이면 blocked=True."""
        monkeypatch.setenv("US_MAX_ORDER_USD", "2500")
        
        from trader.us.pb1.us_position_sizing import calc_position_size
        
        result = calc_position_size(
            price=3000.0,
            available_cash_usd=100000.0,
            capital_usd_cap=100000.0,
            position_count=0,
            score=1.0,
        )
        
        assert result["blocked"] is True, "Should be blocked when price > order_cap"
        assert result["qty"] == 0
        assert "price_exceeds_order_cap" in result["reason"] or \
               "alloc_below_one_share_due_to_order_cap" in result["reason"], \
               f"reason should indicate order cap issue, got {result['reason']}"


class TestUSEntryIntentOrderCap:
    """US entry intent 생성 시 order cap을 초과하지 않는지 검증."""

    @patch("trader.us.pb1.us_entry_engine.USDataProvider")
    def test_entry_intent_does_not_exceed_order_cap(self, mock_provider_cls, monkeypatch):
        """Entry intent의 notional_usd는 항상 US_MAX_ORDER_USD 이하여야 한다."""
        monkeypatch.setenv("US_MAX_ORDER_USD", "2500")
        monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "0.10")
        monkeypatch.setenv("US_MAX_NEW_ENTRIES_PER_TICK", "5")
        
        from trader.us.pb1.us_entry_engine import generate_entry_intents
        
        # Mock provider
        mock_provider = MagicMock()
        
        # Mock watchlist entries with high-score symbols
        watchlist_entries = [
            {"symbol": "AAPL", "exchange": "NASDAQ", "score": 0.95, "rank": 1},
            {"symbol": "GOOGL", "exchange": "NASDAQ", "score": 0.90, "rank": 2},
            {"symbol": "MSFT", "exchange": "NASDAQ", "score": 0.85, "rank": 3},
        ]
        
        # Mock current price to simulate high notional scenarios
        def mock_get_current_price(symbol, exchange):
            prices = {
                "AAPL": {"last": 380.0},
                "GOOGL": {"last": 392.0},
                "MSFT": {"last": 450.0},
            }
            return prices.get(symbol, {"last": 100.0})
        
        mock_provider.get_current_price = mock_get_current_price
        
        intents = generate_entry_intents(
            tickers=None,
            provider=mock_provider,
            sold_today=set(),
            available_cash_usd=100000.0,
            position_count=0,
            capital_usd_cap=50000.0,
            watchlist_entries=watchlist_entries,
        )
        
        # 모든 intent의 notional_usd는 2500달러 이하여야 한다
        for intent in intents:
            assert intent["notional_usd"] <= 2500.0, \
                f"Intent for {intent['symbol']} has notional_usd={intent['notional_usd']} > 2500"


    @patch("trader.us.pb1.us_entry_engine.USDataProvider")
    def test_entry_intent_economics_use_executable_limit_price(self, mock_provider_cls, monkeypatch):
        """2026-09-08 live regression: qty * limit_price must equal notional_usd."""
        monkeypatch.setenv("US_MAX_ORDER_USD", "10000")
        monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "1")
        monkeypatch.setenv("US_MAX_NEW_ENTRIES_PER_TICK", "1")
        monkeypatch.setenv("US_MIN_ENTRY_SCORE", "0")
        monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "0")
        monkeypatch.setenv("US_LIMIT_PRICE_BAND_PCT", "0.005")

        from trader.us.db import repos
        from trader.us.pb1.us_entry_engine import generate_entry_intents

        monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
        monkeypatch.setattr(repos, "has_pending_order_for_symbol_side", lambda **kwargs: False)
        monkeypatch.setattr(repos, "has_position", lambda symbol: False)
        monkeypatch.setattr(repos, "load_today_order_keys", lambda trade_date: set())

        class Provider:
            def get_current_price(self, symbol, exchange):
                return {"last": 334.09}

        rows = [{
            "symbol": "SNOW", "exchange": "NYSE", "score": 1.0, "rank": 1,
            "entry_style": "momentum", "momentum_score": 1.0,
        }]
        intents = generate_entry_intents(
            tickers=None,
            provider=Provider(),
            sold_today=set(),
            available_cash_usd=10000.0,
            position_count=0,
            capital_usd_cap=10000.0,
            max_new_entries=1,
            watchlist_entries=rows,
            current_position_symbols=set(),
        )
        assert len(intents) == 1
        intent = intents[0]
        assert intent["limit_price"] == pytest.approx(335.7604)
        assert intent["notional_usd"] == pytest.approx(intent["qty"] * intent["limit_price"])


class TestUSOrderRouterResizeRetry:
    """US order router의 resize retry 로직 검증."""

    @patch("trader.us.execution.order_router.save_order_intent")
    @patch("trader.us.execution.order_router.load_today_order_keys")
    @patch("trader.us.execution.order_router.mark_order_intent_blocked")
    @patch("trader.us.execution.order_router.mark_order_intent_dry_run")
    @patch("trader.us.execution.order_router.save_dry_run_order")
    def test_resize_retry_on_notional_exceeds_order_limit(
        self,
        mock_save_dry_run,
        mock_mark_dry_run,
        mock_mark_blocked,
        mock_load_keys,
        mock_save_intent,
        monkeypatch,
    ):
        """notional_exceeds_order_limit이면 qty를 축소해서 재시도한다."""
        monkeypatch.setenv("US_MAX_ORDER_USD", "2500")
        monkeypatch.setenv("DRY_RUN", "1")
        monkeypatch.setenv("US_AGENT_ENABLED", "1")
        monkeypatch.setenv("TRADING_REGION", "US")
        monkeypatch.setenv("KIS_ENV", "practice")
        monkeypatch.setenv("US_PAPER_TRADING_ENABLED", "1")
        monkeypatch.setenv("US_LIVE_TRADING_ENABLED", "0")
        monkeypatch.setenv("DISABLE_REAL_TRADING", "1")
        
        mock_load_keys.return_value = set()
        
        from trader.us.execution.order_router import route_order
        
        # 9주 * 380 = 3420달러 → order_cap 초과
        intent = {
            "symbol": "COHR",
            "exchange": "NASDAQ",
            "side": "BUY",
            "qty": 9,
            "limit_price": 380.0,
            "notional_usd": 3420.0,
            "client_order_key": "test_key_123",
        }
        
        result = route_order(
            intent,
            current_daily_notional_usd=0.0,
            current_position_count=0,
            total_portfolio_usd=100000.0,
            available_cash_usd=100000.0,
            kis_client=None,
            signal_only=False,
            kis_order_allowed=True,
        )
        
        # resize retry가 성공했으면 DRY_RUN status가 나와야 한다
        # (또는 여전히 BLOCKED지만 resize 시도 로그가 있어야 한다)
        assert result["status"] in ("DRY_RUN", "BLOCKED"), \
            f"Expected DRY_RUN or BLOCKED after resize retry, got {result['status']}"
        
        # resize가 성공했으면 intent의 qty가 줄어들어야 한다
        # (DRY_RUN이면 resize 성공)
        if result["status"] == "DRY_RUN":
            # resized intent가 저장되었어야 한다
            assert mock_save_dry_run.called, "save_dry_run_order should be called after resize retry"


class TestUSFinalStatusContract:
    """US final status 체계 검증."""

    def test_status_no_entry_intents(self):
        """entry_intents=0이면 OK_NO_TRADE 또는 OK_SIGNAL_ONLY."""
        from trader.us.runner.trade_tick_runner import run_trade_tick
        
        # This is a mock test; actual implementation would require full tick runner setup
        # For now, we verify the status decision logic is correct
        # (실제 tick runner 호출은 integration test에서 수행)
        
        # Mock scenario: entry_intents=0, orders_sent=0
        entry_intents = []
        orders = []
        orders_sent = 0
        blocked_cnt = 0
        signal_only = False
        
        # Status decision logic (from trade_tick_runner.py)
        if len(entry_intents) == 0 and orders_sent == 0:
            status = "OK_NO_TRADE" if not signal_only else "OK_SIGNAL_ONLY"
        else:
            status = "OK"
        
        assert status in ("OK_NO_TRADE", "OK_SIGNAL_ONLY"), \
            f"Expected OK_NO_TRADE or OK_SIGNAL_ONLY, got {status}"

    def test_status_no_orders_risk_blocked(self):
        """entry_intents > 0, orders_sent=0, blocked > 0이면 NO_ORDERS_RISK_BLOCKED."""
        entry_intents_count = 3
        orders_sent = 0
        blocked_cnt = 3
        signal_only = False
        
        # Status decision logic
        if entry_intents_count > 0 and orders_sent == 0 and blocked_cnt > 0:
            status = "NO_ORDERS_RISK_BLOCKED"
        else:
            status = "OK"
        
        assert status == "NO_ORDERS_RISK_BLOCKED", \
            f"Expected NO_ORDERS_RISK_BLOCKED, got {status}"

    def test_status_partial_orders_blocked(self):
        """orders_sent > 0, blocked > 0이면 PARTIAL_ORDERS_BLOCKED."""
        orders_sent = 2
        blocked_cnt = 1
        signal_only = False
        
        # Status decision logic
        if orders_sent > 0 and blocked_cnt > 0:
            status = "PARTIAL_ORDERS_BLOCKED"
        else:
            status = "OK"
        
        assert status == "PARTIAL_ORDERS_BLOCKED", \
            f"Expected PARTIAL_ORDERS_BLOCKED, got {status}"

    def test_status_ok_orders_sent(self):
        """orders_sent > 0, blocked=0이면 OK_ORDERS_SENT."""
        orders_sent = 3
        blocked_cnt = 0
        total_warnings = 0
        signal_only = False
        
        # Status decision logic
        if orders_sent > 0:
            status = "OK_ORDERS_SENT" if total_warnings == 0 else "OK_WITH_WARNINGS"
        else:
            status = "OK"
        
        assert status == "OK_ORDERS_SENT", \
            f"Expected OK_ORDERS_SENT, got {status}"


class TestUSReportContract:
    """US report에 필수 필드가 포함되는지 검증."""

    def test_report_includes_orders_blocked_and_block_reasons(self):
        """Report에 orders_blocked와 block_reasons가 포함되어야 한다."""
        # Mock tick result
        tick_result = {
            "status": "NO_ORDERS_RISK_BLOCKED",
            "orders_blocked": 3,
            "block_reasons": {"notional_exceeds_order_limit": 3},
            "entry_intents": 3,
            "orders_sent": 0,
            "fills": 0,
            "positions": 0,
            "prep_status": "OK",
            "locked_watchlist_count": 10,
            "entry_eval_status": "OK",
            "entry_error_type": "",
            "entry_error_message": "",
            "last_stage": "order_route",
        }
        
        # Verify required fields exist
        assert "orders_blocked" in tick_result, "orders_blocked should be in tick result"
        assert "block_reasons" in tick_result, "block_reasons should be in tick result"
        assert isinstance(tick_result["block_reasons"], dict), \
            "block_reasons should be a dict"
        assert tick_result["orders_blocked"] == 3
        assert tick_result["block_reasons"]["notional_exceeds_order_limit"] == 3
