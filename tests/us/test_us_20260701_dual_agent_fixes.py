import time


def test_us_buy_weight_clamp_submits_reduced_qty(monkeypatch):
    from trader.us.execution.risk_gate import clamp_us_buy_qty_by_position_weight
    result = clamp_us_buy_qty_by_position_weight(
        symbol="ZZZZ", proposed_qty=7, price=10.0, account_equity_usd=1000.0, available_cash_usd=1000.0
    )
    assert result["adjusted_qty"] == 5
    assert result["reason"] == "us_buy_submitted_after_clamp"
    assert result["adjusted_notional"] <= 50.0


def test_us_qty_zero_after_weight_clamp():
    from trader.us.execution.risk_gate import clamp_us_buy_qty_by_position_weight
    result = clamp_us_buy_qty_by_position_weight(
        symbol="YYYY", proposed_qty=1, price=1000.0, account_equity_usd=1000.0, available_cash_usd=1000.0
    )
    assert result["adjusted_qty"] == 0
    assert result["reason"] == "us_qty_zero_after_weight_clamp"


def test_us_sell_ack_balance_delta_full_fill_allows_residual_recheck():
    from trader.us.execution.reconcile import reconcile_us_sell_ack_by_balance_delta
    result = reconcile_us_sell_ack_by_balance_delta(
        {"side": "SELL", "qty_requested": 7, "meta": {"pre_sell_qty": 15}, "pre_sell_qty": 15},
        current_balance_qty=8,
    )
    assert result["status"] == "US_FILLED_BY_BALANCE_DELTA"
    assert result["clear_pending_sell"] is True
    assert result["allow_residual_exit_recheck"] is True


def test_us_pending_sell_stale_released():
    from trader.us.execution.reconcile import reconcile_us_sell_ack_by_balance_delta
    result = reconcile_us_sell_ack_by_balance_delta(
        {"side": "SELL", "qty_requested": 7, "pre_sell_qty": 15, "created_at_ts": time.time() - 999},
        current_balance_qty=15,
        stale_after_sec=180,
    )
    assert result["status"] == "US_PENDING_SELL_STALE_RELEASED"
    assert result["clear_pending_sell"] is True


def test_us_hard_stop_after_partial_soft_stop_residual_exit():
    from trader.us.execution.reconcile import should_us_hard_stop_exit_residual
    result = should_us_hard_stop_exit_residual(residual_qty=8, pnl_pct=-0.12, hard_stop_pct=-0.10)
    assert result == {"should_exit": True, "qty": 8, "reason": "us_hard_stop_after_partial_soft_stop"}


def test_us_prep_quality_grade_c():
    from trader.us.runner.prep_runner import compute_us_prep_quality_grade
    assert compute_us_prep_quality_grade(universe_count=82, candidate_pool_count=53) == "US_PREP_QUALITY_C"


def test_route_order_us_buy_weight_clamp_enabled(monkeypatch):
    monkeypatch.setenv("US_BUY_WEIGHT_CLAMP_ENABLED", "1")
    monkeypatch.setenv("US_AGENT_ENABLED", "1")
    monkeypatch.setenv("TRADING_REGION", "US")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("US_PAPER_TRADING_ENABLED", "1")
    monkeypatch.delenv("US_ORDER_ARMED", raising=False)
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "0")
    monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "0.05")
    monkeypatch.setenv("US_MAX_ORDER_USD", "10000")
    monkeypatch.setenv("US_MAX_DAILY_NOTIONAL_USD", "10000")
    monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "0")
    from trader.us.execution.order_router import clear_sent_order_keys, route_order
    clear_sent_order_keys()
    result = route_order(
        {"symbol": "AAPL", "exchange": "NASD", "side": "BUY", "qty": 7, "price_usd": 10.0, "limit_price": 10.0, "notional_usd": 70.0, "client_order_key": "clamp-route-1"},
        total_portfolio_usd=1000.0,
        available_cash_usd=1000.0,
        allowed_symbols={"AAPL"},
    )
    assert result["status"] == "DRY_RUN"
    assert result["qty"] == 5
    assert result["intent"]["meta"]["us_buy_clamped_by_weight"] is True
