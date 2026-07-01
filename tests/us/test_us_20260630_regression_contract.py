from trader.us.runner.status_contract import classify_tick_status
from trader.us.runner.daily_report_runner import reconcile_order_sources


def test_inner_ok_not_overridden_by_late_timeout_contract():
    result = {"status": "OK_NO_TRADE", "reason": "", "late_timeout_signal_ignored": 1}
    assert classify_tick_status(result) == "success"


def test_single_tick_timeout_is_warning_not_fatal():
    result = {"status": "WARN_TICK_TIMEOUT", "reason": "tick_timeout", "timeout_sec": 150}
    assert classify_tick_status(result) == "warning"


def test_three_consecutive_tick_timeouts_are_fatal():
    result = {"status": "FAILED_CONSECUTIVE_TICK_TIMEOUT", "reason": "FAILED_CONSECUTIVE_TICK_TIMEOUT"}
    assert classify_tick_status(result) == "fatal"


def test_fills_500_degraded_status_continues_session():
    result = {"status": "DEGRADED_FILLS_UNAVAILABLE", "reason": "TEMP_FILLS_UNAVAILABLE"}
    assert classify_tick_status(result) == "success"


def test_daily_report_uses_max_source_and_flags_reconcile_warning():
    rec = reconcile_order_sources(db_orders=10, fills=13, balance_confirmed=0, router_summary=0)
    assert rec["orders_ack"] == 13
    assert "SOURCE_MISMATCH" in rec["warnings"]
    assert "SOURCE_MISMATCH_DB_ORDER_EXISTS_ROUTER_SUMMARY_MISSING" in rec["warnings"]
    assert "SOURCE_MISMATCH_FILLS_EXIST_BALANCE_CONFIRMATION_MISSING" in rec["warnings"]


def test_later_zero_router_summary_does_not_overwrite_am_ack_count():
    rec = reconcile_order_sources(db_orders=10, fills=0, balance_confirmed=0, router_summary=0)
    assert rec["orders_ack"] == 10
    assert "SOURCE_MISMATCH_SESSION_SUMMARY_OVERWRITTEN_OR_MISSING" in rec["warnings"]


def test_full_position_guard_and_liveness_markers_present():
    session_src = open("trader/us/runner/trade_session_runner.py", encoding="utf-8").read()
    tick_src = open("trader/us/runner/trade_tick_runner.py", encoding="utf-8").read()
    assert "US_CAPITAL][CAPACITY" in tick_src
    assert "available_new_slots" in tick_src
    assert "TICK_WARN_TIMEOUT" in session_src
    assert "SESSION_FINALLY" in session_src


def test_sold_today_unconfirmed_design_marker_present():
    # sold_today must be auditable; current contract keeps the report/source fields visible.
    src = open("trader/us/runner/trade_tick_runner.py", encoding="utf-8").read()
    assert "sold_today_symbols" in src


def test_capital_deployment_underdeployed_full_position_action():
    from trader.us.capital_deployment import compute_deployment_metrics, decide_deployment_action
    metrics = compute_deployment_metrics(account_equity_usd=200_000, invested_market_value_usd=50_000, cash_usd=150_000)
    assert metrics["underdeployed"] is True
    assert decide_deployment_action(metrics, position_count=35, max_positions=35) == "ADD_TO_EXISTING_ONLY"


def test_capital_deployment_overdeployed_trim_only():
    from trader.us.capital_deployment import compute_deployment_metrics, decide_deployment_action
    metrics = compute_deployment_metrics(account_equity_usd=100_000, invested_market_value_usd=90_000, cash_usd=10_000)
    assert metrics["overdeployed"] is True
    assert decide_deployment_action(metrics, position_count=20, max_positions=35) == "TRIM_ONLY"


def test_session_runner_uses_dt_for_report_recorded_at():
    src = open("trader/us/runner/trade_session_runner.py", encoding="utf-8").read()
    assert "from datetime import datetime as dt, timedelta" in src
    assert '"report_recorded_at_utc": dt.utcnow().isoformat() + "Z"' in src


def test_daily_report_uses_us_order_repo_not_common_orders():
    src = open("trader/us/runner/daily_report_runner.py", encoding="utf-8").read()
    assert "load_us_daily_orders_for_report" in src
    assert "from orders" not in src.lower()


def test_exit_reason_canonical_contract_markers():
    src = open("trader/us/pb1/us_exit_engine.py", encoding="utf-8").read()
    assert 'return "trailing_stop", exit_reason_detail' in src
    assert 'return "hard_stop", exit_reason_detail' in src
    assert '"exit_reason_detail": exit_reason_detail' in src


def test_full_position_new_symbols_blocked_but_add_allowed_markers():
    src = open("trader/us/pb1/us_entry_engine.py", encoding="utf-8").read()
    assert "allow_new_symbols" in src
    assert "price_lookup_skipped" in src
    assert "allow_add_to_existing" in src
    assert "ADD_TO_EXISTING_BUY" in src


def test_daily_report_calls_us_daily_order_loader_not_load_us_orders():
    src = open("trader/us/runner/daily_report_runner.py", encoding="utf-8").read()
    assert "all_orders_today = load_us_daily_orders_for_report(trade_date)" in src
    assert "load_us_orders(" not in src


def test_hold_intent_has_canonical_exit_reason_without_name_error():
    from trader.us.pb1.us_exit_engine import _make_hold_intent, EXIT_SOFT_STOP_LOSS
    hold = _make_hold_intent("AAPL", EXIT_SOFT_STOP_LOSS, "soft_stop_wait_confirm", -0.04, 1, 2)
    assert hold["side"] == "HOLD"
    assert hold["exit_reason"] == EXIT_SOFT_STOP_LOSS
    assert hold["exit_reason_detail"] == EXIT_SOFT_STOP_LOSS


def test_full_position_fallback_path_skips_before_any_price_lookup(monkeypatch):
    from trader.us.pb1.us_entry_engine import generate_entry_intents

    class Provider:
        def __init__(self):
            self.daily_calls = 0
            self.price_calls = 0
        def get_daily_prices(self, *args, **kwargs):
            self.daily_calls += 1
            raise AssertionError("daily price lookup should be skipped")
        def get_current_price(self, *args, **kwargs):
            self.price_calls += 1
            raise AssertionError("current price lookup should be skipped")

    provider = Provider()
    intents = generate_entry_intents(
        ["AAPL"], provider, set(), 10_000, 35, 100_000,
        current_position_symbols=set(), allow_new_symbols=False, available_new_slots=0,
    )
    assert intents == []
    assert provider.daily_calls == 0
    assert provider.price_calls == 0


def test_add_to_existing_intent_carries_capital_deployment_fields(monkeypatch):
    from trader.us.pb1.us_entry_engine import generate_entry_intents

    monkeypatch.setenv("US_ADD_MIN_PNL_PCT", "0.02")
    monkeypatch.setenv("US_MAX_ORDER_USD", "10000")
    monkeypatch.setenv("US_TARGET_POSITION_WEIGHT", "0.025")
    monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "0.05")

    class Provider:
        positions = [{
            "symbol": "AAPL", "qty": 10, "avg_price": 100.0,
            "market_value_usd": 1100.0, "unrealized_pnl_pct": 0.10,
        }]
        def get_current_price(self, symbol, exchange):
            return {"last": 110.0}

    intents = generate_entry_intents(
        ["AAPL"], Provider(), set(), 10_000, 35, 100_000,
        watchlist_entries=[{"symbol": "AAPL", "exchange": "NASDAQ", "score": 0.9}],
        current_position_symbols={"AAPL"}, allow_new_symbols=False, allow_add_to_existing=True, available_new_slots=0,
    )
    assert intents
    intent = intents[0]
    assert intent["position_action"] == "ADD_TO_EXISTING_BUY"
    assert intent["current_position_market_value_usd"] == 1100.0
    assert intent["current_weight"] is not None
    assert intent["projected_weight"] is not None
    assert intent["meta"]["capital_deployment"]["deployment_action"] == "ADD_TO_EXISTING_BUY"


def test_risk_gate_blocks_projected_weight_from_meta(monkeypatch):
    from trader.us.execution.risk_gate import RiskGateBlocked, assert_order_allowed
    import pytest

    monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "0.05")
    monkeypatch.setenv("US_MAX_ORDER_USD", "10000")
    monkeypatch.setenv("US_MAX_DAILY_NOTIONAL_USD", "10000")
    monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "0")
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("US_ORDER_ARMED", "1")
    monkeypatch.setenv("US_KIS_ORDER_ALLOWED", "1")
    monkeypatch.setenv("DISABLE_LIVE_TRADING", "0")
    monkeypatch.setenv("DISABLE_REAL_TRADING", "0")
    monkeypatch.setenv("ALLOW_REAL_ORDER", "1")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "1")
    monkeypatch.setenv("US_LIVE_TRADING_ENABLED", "1")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("TRADING_REGION", "US")
    intent = {
        "symbol": "AAPL", "exchange": "NASDAQ", "side": "BUY", "qty": 1,
        "notional_usd": 100.0, "client_order_key": "k",
        "position_action": "ADD_TO_EXISTING_BUY",
        "meta": {"capital_deployment": {"projected_weight": 0.06}},
    }
    with pytest.raises(RiskGateBlocked) as exc:
        assert_order_allowed(
            intent, current_daily_notional_usd=0, current_position_count=35,
            total_portfolio_usd=100_000, available_cash_usd=50_000,
            is_existing_position_buy=True, allowed_symbols={"AAPL"}, trade_date="2026-06-30",
        )
    assert "position_weight_exceeded" in str(exc.value)
