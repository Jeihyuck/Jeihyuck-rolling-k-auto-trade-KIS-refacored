from trader.us.pb1.us_entry_engine import _risk_clamp_new_buy_size, _validate_new_buy_explain_contract
from trader.us.pb1.us_exit_engine import evaluate_exit
from trader.us.execution.reconcile import confirm_order_by_balance_delta


def test_risk_aware_buy_sizing_clamps_to_max_position_weight():
    sized = _risk_clamp_new_buy_size(
        symbol="TXN",
        account_equity=30000,
        max_position_weight=0.05,
        current_symbol_market_value=0,
        signal_target_notional=3300,
        price=200,
        remaining_buy_budget=10000,
    )
    assert sized["allowed_notional"] == 1500
    assert sized["final_notional"] <= 1500
    assert sized["final_qty"] == 7
    assert "skip_reason" not in sized


def test_insufficient_capacity_after_risk_clamp_skips():
    sized = _risk_clamp_new_buy_size(
        symbol="META",
        account_equity=30000,
        max_position_weight=0.05,
        current_symbol_market_value=1450,
        signal_target_notional=3300,
        price=200,
        remaining_buy_budget=10000,
    )
    assert sized["allowed_notional"] == 50
    assert sized["final_qty"] == 0
    assert sized["skip_reason"] == "INSUFFICIENT_CAPACITY_AFTER_RISK_CLAMP"


def test_hard_stop_full_exit(monkeypatch):
    monkeypatch.setattr("trader.us.pb1.us_exit_engine.should_skip_exit_due_to_pending_sell", lambda *a, **k: (False, None, None))
    intent = evaluate_exit({"symbol": "FLEX", "qty": 4, "orderable_qty": 4, "entry_price": 100, "max_price": 100}, 90)
    assert intent["qty"] == 4
    assert intent["partial_allowed"] is False
    assert intent["exit_reason"] == "hard_stop_full_exit"


def test_balance_confirmed_sell_delta():
    result = confirm_order_by_balance_delta("SELL", order_qty=4, pre_qty=8, post_qty=4)
    assert result["status"] == "BALANCE_CONFIRMED_SELL"
    assert result["pending"] is False
    assert result["remaining_qty"] == 0


def test_duplicate_sell_suppression(monkeypatch):
    from trader.us.pb1.us_exit_engine import _make_exit_intent
    monkeypatch.setattr("trader.us.pb1.us_exit_engine.should_skip_exit_due_to_pending_sell", lambda *a, **k: (True, "recent_sell_ack_exists", {"order_no": "ACK1"}))
    intent = _make_exit_intent("BE", "NASDAQ", 1, 100, 110, "soft_stop_loss", "new_exit_signal", -10, -0.1, holding_qty=1, orderable_qty=1)
    assert intent is None


def test_entry_explain_contract_blocks_skip_and_zero_scores():
    ok, reason = _validate_new_buy_explain_contract(
        "GLW",
        {"entry_style": "SKIP", "breakout_score": 0, "pullback_score": 0, "momentum_score": 0},
        "SKIP",
    )
    assert ok is False
    assert reason == "ENTRY_EXPLAIN_CONTRACT_ERROR"


def test_daily_final_report_payload_uses_distinct_orders(tmp_path, monkeypatch):
    from trader.us.runner.trade_session_runner import _write_us_session_report
    monkeypatch.chdir(tmp_path)
    payload = {"trade_date": "2026-07-02", "session": "daily_final", "real_broker_sells": 10, "real_broker_buys": 0}
    _write_us_session_report(payload, session="close")
    import json
    saved = json.loads((tmp_path / "reports/us_daily/latest_us_daily_report.json").read_text())
    assert saved["session"] in {"daily_final", "close"}
    assert saved["real_broker_sells"] == 10
