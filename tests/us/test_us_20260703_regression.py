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


def _capture_success(captured, **kwargs):
    captured.append(kwargs)
    return {"status": "OK"}


def test_partial_sell_ack_balance_delta_confirm_via_reconcile_path(monkeypatch):
    import trader.us.execution.reconcile as reconcile
    import trader.us.db.repos as repos

    monkeypatch.setattr(repos, "load_pending_ack_orders", lambda trade_date, env="practice": [{
        "symbol": "FLEX",
        "side": "SELL",
        "order_no": "0000043867",
        "client_order_key": "FLEX-SELL-1",
        "qty_requested": 4,
        "avg_price_usd": 35.0,
        "meta": {"pre_sell_qty": 8},
    }])
    captured = []
    monkeypatch.setattr(repos, "mark_order_filled_by_reconcile", lambda **kwargs: _capture_success(captured, **kwargs))

    class Provider:
        def __init__(self):
            self.force_refresh_values = []

        def get_balance(self, force_refresh=False):
            self.force_refresh_values.append(force_refresh)
            return {"positions": [{"symbol": "FLEX", "qty": 4, "avg_price": 35.0}]}

        def get_fills_by_order_no(self, **kwargs):
            return {}

    provider = Provider()
    result = reconcile.reconcile_ack_orders_with_balance(provider=provider, trade_date="2026-07-02", env="practice")

    assert provider.force_refresh_values == [True]
    assert result["balance_reconcile_count"] == 1
    assert result["unresolved_count"] == 0
    assert "FLEX" in result["symbols_by_status"]["balance_confirmed"]
    assert captured[0]["source"] == "balance_reconcile_sell"
    assert captured[0]["meta"]["balance_delta_status"] == "BALANCE_CONFIRMED_SELL"


def test_reconcile_bypasses_stale_balance_cache_for_partial_sell(monkeypatch):
    import trader.us.execution.reconcile as reconcile
    import trader.us.db.repos as repos

    monkeypatch.setattr(repos, "load_pending_ack_orders", lambda trade_date, env="practice": [{
        "symbol": "FLEX",
        "side": "SELL",
        "order_no": "0000043867",
        "client_order_key": "FLEX-SELL-1",
        "qty_requested": 4,
        "avg_price_usd": 35.0,
        "meta": {"holding_qty": 8},
    }])
    captured = []
    monkeypatch.setattr(repos, "mark_order_filled_by_reconcile", lambda **kwargs: _capture_success(captured, **kwargs))

    class Provider:
        def __init__(self):
            self.cached_qty = 8
            self.fresh_qty = 4
            self.force_refresh_values = []

        def get_balance(self, force_refresh=False):
            self.force_refresh_values.append(force_refresh)
            qty = self.fresh_qty if force_refresh else self.cached_qty
            return {"positions": [{"symbol": "FLEX", "qty": qty, "avg_price": 35.0}]}

        def get_fills_by_order_no(self, **kwargs):
            return {}

    provider = Provider()
    result = reconcile.reconcile_ack_orders_with_balance(provider=provider, trade_date="2026-07-02", env="practice")

    assert provider.force_refresh_values == [True]
    assert result["balance_reconcile_count"] == 1
    assert result["unresolved_count"] == 0
    assert captured[0]["filled_qty"] == 4


def test_hard_stop_aliases_force_full_exit(monkeypatch):
    from trader.us.pb1.us_exit_engine import _make_exit_intent, EXIT_HARD_STOP_LOSS

    monkeypatch.setattr("trader.us.pb1.us_exit_engine.should_skip_exit_due_to_pending_sell", lambda *a, **k: (False, None, None))
    for alias in ["hard_stop", "hard_stop_loss", "hard_stop_full_exit", EXIT_HARD_STOP_LOSS]:
        intent = _make_exit_intent(
            "FLEX", "NASDAQ", 1, 90.0, 100.0, alias, "alias hard stop", -40.0, -0.10,
            holding_qty=8, orderable_qty=4,
        )
        assert intent["qty"] == 4
        assert intent["partial_allowed"] is False
        assert intent["exit_reason"] == "hard_stop_full_exit"
        assert intent["meta"]["partial_allowed"] is False


def test_full_sell_position_absent_fallback_still_confirms(monkeypatch):
    import trader.us.execution.reconcile as reconcile
    import trader.us.db.repos as repos

    monkeypatch.setattr(repos, "load_pending_ack_orders", lambda trade_date, env="practice": [{
        "symbol": "FLEX",
        "side": "SELL",
        "order_no": "0000043867",
        "client_order_key": "FLEX-SELL-FULL",
        "qty_requested": 4,
        "avg_price_usd": 35.0,
        "pre_order_position_qty": 4,
        "meta": {},
    }])
    captured = []
    monkeypatch.setattr(repos, "mark_order_filled_by_reconcile", lambda **kwargs: _capture_success(captured, **kwargs))

    class Provider:
        def get_balance(self, force_refresh=False):
            return {"positions": []}

        def get_fills_by_order_no(self, **kwargs):
            return {}

    result = reconcile.reconcile_ack_orders_with_balance(provider=Provider(), trade_date="2026-07-02", env="practice")

    assert result["balance_reconcile_count"] == 1
    assert result["unresolved_count"] == 0
    assert captured[0]["source"] in {"balance_reconcile_sell", "balance_reconcile_delta_sell"}
