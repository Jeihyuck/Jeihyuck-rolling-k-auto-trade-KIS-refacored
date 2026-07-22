import json

from trader.us.execution import reconcile
from trader.us.runner.trade_tick_runner import (
    _prior_failed_orders_require_reconcile_only,
    _suppress_pending_sell_exit_intents,
    run_trade_tick,
)


def test_prior_failed_ack_session_requires_reconcile_only(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    health = tmp_path / "reports/us_schedule_health"
    health.mkdir(parents=True)
    (health / "2026-07-21.json").write_text(json.dumps({"sessions": {
        "am": {"effective_status": "FAILED", "reason": "fill_persistence_failed_after_orders_sent", "orders_ack": 10}
    }}))

    assert _prior_failed_orders_require_reconcile_only("2026-07-21", "afternoon") is True


def test_reconcile_only_tick_does_not_route_and_clean_marker_releases_next_tick(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    health = tmp_path / "reports/us_schedule_health"
    health.mkdir(parents=True)
    (health / "2026-07-21.json").write_text(json.dumps({"sessions": {
        "am": {"effective_status": "FAILED", "reason": "fill_persistence_failed_after_orders_sent", "orders_ack": 10}
    }}))

    class Provider:
        def _get_client(self):
            return type("Client", (), {"stats": {}})()
        def get_orderable_cash(self, **_kwargs):
            return 10000.0

    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=False: Provider())
    monkeypatch.setattr("trader.us.budget.resolve_us_order_budget", lambda cash: {"effective_order_budget_usd": cash})
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_positions", lambda **_kwargs: {
        "status": "OK", "balance_fetch_status": "OK", "authoritative_positions": True,
        "preserve_previous_positions": False, "positions": [], "position_count": 0,
        "position_symbols": [], "block_new_entry": False,
    })
    monkeypatch.setattr("trader.us.execution.fills.get_fills_today", lambda **_kwargs: {"status": "OK", "fills": []})
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_ack_orders_with_balance", lambda **_kwargs: {
        "status": "OK", "pending_count": 0, "unresolved_count": 0, "symbols_by_status": {},
    })
    monkeypatch.setattr("trader.us.db.repos.save_position_snapshot", lambda *_args, **_kwargs: None)
    route_calls = []
    monkeypatch.setattr("trader.us.runner.trade_tick_runner.route_exit_orders_immediately", lambda *_args, **_kwargs: route_calls.append(1))

    result = run_trade_tick(session="afternoon", force_now="2026-07-21T10:00:00-04:00", tick_index=2)
    assert result["status"] == "OK_RECONCILE_ONLY_CLEAN"
    assert route_calls == []
    assert _prior_failed_orders_require_reconcile_only("2026-07-21", "afternoon") is False


def test_clean_marker_does_not_release_unresolved_ack(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    marker = tmp_path / "runtime/health"
    marker.mkdir(parents=True)
    (marker / "us-2026-07-21.json").write_text(json.dumps({"sessions": {
        "afternoon": {"reconcile_only_clean": 1, "pending_ack_count": 0,
                      "unresolved_ack_count": 3, "manual_reconcile_required": 1}
    }}))
    assert _prior_failed_orders_require_reconcile_only("2026-07-21", "close") is True


def test_pending_sell_suppresses_all_exit_intent_types(monkeypatch):
    monkeypatch.setattr(
        "trader.us.db.repos.has_pending_order_for_symbol_side",
        lambda **kwargs: kwargs["symbol"] == "PLTR",
    )
    intents = [{"symbol": "PLTR", "side": "SELL", "reason": reason} for reason in (
        "profit_capture", "trailing_stop", "cluster_exposure_trim", "DEFENSE_RISK_OFF_TRIM"
    )]
    assert _suppress_pending_sell_exit_intents(intents, "2026-07-21") == []


def test_close_balance_absent_sell_is_not_unresolved(monkeypatch):
    orders = [{
        "symbol": "AAPL", "side": "SELL", "qty_requested": 1, "status": "ACK",
        "order_no": "0000041216", "client_order_key": "aapl-sell",
    }]
    monkeypatch.setattr(reconcile, "_get_balance_force_refresh", lambda provider: {"positions": []})
    monkeypatch.setattr(reconcile, "validate_reconcile_identity", lambda **kwargs: {"status": "OK"})
    result = reconcile.classify_ack_orders_with_final_balance(provider=object(), trade_date="2026-07-21", orders=orders)
    assert result["pending_order_count"] == 0
    assert result["orders"][0]["final_status"] == "position_absent_confirmed_sell"


def test_close_balance_absent_unaccepted_sell_remains_unresolved(monkeypatch):
    orders = [{"symbol": "PLTR", "side": "SELL", "qty_requested": 1, "status": "SUBMITTED",
               "order_no": "0000041212", "client_order_key": "pltr-sell"}]
    monkeypatch.setattr(reconcile, "_get_balance_force_refresh", lambda provider: {"positions": []})
    monkeypatch.setattr(reconcile, "validate_reconcile_identity", lambda **kwargs: {"status": "OK"})
    result = reconcile.classify_ack_orders_with_final_balance(provider=object(), trade_date="2026-07-21", orders=orders)
    assert result["pending_order_count"] == 1
    assert result["orders"][0]["final_status"] == "ack_only_unresolved"
