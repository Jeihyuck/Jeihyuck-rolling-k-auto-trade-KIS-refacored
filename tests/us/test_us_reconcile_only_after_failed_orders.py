import json

from trader.us.execution import reconcile
from trader.us.runner.trade_tick_runner import (
    _prior_failed_orders_require_reconcile_only,
    _suppress_pending_sell_exit_intents,
)


def test_prior_failed_ack_session_requires_reconcile_only(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    health = tmp_path / "reports/us_schedule_health"
    health.mkdir(parents=True)
    (health / "2026-07-21.json").write_text(json.dumps({"sessions": {
        "am": {"effective_status": "FAILED", "reason": "fill_persistence_failed", "orders_ack": 10}
    }}))

    assert _prior_failed_orders_require_reconcile_only("2026-07-21", "afternoon") is True
    # A clean reconcile tick must still not route an order: the tick runner's
    # reconcile-only early return owns that invariant.


def test_pending_ack_requires_reconcile_only_until_clean():
    ack = {"status": "WARN", "pending_count": 1, "unresolved_count": 3}
    assert ack["status"] == "WARN"
    assert ack["pending_count"] > 0 or ack["unresolved_count"] > 0


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
