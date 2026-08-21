from trader.us.execution import reconcile
from trader.us.runner.daily_report_runner import reconcile_order_sources


class CancelProvider:
    def get_balance(self, force_refresh=False):
        return {"positions": []}
    def get_fills_by_order_no(self, **_kwargs):
        return {"status": "cancel_complete", "filled_qty": 0, "remaining_qty": 0,
                "symbol": "AAPL", "side": "SELL", "order_no": "O1"}


def test_manual_cancel_is_terminal_and_not_unresolved(monkeypatch):
    order = {"symbol": "AAPL", "side": "SELL", "order_no": "O1", "client_order_key": "k",
             "qty_requested": 2, "meta": {"pre_order_holding_qty": 2}}
    monkeypatch.setattr("trader.us.db.repos.load_pending_ack_orders", lambda **_: [order])
    monkeypatch.setattr("trader.us.db.repos.apply_broker_order_observation", lambda **_: {"status": "OK"})
    result = reconcile.reconcile_ack_orders_with_balance(provider=CancelProvider(), trade_date="2026-08-21")
    assert result["status"] == "OK"
    assert result["canceled_count"] == 1
    assert result["unresolved_count"] == 0


def test_canceled_order_db_fill_mismatch_is_warning_not_failure():
    result = reconcile_order_sources(db_orders=2, fills=1, balance_confirmed=1, router_summary=1,
                                     canceled_orders=1, broker_pending=0,
                                     fills_query_ok=True, balance_snapshot_ok=True)
    assert result["db_orders"] == 1
    assert result["canceled_orders"] == 1
    assert result["consistency"] in {"BROKER_RECONCILED", "OK", "OK_WITH_WARNINGS"}


def test_same_day_defense_trim_semantic_duplicate_includes_canceled(monkeypatch):
    from trader.us.execution.order_router import same_day_semantic_sell_exists
    prior = {"symbol": "SOXL", "side": "SELL", "status": "CANCELLED",
             "strategy_owner": "US_STANDARD", "reason": "DEFENSE_RISK_OFF_TRIM",
             "meta": {"position_lifecycle_id": "life-1"}}
    monkeypatch.setattr("trader.us.db.repos.load_us_daily_orders_for_report", lambda _date: [prior])
    intent = {"symbol": "SOXL", "side": "SELL", "trade_date": "2026-08-21",
              "strategy_owner": "US_STANDARD", "reason": "DEFENSE_RISK_OFF_TRIM",
              "meta": {"position_lifecycle_id": "life-1"}}
    assert same_day_semantic_sell_exists(intent)
