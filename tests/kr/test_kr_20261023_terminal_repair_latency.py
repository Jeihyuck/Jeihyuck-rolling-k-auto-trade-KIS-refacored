"""2026-10-23 KR terminal holdings recovery hot-path regression.

All tests are isolated and do not touch KIS or the operational Supabase DB.
"""
from datetime import datetime, timezone

from trader.reconcile_kis import _promote_open_buy_orders_from_holdings


class _TerminalOnlyOrders:
    def __init__(self, terminal_orders):
        self.terminal_orders = terminal_orders
        self.updates = []

    def get_open_orders(self, env):
        return []

    def list_recent_holdings_promoted_orders_for_repair(self, env, **kwargs):
        assert kwargs["lookback_days"] == 35
        return self.terminal_orders

    def upsert_reconciled_order(self, **kwargs):
        self.updates.append(kwargs)
        raise AssertionError("historic drift cannot write economic state")


def test_128_historical_holding_drift_candidates_never_call_cutover_db(monkeypatch):
    """Previously incurred one settlement cutover DB read per irrelevant row."""
    import trader.settlement.kr_cutover as cutover

    calls = []

    def read_scope(*args, **kwargs):
        calls.append(kwargs)
        raise AssertionError("drifted terminal order must be pruned first")

    monkeypatch.setattr(cutover, "load_kr_runtime_release_for_scope", read_scope)
    now = datetime(2026, 10, 23, 10, tzinfo=timezone.utc)
    terminal = [{
        "order_id": f"old-ord-{i}",
        "client_order_key": f"old-claim-{i}",
        "env": "practice", "strategy": "pb1_pullback_close",
        "code": "066570", "side": "SELL",
        "status": "FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED",
        "qty": 5, "created_at": now,
        "trading_epoch_id": "test-epoch",
        "response_json": {
            "promotion_source": "kis_holdings", "confirmed_fill_qty": 5,
            "pre_order_holding_qty": 5, "holding_qty": 0,
        },
    } for i in range(128)]
    repo = _TerminalOnlyOrders(terminal)
    # KIS current holding is nonzero; it cannot validate this historical
    # terminal observation. The order remains untouched/unresolved.
    result = _promote_open_buy_orders_from_holdings(
        env="practice", strategy="pb1_pullback_close", ctx_run_id=None,
        tick_ts=now,
        holdings_rows=[{"pdno": "066570", "hldg_qty": "3", "pchs_avg_pric": "200000"}],
        orders_repo=repo, fills_repo=object(), positions_repo=None,
    )
    assert result == {"orders": 0, "fills": 0, "codes": []}
    assert repo.updates == []
    assert calls == []


def test_terminal_recovery_with_invalid_saved_quantity_skips_cutover(monkeypatch):
    import trader.settlement.kr_cutover as cutover

    calls = []
    monkeypatch.setattr(
        cutover, "load_kr_runtime_release_for_scope",
        lambda *a, **k: calls.append(k),
    )
    now = datetime(2026, 10, 23, 10, tzinfo=timezone.utc)
    repo = _TerminalOnlyOrders([{
        "order_id": "bad-qty", "code": "053800", "side": "BUY",
        "status": "FILLED", "qty": 11, "created_at": now,
        "trading_epoch_id": "test-epoch",
        "response_json": {
            "promotion_source": "kis_holdings", "confirmed_fill_qty": 12,
            "pre_order_holding_qty": 0, "holding_qty": 12,
        },
    }])
    result = _promote_open_buy_orders_from_holdings(
        env="practice", strategy="pb1_pullback_close", ctx_run_id=None,
        tick_ts=now,
        holdings_rows=[{"pdno": "053800", "hldg_qty": "12", "pchs_avg_pric": "90308"}],
        orders_repo=repo, fills_repo=object(), positions_repo=None,
    )
    assert result["fills"] == result["orders"] == 0
    assert calls == []
