from datetime import datetime, timezone

from trader.reconcile_kis import _promote_open_buy_orders_from_holdings


class _Orders:
    def __init__(self, holding_baseline=10, submitted_qty=2):
        self.last = None
        self.holding_baseline = holding_baseline
        self.submitted_qty = submitted_qty

    def get_open_orders(self, env):
        ts = datetime(2026, 9, 14, 1, 0, tzinfo=timezone.utc)
        return [{
            "side": "SELL",
            "status": "ACKED",
            "code": "010060",
            "qty": self.submitted_qty,
            "sid": 1,
            "mode": 1,
            "stage": "TP1",
            "client_order_key": "tp1-key",
            "position_cycle_id": "cycle-1",
            "portfolio_epoch_id": "epoch-1",
            "request_json": {
                "pre_order_holding_qty": self.holding_baseline,
                "submitted_qty": self.submitted_qty,
                "requested_qty": self.submitted_qty,
                "profit_capture_stage": "tp1",
            },
            "response_json": {},
            "submitted_at": ts,
            "acked_at": ts,
        }]

    def upsert_reconciled_order(self, **kwargs):
        self.last = kwargs


class _Fills:
    def __init__(self):
        self.calls = []

    def upsert_fill(self, **kwargs):
        self.calls.append(kwargs)


class _Positions:
    def __init__(self):
        self.calls = []

    def mark_profit_capture_fill(self, **kwargs):
        self.calls.append(kwargs)
        return True


def _run(current_qty):
    orders = _Orders()
    fills = _Fills()
    positions = _Positions()
    result = _promote_open_buy_orders_from_holdings(
        env="practice",
        strategy="pb1_pullback_close",
        ctx_run_id=None,
        tick_ts=datetime(2026, 9, 14, 1, 5, tzinfo=timezone.utc),
        holdings_rows=[{"pdno": "010060", "hldg_qty": str(current_qty), "pchs_avg_pric": "100"}],
        orders_repo=orders,
        fills_repo=fills,
        positions_repo=positions,
    )
    return orders, fills, positions, result


def test_full_tp_quantity_confirms_stage_even_when_sell_price_is_unresolved():
    orders, fills, positions, result = _run(current_qty=8)

    assert orders.last["status"] == "FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED"
    assert result["orders"] == 1
    assert result["fills"] == 0
    assert fills.calls == []
    assert len(positions.calls) == 1
    assert positions.calls[0]["stage"] == "tp1"
    assert positions.calls[0]["filled_qty"] == 2
    assert positions.calls[0]["position_cycle_id"] == "cycle-1"


def test_partial_quantity_without_price_stays_partial_and_does_not_advance_tp():
    orders, fills, positions, result = _run(current_qty=9)

    assert orders.last["status"] == "PARTIAL_FILLED"
    assert result["orders"] == 1
    assert result["fills"] == 0
    assert fills.calls == []
    assert positions.calls == []
