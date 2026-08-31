from trader.execution_state import OrderState, reconcile_balance_delta
from trader.reconcile_kis import _promote_open_buy_orders_from_holdings
from datetime import datetime


def test_buy_balance_delta_confirms_fill():
    result = reconcile_balance_delta(side="BUY", pre_qty=14, post_qty=21, submitted_qty=7)
    assert result.state is OrderState.FILLED
    assert result.confirmed_fill_qty == 7


def test_sell_balance_delta_confirms_fill_and_remaining_position():
    result = reconcile_balance_delta(side="SELL", pre_qty=21, post_qty=7, submitted_qty=14)
    assert result.state is OrderState.FILLED
    assert result.confirmed_fill_qty == 14
    assert 21 - result.confirmed_fill_qty == 7


def test_ack_is_not_fill_without_proven_delta():
    result = reconcile_balance_delta(side="BUY", pre_qty=14, post_qty=14, submitted_qty=7)
    assert result.state is OrderState.UNRESOLVED_ACK
    assert result.confirmed_fill_qty == 0


def test_impossible_delta_is_reconcile_error():
    result = reconcile_balance_delta(side="BUY", pre_qty=14, post_qty=22, submitted_qty=7)
    assert result.state is OrderState.RECONCILE_ERROR


class _Orders:
    def __init__(self, response):
        self.response = response
        self.saved = []

    def get_open_orders(self, _env):
        return [{"order_id": "o1", "side": "SELL", "status": "ACKED", "code": "010060",
                 "qty": 14, "ord_type": "MARKET", "client_order_key": "sell-1",
                 "request_json": {"pre_order_holding_qty": 21, "submitted_qty": 14},
                 "response_json": self.response}]

    def upsert_reconciled_order(self, **kwargs):
        self.saved.append(kwargs)


class _Fills:
    def __init__(self): self.saved = []
    def upsert_fill(self, **kwargs): self.saved.append(kwargs)


def _reconcile_sell(response):
    orders, fills = _Orders(response), _Fills()
    result = _promote_open_buy_orders_from_holdings(
        env="practice", strategy="pb1_pullback_close", ctx_run_id=None,
        tick_ts=datetime(2026, 8, 31, 13, 0),
        holdings_rows=[{"pdno": "010060", "hldg_qty": "7", "pchs_avg_pric": "271660"}],
        orders_repo=orders, fills_repo=fills,
    )
    return result, orders, fills


def test_sell_qty_confirmed_without_execution_price_does_not_fabricate_fill():
    result, orders, fills = _reconcile_sell({})
    assert result["fills"] == 0
    assert fills.saved == []
    assert orders.saved[-1]["status"] == "FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED"
    assert orders.saved[-1]["response_json"]["confirmed_fill_qty"] == 14
    assert orders.saved[-1]["response_json"]["confirmed_fill_price"] is None
    assert orders.saved[-1]["response_json"]["realized_pnl_status"] == "REALIZED_PNL_UNRESOLVED"


def test_sell_uses_actual_broker_execution_price():
    result, orders, fills = _reconcile_sell({"execution_detail": {"ccld_unpr": "266700"}})
    assert result["fills"] == 1
    assert fills.saved[-1]["qty"] == 14
    assert fills.saved[-1]["price"] == 266700
    assert fills.saved[-1]["fill_meta_json"]["fill_source"] == "BROKER_EXECUTION"
    assert fills.saved[-1]["fill_meta_json"]["price_confirmed"] is True
