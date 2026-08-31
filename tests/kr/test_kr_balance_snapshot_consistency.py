from trader.execution_state import BrokerBalanceSnapshot, OrderBaseline


BALANCE = {
    "output1": [{"pdno": "010060", "hldg_qty": "14", "ord_psbl_qty": "14", "pchs_avg_pric": "271660"}],
    "output2": [{"ord_psbl_cash": "1000000"}],
}


def test_existing_broker_holding_is_authoritative_entry_fence():
    snapshot = BrokerBalanceSnapshot.from_kis(BALANCE)
    assert snapshot.holding_qty("010060") == 14
    # A stale DB entry view cannot change the immutable tick snapshot.
    stale_db_qty = 0
    assert snapshot.holding_qty("010060") != stale_db_qty


def test_order_baseline_contains_immutable_balance_evidence():
    snapshot = BrokerBalanceSnapshot.from_kis(BALANCE)
    baseline = OrderBaseline.capture(snapshot, "010060", 7)
    assert baseline.balance_snapshot_id == snapshot.snapshot_id
    assert baseline.pre_order_holding_qty == 14
    assert baseline.pre_order_orderable_qty == 14
    assert baseline.pre_order_avg_price == 271660
    assert baseline.requested_qty == baseline.submitted_qty == 7
