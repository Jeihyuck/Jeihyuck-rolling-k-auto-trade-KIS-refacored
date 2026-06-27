def test_pending_order_count_uses_ack_reconcile_unresolved_formula():
    tick = {"orders_ack": 6, "fills_api_count": 4, "ack_reconcile_balance_reconcile_count": 2, "ack_reconcile_unresolved_count": 0, "pending_order_count": 0}
    assert tick["orders_ack"] == 6
    assert tick["fills_api_count"] == 4
    assert tick["ack_reconcile_balance_reconcile_count"] == 2
    assert tick["pending_order_count"] == tick["ack_reconcile_unresolved_count"] == 0
