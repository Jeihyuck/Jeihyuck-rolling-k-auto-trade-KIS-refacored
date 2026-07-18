import trader.us.db.repos as repos


def _order():
    repos.reset_memory_stores()
    repos.save_order_ack({"client_order_key":"K","symbol":"AMD","exchange":"NASDAQ","side":"SELL","qty_requested":10,"order_no":"O1","status":"ACK"}, trade_date="2026-07-16")


def test_equal_qty_equal_price_partial_fills_do_not_collide():
    _order()
    repos.mark_order_filled_by_reconcile(order_no="O1",client_order_key="K",symbol="AMD",side="SELL",filled_qty=3,requested_qty=10,cumulative_filled_qty=3,avg_price_usd=100,trade_date="2026-07-16",evidence_type="KIS_ORDER_DETAIL_ACTUAL",source="fills_by_order_no")
    repos.mark_order_filled_by_reconcile(order_no="O1",client_order_key="K",symbol="AMD",side="SELL",filled_qty=6,requested_qty=10,cumulative_filled_qty=6,avg_price_usd=100,trade_date="2026-07-16",evidence_type="KIS_ORDER_DETAIL_ACTUAL",source="fills_by_order_no")
    assert repos._MEM_ORDERS[0]["qty_filled"] == 6
    assert sum(f["qty"] for f in repos._MEM_FILLS) == 6
    assert len({f["fill_idempotency_key"] for f in repos._MEM_FILLS}) == 2


def test_fill_idempotency_includes_cumulative_quantity():
    a={"symbol":"AMD","side":"SELL","order_no":"O1","client_order_key":"K","qty":3,"price_usd":100,"meta":{"fill_evidence_type":"KIS_ORDER_DETAIL_ACTUAL","cumulative_filled_qty":3}}
    b={**a,"meta":{"fill_evidence_type":"KIS_ORDER_DETAIL_ACTUAL","cumulative_filled_qty":6}}
    assert repos._us_fill_idempotency_key_text(a,"2026-07-16") != repos._us_fill_idempotency_key_text(b,"2026-07-16")
