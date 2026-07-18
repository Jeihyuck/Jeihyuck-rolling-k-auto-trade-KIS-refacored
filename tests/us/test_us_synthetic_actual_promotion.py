import trader.us.db.repos as repos
from trader.us.runner.daily_report_runner import load_us_fills_breakdown


def _seed():
    repos.reset_memory_stores(); repos.save_order_ack({"client_order_key":"K","symbol":"AMD","exchange":"NASDAQ","side":"SELL","qty_requested":10,"order_no":"O1","status":"ACK"}, trade_date="2026-07-16")


def test_kis_actual_promotes_equal_cumulative_synthetic():
    _seed(); repos.mark_order_filled_by_reconcile(order_no="O1",client_order_key="K",symbol="AMD",side="SELL",filled_qty=3,requested_qty=10,cumulative_filled_qty=3,avg_price_usd=100,trade_date="2026-07-16",evidence_type="BALANCE_DELTA_SYNTHETIC")
    r=repos.mark_order_filled_by_reconcile(order_no="O1",client_order_key="K",symbol="AMD",side="SELL",filled_qty=3,requested_qty=10,cumulative_filled_qty=3,avg_price_usd=100,trade_date="2026-07-16",evidence_type="KIS_ORDER_DETAIL_ACTUAL",source="fills_by_order_no")
    assert r["synthetic_superseded_count"] == 1 and any(not repos.is_synthetic_fill_meta(f["meta"]) for f in repos._MEM_FILLS)


def test_kis_actual_supersedes_prior_synthetic_partial():
    _seed(); repos.mark_order_filled_by_reconcile(order_no="O1",client_order_key="K",symbol="AMD",side="SELL",filled_qty=3,requested_qty=10,cumulative_filled_qty=3,avg_price_usd=100,trade_date="2026-07-16",evidence_type="BALANCE_DELTA_SYNTHETIC")
    r=repos.mark_order_filled_by_reconcile(order_no="O1",client_order_key="K",symbol="AMD",side="SELL",filled_qty=6,requested_qty=10,cumulative_filled_qty=6,avg_price_usd=100,trade_date="2026-07-16",evidence_type="KIS_ORDER_DETAIL_ACTUAL",source="fills_by_order_no")
    assert r["synthetic_superseded_count"] == 1 and repos._MEM_ORDERS[0]["qty_filled"] == 6


def test_actual_smaller_than_synthetic_is_quarantined():
    _seed(); repos.mark_order_filled_by_reconcile(order_no="O1",client_order_key="K",symbol="AMD",side="SELL",filled_qty=10,requested_qty=10,cumulative_filled_qty=10,avg_price_usd=100,trade_date="2026-07-16",evidence_type="BALANCE_DELTA_SYNTHETIC")
    r=repos.mark_order_filled_by_reconcile(order_no="O1",client_order_key="K",symbol="AMD",side="SELL",filled_qty=7,requested_qty=10,cumulative_filled_qty=7,avg_price_usd=100,trade_date="2026-07-16",evidence_type="KIS_ORDER_DETAIL_ACTUAL",source="fills_by_order_no")
    assert r["status"] == "EVIDENCE_QUANTITY_CONFLICT" and r["synthetic_cumulative"] == 10


def test_promoted_synthetic_is_not_counted_in_accounting():
    test_kis_actual_promotes_equal_cumulative_synthetic()
    active_synthetic=[f for f in repos._MEM_FILLS if repos.is_synthetic_fill_meta(f["meta"]) and f["meta"].get("accounting_active") is not False]
    assert active_synthetic == []
