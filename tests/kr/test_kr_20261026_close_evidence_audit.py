from scripts.kr_close_settlement_audit import audit_sell_settlement

def test_missing_native_fill_is_reported():
    report = audit_sell_settlement(
        orders=[dict(order_id="o", code="293490", side="SELL", qty=170)],
        fills=[dict(order_id="o", side="SELL", qty=17, price=11292,
                    evidence_kind="KIS_ORDER_CUMULATIVE_EXECUTION")],
        positions=[],
    )
    assert report["gap_count"] == 1
    assert report["gaps"][0]["missing_priced_sell_qty"] == 153
