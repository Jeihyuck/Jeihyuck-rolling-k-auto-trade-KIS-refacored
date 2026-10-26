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


def test_filled_order_status_does_not_substitute_for_zero_native_execution():
    report = audit_sell_settlement(
        orders=[dict(order_id="tfe", code="425420", side="SELL", status="FILLED", qty=30)],
        fills=[],
        positions=[dict(position_cycle_id="cycle", status="CLOSED",
                        closed_reason="BROKER_ZERO_ECONOMIC_SETTLEMENT_PENDING")],
    )
    assert report["status"] == "ECONOMIC_SETTLEMENT_PENDING"
    assert report["gaps"][0]["missing_priced_sell_qty"] == 30


def test_cumulative_repeated_snapshots_are_not_double_counted():
    report = audit_sell_settlement(
        orders=[dict(order_id="k", side="SELL", qty=170, code="293490")],
        fills=[
            dict(order_id="k", side="SELL", qty=17, price=11292, evidence_kind="KIS_ORDER_CUMULATIVE_EXECUTION"),
            dict(order_id="k", side="SELL", qty=170, price=11288, evidence_kind="KIS_ORDER_CUMULATIVE_EXECUTION"),
        ],
        positions=[],
    )
    assert report["gap_count"] == 0
