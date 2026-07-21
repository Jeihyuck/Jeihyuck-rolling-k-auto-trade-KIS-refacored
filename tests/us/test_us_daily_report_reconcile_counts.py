from trader.us.runner.daily_report_runner import reconcile_order_sources


def test_actual_fills_reconcile_padded_ack_aggregate_without_source_mismatch():
    result = reconcile_order_sources(db_orders=8, fills=8, balance_confirmed=0, router_summary=8)
    assert result["orders_ack"] == 8
    assert result["fill_api_count"] == 8
    assert result["broker_reconciled"] is True
    assert "SOURCE_MISMATCH" not in result["warnings"]
