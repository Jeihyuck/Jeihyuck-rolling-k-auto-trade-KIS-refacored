from trader.us.runner.trade_session_runner import attribute_session_fills


def test_failed_session_can_report_confirmed_fill():
    orders = [{"session": "am", "session_run_id": "am-1", "canonical_order_no": "40604"}]
    fills = [{"order_no": "40604", "filled_qty": 1}, {"order_no": "99999", "filled_qty": 2}]
    result = attribute_session_fills(orders, fills, session="am", session_run_id="am-1")
    assert result == {"session_fills_count": 1, "unresolved_order_count": 0}
