from trader.us.runner.trade_session_utils import attribute_session_fills


def test_trade_session_utils_attribute_session_fills():
    orders = [{"session": "am", "session_run_id": "am-1", "canonical_order_no": "40604"}]
    fills = [{"order_no": "40604", "filled_qty": 1}, {"order_no": "99999", "filled_qty": 2}]
    assert attribute_session_fills(orders, fills, session="am", session_run_id="am-1") == {
        "session_fills_count": 1,
        "unresolved_order_count": 0,
    }
