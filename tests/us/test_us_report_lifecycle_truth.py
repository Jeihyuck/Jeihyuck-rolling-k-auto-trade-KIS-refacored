from trader.us.runner.daily_report_runner import _order_lifecycle_truth


def test_us_report_separates_ack_open_partial_fill_and_fill():
    assert _order_lifecycle_truth("ACK", {}) == {
        "acknowledged": True, "open": False, "partially_filled": False,
        "filled": False, "cancelled": False,
    }
    assert _order_lifecycle_truth("OPEN", {})["open"] is True
    assert _order_lifecycle_truth("PARTIALLY_FILLED", {})["partially_filled"] is True
    assert _order_lifecycle_truth("FILLED", {})["filled"] is True


def test_us_report_cancel_ack_is_not_cancelled():
    assert _order_lifecycle_truth("ACK", {"cancel_acknowledged_at": "now"})["cancelled"] is False

