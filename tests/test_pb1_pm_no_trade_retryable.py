from datetime import datetime
from trader.pb1_runner import should_skip_duplicate_from_marker, normalize_pm_no_trade_marker, _pm_tick_bucket


def test_pm_ok_no_trade_does_not_skip_duplicate():
    marker = normalize_pm_no_trade_marker({
        "status": "OK_NO_TRADE",
        "reason": "NO_ORDERABLE_CANDIDATES",
        "completed": 1,
        "retryable": 0,
    }, session="afternoon")
    assert marker["retryable"] is True
    assert marker["completed"] is False
    assert should_skip_duplicate_from_marker(marker, session="afternoon") is False


def test_pm_orders_submitted_can_skip_duplicate():
    marker = {"status": "OK", "reason": "ORDERS_SUBMITTED", "completed": 1, "retryable": 0, "buy_orders": 3}
    assert should_skip_duplicate_from_marker(marker, session="afternoon") is True


def test_pm_tick_bucket():
    assert _pm_tick_bucket(datetime(2026, 7, 3, 13, 1)) == "1300"
    assert _pm_tick_bucket(datetime(2026, 7, 3, 13, 30)) == "1330"
