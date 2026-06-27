def test_entry_degraded_status_contract_is_warning_not_success():
    from trader.us.runner.status_contract import classify_tick_status
    for status in ["OK_ENTRY_DEGRADED_NO_BUY", "OK_EXIT_SENT_ENTRY_DEGRADED", "OK_NO_TRADE_ENTRY_DEGRADED"]:
        assert classify_tick_status({"status": status}) == "warning"
    assert classify_tick_status({"status": "FAILED_ALL_EXIT_ORDERS_BLOCKED", "block_reasons": ["pending_sell_order_exists"]}) == "warning"
