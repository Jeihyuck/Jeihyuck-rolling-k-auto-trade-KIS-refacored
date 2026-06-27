def test_entry_degraded_status_is_warning_contract():
    from trader.us.runner.status_contract import classify_tick_status
    assert classify_tick_status({"status": "OK_NO_TRADE_ENTRY_DEGRADED", "entry_error_type": "watchlist_load_timeout"}) == "success"
    assert classify_tick_status({"status": "OK_EXIT_SENT_ENTRY_DEGRADED", "entry_error_type": "watchlist_load_timeout"}) == "success"
