from trader.us.runner.status_contract import classify_tick_status

def test_duplicate_only_exit_blocks_are_warning():
    result = {"status": "FAILED_ALL_EXIT_ORDERS_BLOCKED", "block_reasons": {"US_SAME_DAY_SEMANTIC_SELL_DUPLICATE": 3}}
    assert classify_tick_status(result) == "warning"

def test_real_exit_block_remains_fatal():
    result = {"status": "FAILED_ALL_EXIT_ORDERS_BLOCKED", "block_reasons": {"broker_position_mismatch": 1}}
    assert classify_tick_status(result) == "fatal"
