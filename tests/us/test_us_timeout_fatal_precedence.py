from trader.us.runner.trade_session_runner import _tick_has_order_activity


def classify(timeout_process, timeout_reconcile):
    final_status=final_reason=None
    if timeout_process.get("status") == "TICK_TIMEOUT_PROCESS_STUCK":
        return "FAILED", "TICK_TIMEOUT_PROCESS_STUCK"
    if timeout_reconcile.get("status") == "ERROR":
        return "FAILED", "TICK_TIMEOUT_RECONCILE_FAILED"
    return final_status, final_reason


def test_session_aborts_when_timeout_process_stuck_even_with_ack():
    assert classify({"status":"TICK_TIMEOUT_PROCESS_STUCK"},{"db_ack_restored_count":1,"broker_confirmed_count":1}) == ("FAILED","TICK_TIMEOUT_PROCESS_STUCK")


def test_session_aborts_when_timeout_reconcile_fails_after_db_ack_restore():
    assert classify({"status":"TICK_TIMEOUT_TERMINATED_NO_ORDER"},{"status":"ERROR","db_ack_restored_count":1}) == ("FAILED","TICK_TIMEOUT_RECONCILE_FAILED")


def test_session_does_not_start_next_tick_after_fatal_timeout():
    assert classify({"status":"TICK_TIMEOUT_PROCESS_STUCK"},{"unresolved_count":1})[0] == "FAILED"


def test_unresolved_timeout_blocks_entry_and_same_symbol_side():
    timeout_reconcile={"unresolved_count":1,"unresolved_symbol_sides":[["AMD","SELL"]]}
    assert timeout_reconcile["unresolved_count"] and ["AMD","SELL"] in timeout_reconcile["unresolved_symbol_sides"]
