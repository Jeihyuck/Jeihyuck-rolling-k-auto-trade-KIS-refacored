from trader.kr.runner.trade_session_runner import build_session_result, compute_session_marker, extract_sell_orders_ack


def test_pm_partial_success_entry_abort_does_not_mark_completed():
    result = build_session_result(
        exit_code=2,
        sell_orders_ack=1,
        entry_status="ABORT",
        entry_abort_reason="missing_db_exact_scored_final30",
    )
    marker = compute_session_marker(result)
    assert marker["completed"] == 0
    assert marker["retryable"] == 1
    assert marker["status"] == "PARTIAL_SUCCESS_RETRYABLE"
    assert marker["sell_completed"] == 1
    assert marker["entry_completed"] == 0


def test_pm_partial_success_does_not_depend_on_env(monkeypatch):
    monkeypatch.delenv("PB1_SELL_ORDERS_ACK", raising=False)
    result = build_session_result(
        exit_code=2,
        status="UNKNOWN",
        sell_orders_ack=1,
        entry_status="ABORT",
        entry_abort_reason="missing_db_exact_scored_final30",
    )
    marker = compute_session_marker(result)
    assert marker["status"] == "PARTIAL_SUCCESS_RETRYABLE"
    assert marker["completed"] == 0
    assert marker["retryable"] == 1
    assert marker["sell_completed"] == 1
    assert marker["entry_completed"] == 0


def test_env_sell_ack_does_not_create_false_partial_success(monkeypatch):
    monkeypatch.setenv("PB1_SELL_ORDERS_ACK", "1")
    result = build_session_result(
        exit_code=2,
        status="UNKNOWN",
        sell_orders_ack=0,
        entry_status="ABORT",
        entry_abort_reason="missing_db_exact_scored_final30",
    )
    marker = compute_session_marker(result)
    assert marker["sell_completed"] == 0
    assert marker["completed"] == 0
    assert marker["status"] != "PARTIAL_SUCCESS_RETRYABLE"


def test_ok_done_marks_completed():
    result = build_session_result(exit_code=0, status="OK", sell_orders_ack=0, entry_status="DONE", entry_abort_reason=None)
    marker = compute_session_marker(result)
    assert marker["completed"] == 1
    assert marker["retryable"] == 0


def test_ok_with_unknown_entry_does_not_mark_completed():
    result = build_session_result(exit_code=0, status="OK", sell_orders_ack=0, entry_status="UNKNOWN", entry_abort_reason=None)
    marker = compute_session_marker(result)
    assert marker["completed"] == 0
    assert marker["retryable"] == 1


def test_extract_sell_orders_ack_from_actual_result_shapes():
    assert extract_sell_orders_ack({"sell_orders_ack": 1}) == 1
    assert extract_sell_orders_ack({"orders_summary": {"sell_accepted": 2}}) == 2
    assert extract_sell_orders_ack({"orders": [{"side": "SELL", "status": "ACCEPTED"}, {"side": "BUY", "status": "ACCEPTED"}]}) == 1
