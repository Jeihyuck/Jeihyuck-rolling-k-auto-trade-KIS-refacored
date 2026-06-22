from trader.kr.runner.trade_session_runner import build_session_result, compute_session_marker


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


def test_ok_requires_entry_done_to_mark_completed():
    marker = compute_session_marker(build_session_result(exit_code=0, status="OK", entry_status="DONE"))
    assert marker["completed"] == 1
    assert marker["retryable"] == 0
