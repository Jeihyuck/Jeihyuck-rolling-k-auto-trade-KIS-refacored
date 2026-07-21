from trader.kr.runner.trade_session_runner import lock_unavailable_result_fields, normalize_kr_session_completion


def test_locked_pb1_result_is_failed_and_retryable():
    status, reason, completed, retryable = normalize_kr_session_completion(
        status="OK", summary_reason="PB1_SESSION_DONE", marker={"completed": 1},
        pb1_status="SKIP_LOCKED", pb1_exit_reason="PB1_ADVISORY_LOCK_UNAVAILABLE",
    )
    assert (status, reason, completed, retryable) == ("FAILED", "PB1_ADVISORY_LOCK_UNAVAILABLE", 0, 1)


def test_missing_pb1_result_is_never_ok():
    status, reason, completed, retryable = normalize_kr_session_completion(
        status="OK", summary_reason="PB1_RESULT_MISSING", marker={"completed": 1},
        pb1_status="", pb1_exit_reason="PB1_RESULT_MISSING",
    )
    assert (status, reason, completed, retryable) == ("FAILED", "PB1_RESULT_MISSING", 0, 1)


def test_close_locked_pb1_never_reports_engine_or_order_success():
    status, reason, completed, retryable = normalize_kr_session_completion(
        status="OK", summary_reason="PB1_SESSION_DONE", marker={"completed": 1},
        pb1_status="SKIP_LOCKED", pb1_exit_reason="PB1_ADVISORY_LOCK_UNAVAILABLE",
    )
    result = {"status": status, "reason": reason, "completed": completed, "retryable": retryable}
    result.update(lock_unavailable_result_fields())

    assert result == {
        "status": "FAILED", "reason": "PB1_ADVISORY_LOCK_UNAVAILABLE", "completed": 0, "retryable": 1,
        "engine_started": False, "pb1_result_present": False, "orders_intent": 0, "orders_ack": 0,
    }
