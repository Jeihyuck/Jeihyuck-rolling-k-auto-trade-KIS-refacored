from trader.kr.runner.trade_session_runner import normalize_kr_session_completion


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
