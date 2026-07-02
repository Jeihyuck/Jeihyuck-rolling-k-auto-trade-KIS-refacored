from trader.pb1_runner import normalize_session_result, should_skip_duplicate_from_marker


def test_entry_plan_invalid_is_retryable_not_completed():
    normalized = normalize_session_result(
        status="OK_NO_TRADE",
        reason="ALL_CANDIDATES_SKIPPED_BEFORE_API_SUBMIT",
        order_candidates=1,
        api_submitted=0,
        skipped=1,
        skip_reasons=["entry_plan_invalid"],
    )
    assert normalized.status == "RETRYABLE_ORDER_BUILD_ERROR"
    assert normalized.completed == 0
    assert normalized.retryable == 1
    assert normalized.exit_reason == "ENTRY_PLAN_INVALID_BEFORE_API_SUBMIT"


def test_no_candidates_ok_no_trade_can_complete():
    normalized = normalize_session_result(
        status="OK_NO_TRADE",
        reason="NO_ORDERABLE_CANDIDATES",
        order_candidates=0,
        api_submitted=0,
        skipped=0,
        skip_reasons=[],
    )
    assert normalized.completed == 1
    assert normalized.retryable == 0


def test_pm_recovery_proceeds_when_marker_retryable():
    payload = {
        "completed": False,
        "retryable": True,
        "status": "RETRYABLE_ORDER_BUILD_ERROR",
        "exit_reason": "ENTRY_PLAN_INVALID_BEFORE_API_SUBMIT",
    }
    assert should_skip_duplicate_from_marker(payload) is False
