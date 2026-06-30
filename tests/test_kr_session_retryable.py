from trader.kr.runner.trade_session_runner import normalize_kr_session_completion
from trader.pb1_runner import normalize_session_result


def test_all_candidates_skipped_before_submit_is_retryable():
    result = normalize_session_result(
        status="OK_NO_TRADE",
        reason="ALL_CANDIDATES_SKIPPED_BEFORE_API_SUBMIT",
        order_candidates=1,
        api_submitted=0,
        skipped=1,
        skip_reasons=["entry_plan_invalid"],
    )

    assert result.completed == 0
    assert result.retryable == 1
    assert result.status == "RETRYABLE_ORDER_BUILD_ERROR"


def test_pm_duplicate_after_completed_marker_is_not_retryable():
    status, reason, completed, retryable = normalize_kr_session_completion(
        status="OK",
        summary_reason="PB1_SESSION_DONE",
        marker={"completed": 1, "retryable": 0},
        pb1_status="SKIP_PHASE_WINDOW",
        pb1_exit_reason="phase_guard_skip_duplicate_pm_run",
    )

    assert status == "SKIP_DUPLICATE_NORMAL"
    assert reason == "phase_guard_skip_duplicate_pm_run"
    assert completed == 1
    assert retryable == 0


def test_pm_duplicate_after_entry_plan_invalid_marker_is_retryable():
    status, reason, completed, retryable = normalize_kr_session_completion(
        status="OK",
        summary_reason="PB1_SESSION_DONE",
        marker={"completed": 0, "retryable": 1},
        pb1_status="RETRYABLE_ORDER_BUILD_ERROR",
        pb1_exit_reason="ENTRY_PLAN_INVALID_BEFORE_API_SUBMIT",
    )

    assert status == "RETRYABLE_ORDER_BUILD_ERROR"
    assert reason == "ENTRY_PLAN_INVALID_BEFORE_API_SUBMIT"
    assert completed == 0
    assert retryable == 1
