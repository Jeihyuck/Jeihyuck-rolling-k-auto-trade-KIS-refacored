"""11/23 incident: historical reconcile fence is retryable and never a BUY permit."""
from trader.kr.runner.trade_session_runner import normalize_kr_session_completion


def test_reconcile_gate_denial_is_retryable_and_not_completed():
    status, reason, completed, retryable = normalize_kr_session_completion(
        status="OK_NO_TRADE",
        summary_reason="PB1_SESSION_DONE",
        marker={"completed": 1, "retryable": 0},
        pb1_status="BLOCKED_RECONCILE",
        pb1_exit_reason="historical_recovery_unverified",
    )
    assert status == "BLOCKED_RECONCILE"
    assert reason == "historical_recovery_unverified"
    assert completed == 0
    assert retryable == 1


def test_preexisting_retryable_build_error_is_not_reclassified():
    status, reason, completed, retryable = normalize_kr_session_completion(
        status="OK_NO_TRADE",
        summary_reason="PB1_SESSION_DONE",
        marker={"completed": 1, "retryable": 0},
        pb1_status="RETRYABLE_ORDER_BUILD_ERROR",
        pb1_exit_reason="ORDER_CANDIDATE_BUT_ZERO_API_SUBMIT",
    )
    assert status == "RETRYABLE_ORDER_BUILD_ERROR"
    assert completed == 0
    assert retryable == 1
