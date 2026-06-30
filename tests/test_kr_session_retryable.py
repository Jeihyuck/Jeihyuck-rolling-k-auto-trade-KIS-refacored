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
