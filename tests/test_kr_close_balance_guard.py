from trader.kr.runner.trade_session_runner import close_balance_guard


def test_close_balance_inconsistent_rows_zero_market_value_positive():
    result = close_balance_guard(
        holdings_count=0,
        market_value=2589451,
        raw_balance={"output1": [], "output2": {"tot_evlu_amt": "2589451"}},
    )

    assert result.status == "WARN_BALANCE_INCONSISTENT"
    assert result.retryable == 1
    assert result.completed == 0
