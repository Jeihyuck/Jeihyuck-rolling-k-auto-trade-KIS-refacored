from trader.kr.runner.trade_session_runner import _balance_market_value, close_balance_guard


def test_close_balance_inconsistent_rows_zero_market_value_positive():
    result = close_balance_guard(
        holdings_count=0,
        market_value=2589451,
        raw_balance={"output1": [], "output2": {"tot_evlu_amt": "2589451"}},
    )

    assert result.status == "WARN_BALANCE_INCONSISTENT"
    assert result.retryable == 1
    assert result.completed == 0


def test_close_balance_cash_only_is_not_inconsistent():
    raw_balance = {"output1": [], "output2": {"tot_evlu_amt": "100000000", "dnca_tot_amt": "100000000"}}
    result = close_balance_guard(
        holdings_count=0,
        market_value=_balance_market_value(raw_balance),
        raw_balance=raw_balance,
    )

    assert result.status == "OK"
    assert result.retryable == 0
    assert result.completed == 1
