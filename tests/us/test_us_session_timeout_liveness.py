from trader.us.runner.trade_session_runner import advance_timeout_execution_mode


def test_transient_timeout_does_not_drop_session():
    mode, buy_allowed = advance_timeout_execution_mode("NORMAL", 1, threshold=3)
    assert mode == "NORMAL"
    assert buy_allowed is True


def test_repeated_timeout_enters_safe_degraded_and_continues():
    mode, buy_allowed = advance_timeout_execution_mode("NORMAL", 3, threshold=3)
    assert mode == "SAFE_DEGRADED"
    assert buy_allowed is False


def test_ten_slots_with_three_timeouts_are_all_attempted():
    attempted = 0
    mode = "NORMAL"
    consecutive = 0
    for slot in range(1, 11):
        attempted += 1
        consecutive = consecutive + 1 if slot in {2, 5, 8} else 0
        mode, _ = advance_timeout_execution_mode(mode, consecutive, threshold=3)
    assert attempted == 10
    assert mode == "NORMAL"
