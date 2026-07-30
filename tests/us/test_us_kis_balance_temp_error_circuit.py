from trader.us.runner.trade_tick_runner import evaluate_balance_error_circuit


def test_balance_thresholds():
    assert not evaluate_balance_error_circuit(4)["balance_warning"]
    assert evaluate_balance_error_circuit(5)["balance_warning"]
    assert evaluate_balance_error_circuit(10)["balance_reconcile_degraded"]
    blocked = evaluate_balance_error_circuit(20)
    assert blocked["entry_can_proceed"] is False and blocked["exit_can_proceed"] is True


def test_three_consecutive_failures_block_entry():
    result = evaluate_balance_error_circuit(0, consecutive_failed_ticks=3)
    assert result["entry_blocked_by_balance_degraded"] is True


def test_risk_off_reason_is_preserved_with_balance_degradation():
    result = evaluate_balance_error_circuit(20, entry_block_reasons=["risk_off_entry_block"])
    assert result["entry_block_reasons"] == ["risk_off_entry_block", "balance_reconcile_degraded"]
