from trader.us.execution.v3_stage_truth import classify_v3_claim_rows


def test_empty_proven_claim_set_means_not_triggered():
    assert classify_v3_claim_rows([]) == {
        "PROFIT_DEFENSE_PARTIAL": "NOT_TRIGGERED", "RUNNER_FINAL": "NOT_TRIGGERED",
    }


def test_restart_and_late_ack_block_new_partial():
    assert classify_v3_claim_rows([
        {"action": "PROFIT_DEFENSE_PARTIAL", "action_state": "SATISFIED"},
    ])["PROFIT_DEFENSE_PARTIAL"] == "DONE"
    assert classify_v3_claim_rows([
        {"action": "PROFIT_DEFENSE_PARTIAL", "action_state": "SATISFIED"},
        {"action": "PROFIT_DEFENSE_PARTIAL", "action_state": "UNCERTAIN"},
    ])["PROFIT_DEFENSE_PARTIAL"] == "ACTIVE_ORDER"


def test_unknown_or_retryable_state_fails_closed():
    for value in ("RETRYABLE", "SUPERSEDED", "NOT_A_REAL_STATE", None):
        got = classify_v3_claim_rows([
            {"action": "RUNNER_FINAL", "action_state": value},
        ])
        assert got["RUNNER_FINAL"] == "BLOCKED"
    assert classify_v3_claim_rows(None)["RUNNER_FINAL"] == "UNKNOWN"
