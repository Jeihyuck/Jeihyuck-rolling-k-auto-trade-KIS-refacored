"""Regression for US 2026-10-08 execution-claim conflict incident.

A blocked claim is not evidence that another order reached the broker.
The blocker must still fail close integrity until reconciled.
"""
from trader.us.runner.daily_report_runner import _broker_recovery_health_errors


def test_claim_conflicts_remain_fatal_but_are_not_called_duplicate_submits():
    errors = _broker_recovery_health_errors({
        "available": True,
        "duplicate_semantic_submit_detection_count": 141,
        "unresolved_execution_actions": 2,
    })
    assert "EXECUTION_CLAIM_CONFLICT_BLOCKED:141" in errors
    assert "UNRESOLVED_EXECUTION_ACTIONS:2" in errors
    assert not any("DUPLICATE_SEMANTIC_SUBMIT" in e for e in errors)


def test_clean_claim_health_has_no_false_failure():
    assert _broker_recovery_health_errors({
        "available": True,
        "duplicate_semantic_submit_detection_count": 0,
        "unresolved_execution_actions": 0,
    }) == []


def test_unavailable_recovery_health_remains_fatal():
    assert _broker_recovery_health_errors(None) == [
        "BROKER_RECOVERY_HEALTH_UNAVAILABLE"
    ]
