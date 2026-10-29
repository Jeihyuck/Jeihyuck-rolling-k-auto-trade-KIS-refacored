"""2026-10-27 SYY 42->41 claim mismatch must not report Tick OK/warnings=0."""
from trader.us.runner.claim_route_health import (
    summarize_claim_fences,
    apply_claim_fence_tick_status,
)


def test_syy_execution_claim_fence_is_visible_without_broker_order():
    rows = [{"status": "ORDER_FENCED_UNRESOLVED_ACTION",
             "reason": "remaining_target_qty_mismatch",
             "broker_submit": False, "retry_order": False,
             "requires_reconcile": True,
             "intent": {"symbol": "SYY", "side": "BUY", "qty": 42}}]
    health = summarize_claim_fences(rows)
    assert health["count"] == 1
    assert health["reasons"] == ["remaining_target_qty_mismatch"]
    assert health["symbols"] == ["SYY"]
    assert health["requires_reconcile"] is True
    assert apply_claim_fence_tick_status(
        "OK", claim_fence_count=1, orders_sent=0,
        entry_intents=1, exit_intents=0,
    ) == "ORDER_FENCED_CLAIM_RECONCILE_REQUIRED"


def test_no_claim_fence_does_not_change_status():
    assert summarize_claim_fences([{"status": "ACK"}])["count"] == 0
    assert apply_claim_fence_tick_status(
        "OK", claim_fence_count=0, orders_sent=0,
        entry_intents=0, exit_intents=0,
    ) == "OK"


def test_partially_routed_buy_is_visible_as_warning():
    assert apply_claim_fence_tick_status(
        "OK_ORDERS_SENT", claim_fence_count=1, orders_sent=1,
        entry_intents=2, exit_intents=0,
    ) == "OK_WITH_WARNINGS"


def test_critical_protective_sell_status_never_masked():
    for original in ("FAILED_ALL_EXIT_ORDERS_REJECTED",
                     "FAILED_ALL_EXIT_ORDERS_BLOCKED",
                     "ERROR"):
        assert apply_claim_fence_tick_status(
            original, claim_fence_count=1, orders_sent=0,
            entry_intents=1, exit_intents=1,
        ) == original


def test_exact_claim_42_to_41_diagnosis_is_safe_not_reorder():
    from trader.us.runner.claim_route_health import classify_claim_remainder_conflict
    evidence = {
        "status": "VERIFIED", "action_state": "RETRYABLE",
        "target_qty": 41, "remaining_target_qty": 41,
        "cumulative_filled_qty": 0, "active_attempt_exists": False,
    }
    result = classify_claim_remainder_conflict(42, evidence)
    assert result["reason"] == "CLAIM_TARGET_VS_NEW_SIGNAL_MISMATCH"
    assert result["persisted_remaining_qty"] == 41
    assert result["safe_to_auto_retry"] is False


def test_claim_active_or_unverified_never_authorizes_retry():
    from trader.us.runner.claim_route_health import classify_claim_remainder_conflict
    for evidence in (
        {"status": "UNVERIFIED"},
        {"status": "VERIFIED", "action_state": "IN_FLIGHT",
         "active_attempt_exists": True},
    ):
        result = classify_claim_remainder_conflict(41, evidence)
        assert result["safe_to_auto_retry"] is False
        assert result["action"] == "RECONCILE_KIS_AND_CLAIM_READ_ONLY"


def test_equal_requested_remaining_still_needs_broker_truth():
    from trader.us.runner.claim_route_health import classify_claim_remainder_conflict
    result = classify_claim_remainder_conflict(41, {
        "status": "VERIFIED", "action_state": "OPEN",
        "target_qty": 41, "remaining_target_qty": 41,
        "cumulative_filled_qty": 0, "active_attempt_exists": False,
    })
    assert result["reason"] == "CLAIM_QTY_MATCH_REQUIRES_FRESH_BROKER_VALIDATION"
    assert not result["safe_to_auto_retry"]


def test_claim_target_minus_filled_must_match_remainder():
    from trader.us.runner.claim_route_health import classify_claim_remainder_conflict
    result = classify_claim_remainder_conflict(41, {
        "status": "VERIFIED", "action_state": "PARTIALLY_SATISFIED",
        "target_qty": 42, "remaining_target_qty": 41,
        "cumulative_filled_qty": 2, "active_attempt_exists": False,
    })
    assert result["reason"] == "CLAIM_QUANTITY_INVARIANT_BROKEN"


def test_actual_20261027_syy_claim_finally_satisfied_41_never_reorders_42():
    from trader.us.runner.claim_route_health import classify_claim_remainder_conflict
    result = classify_claim_remainder_conflict(42, {
        "status": "VERIFIED", "action_state": "SATISFIED",
        "target_qty": 41, "cumulative_filled_qty": 41,
        "remaining_target_qty": 0, "active_attempt_exists": False,
    })
    assert result["reason"] == "PRIOR_BUY_ALREADY_SATISFIED_DO_NOT_REORDER"
    assert result["safe_to_auto_retry"] is False


def test_syy_actual_20261027_aborted_41_before_later_filled_41_timeline():
    """DB timeline: abort at 14:51 UTC; successful second 41 at 15:00 UTC.

    A 42-signal fence between those actions must not be explained away as
    'previous 41 was already filled' at the time of the fence.
    """
    from trader.us.runner.claim_route_health import classify_claim_remainder_conflict
    previous_attempts = [
        {"attempt_no": 1, "requested_qty": 41,
         "attempt_state": "ABANDONED_PRE_SUBMIT",
         "created_at": "2026-10-27 14:51:55+00"},
        {"attempt_no": 2, "requested_qty": 41,
         "attempt_state": "FILLED",
         "created_at": "2026-10-27 15:00:26+00"},
    ]
    during_fence = classify_claim_remainder_conflict(42, {
        "status": "VERIFIED", "action_state": "OPEN",
        "target_qty": 41, "cumulative_filled_qty": 0,
        "remaining_target_qty": 41, "active_attempt_exists": False,
        "recent_attempts": previous_attempts[:1],
    })
    assert during_fence["reason"] == "CLAIM_TARGET_VS_NEW_SIGNAL_MISMATCH"
    assert during_fence["recent_attempts"][0]["attempt_state"] == "ABANDONED_PRE_SUBMIT"
    assert during_fence["safe_to_auto_retry"] is False
    # A later exact-target signal is not automatically submitted either;
    # it still requires a fresh broker GET and independent risk authorization.
    exact_target = classify_claim_remainder_conflict(41, {
        "status": "VERIFIED", "action_state": "OPEN",
        "target_qty": 41, "cumulative_filled_qty": 0,
        "remaining_target_qty": 41, "active_attempt_exists": False,
        "recent_attempts": previous_attempts[:1],
    })
    assert exact_target["reason"] == "CLAIM_QTY_MATCH_REQUIRES_FRESH_BROKER_VALIDATION"
    assert exact_target["safe_to_auto_retry"] is False
    terminal = classify_claim_remainder_conflict(42, {
        "status": "VERIFIED", "action_state": "SATISFIED",
        "target_qty": 41, "cumulative_filled_qty": 41,
        "remaining_target_qty": 0, "active_attempt_exists": False,
        "recent_attempts": previous_attempts,
    })
    assert terminal["reason"] == "PRIOR_BUY_ALREADY_SATISFIED_DO_NOT_REORDER"
    assert [a["attempt_state"] for a in terminal["recent_attempts"]] == [
        "ABANDONED_PRE_SUBMIT", "FILLED",
    ]
