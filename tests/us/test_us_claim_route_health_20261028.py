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
