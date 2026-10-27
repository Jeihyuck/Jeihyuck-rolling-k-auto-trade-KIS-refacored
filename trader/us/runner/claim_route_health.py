"""Classify durable execution-claim fences separately from risk rejections.

A fence before broker HTTP is correct safety behavior, but a Tick that did
not submit due to claim conflict is NOT an error-free successful trading Tick.
"""
from __future__ import annotations


def summarize_claim_fences(orders: list[dict]) -> dict:
    fences = [
        row for row in orders or []
        if str(row.get("status") or "") == "ORDER_FENCED_UNRESOLVED_ACTION"
    ]
    return {
        "count": len(fences),
        "requires_reconcile": any(row.get("requires_reconcile") is True for row in fences),
        "reasons": sorted({str(row.get("reason") or "unresolved_execution_claim")
                           for row in fences}),
        "symbols": sorted({str(row.get("symbol") or (row.get("intent") or {}).get("symbol") or "").upper()
                           for row in fences if row.get("symbol") or (row.get("intent") or {}).get("symbol")}),
    }


def apply_claim_fence_tick_status(
    status: str, *, claim_fence_count: int, orders_sent: int,
    entry_intents: int, exit_intents: int,
) -> str:
    """Keep critical SELL/error outcomes; correct a falsely clean no-order Tick."""
    if claim_fence_count <= 0:
        return status
    if status in {
        "ERROR", "OK_WITH_ERRORS",
        "FAILED_ALL_EXIT_ORDERS_REJECTED", "FAILED_PARTIAL_EXIT_ORDERS_REJECTED",
        "FAILED_ALL_EXIT_ORDERS_BLOCKED", "FAILED_EXIT_INTENTS_NOT_ROUTED",
    }:
        return status
    if orders_sent == 0 and entry_intents > 0 and exit_intents == 0:
        return "ORDER_FENCED_CLAIM_RECONCILE_REQUIRED"
    if status in {"OK", "OK_NO_TRADE", "OK_ORDERS_SENT"}:
        return "OK_WITH_WARNINGS"
    return status


def classify_claim_remainder_conflict(attempted_qty: int, evidence: dict) -> dict:
    """Audit a 42->41 fence without manufacturing a replacement order.

    A correct current Claim row is never equivalent to current KIS broker
    state. This function only reports mismatch cause and remediation blocker.
    """
    base = {
        "safe_to_auto_retry": False,
        "broker_terminal_verified": False,
        "action": "RECONCILE_KIS_AND_CLAIM_READ_ONLY",
    }
    if evidence.get("status") != "VERIFIED":
        return {**base, "reason": "CLAIM_DB_PROOF_UNVERIFIED"}
    if evidence.get("active_attempt_exists"):
        return {**base, "reason": "ACTIVE_ATTEMPT_UNRESOLVED"}
    if str(evidence.get("action_state") or "") not in {
        "OPEN", "RETRYABLE", "PARTIALLY_SATISFIED",
    }:
        return {**base, "reason": "CLAIM_NOT_RETRYABLE"}
    try:
        requested = int(attempted_qty)
        remaining = int(evidence["remaining_target_qty"])
        target = int(evidence["target_qty"])
        filled = int(evidence.get("cumulative_filled_qty") or 0)
    except (KeyError, TypeError, ValueError, OverflowError):
        return {**base, "reason": "CLAIM_QUANTITY_UNVERIFIED"}
    if remaining <= 0 or filled < 0 or target <= 0 or target - filled != remaining:
        return {**base, "reason": "CLAIM_QUANTITY_INVARIANT_BROKEN"}
    if requested != remaining:
        return {
            **base, "reason": "CLAIM_TARGET_VS_NEW_SIGNAL_MISMATCH",
            "attempted_qty": requested,
            "persisted_target_qty": target,
            "persisted_remaining_qty": remaining,
            "persisted_filled_qty": filled,
        }
    return {
        **base, "reason": "CLAIM_QTY_MATCH_REQUIRES_FRESH_BROKER_VALIDATION",
        "attempted_qty": requested,
        "persisted_target_qty": target,
        "persisted_remaining_qty": remaining,
        "persisted_filled_qty": filled,
    }
