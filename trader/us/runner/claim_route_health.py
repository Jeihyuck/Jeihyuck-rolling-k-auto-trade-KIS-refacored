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
