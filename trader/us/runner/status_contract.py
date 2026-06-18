# -*- coding: utf-8 -*-
"""Shared US tick/session status contract."""
from __future__ import annotations

SUCCESS_STATUSES = {
    "OK", "OK_SIGNAL_ONLY", "OK_NO_TRADE", "OK_WITH_WARNINGS", "OK_ORDERS_SENT",
    "OK_ENTRY_ORDERS_SENT", "OK_EXIT_ORDERS_SENT", "OK_EXIT_POSITION_CLOSED",
    "OK_RECONCILED", "NO_ENTRY_INTENTS", "NO_ORDERS_RISK_BLOCKED", "PARTIAL_ORDERS_BLOCKED",
}

WARNING_STATUSES = {
    "WARN_RECONCILE_PENDING", "WARN_NO_FILL_YET", "WARN_KIS_TEMPORARY_ERROR",
    "WARN_SELL_REJECT_RECONCILE_PENDING", "WARN_DUPLICATE_EXIT_BLOCKED",
    "FAILED_PARTIAL_EXIT_ORDERS_REJECTED", "FAILED_ALL_EXIT_ORDERS_BLOCKED",
}

FATAL_STATUSES = {
    "FAILED", "ERROR", "FAILED_KIS_AUTH", "FAILED_PREP_GUARD", "FAILED_DB",
    "FAILED_ALL_ENTRY_ORDERS_REJECTED", "FAILED_EXIT_INTENTS_NOT_ROUTED", "FAILED_UNKNOWN",
}

NO_BALANCE_PATTERNS = (
    "잔고내역이 없습니다", "잔고가 없습니다", "매도가능수량", "주문가능수량",
    "no balance", "insufficient position",
)


def is_no_balance_sell_reject(message: str) -> bool:
    text = str(message or "").lower()
    return any(pattern.lower() in text for pattern in NO_BALANCE_PATTERNS)


def classify_tick_status(tick_result: dict | str | None) -> str:
    """Return success/warning/fatal for a tick result payload or raw status."""
    if isinstance(tick_result, dict):
        status = str(tick_result.get("status") or "")
        if status == "FAILED_ALL_EXIT_ORDERS_REJECTED":
            no_balance_symbols = {str(s).upper() for s in (tick_result.get("no_balance_sell_symbols") or [])}
            recent_ack_symbols = {str(s).upper() for s in (tick_result.get("recent_sell_ack_symbols") or [])}
            qty_zero_symbols = {str(s).upper() for s in (tick_result.get("balance_qty_zero_symbols") or [])}
            orderable_zero_symbols = {str(s).upper() for s in (tick_result.get("orderable_qty_zero_symbols") or [])}
            absent_symbols = {str(s).upper() for s in (tick_result.get("position_absent_symbols") or [])}
            closed_symbols = qty_zero_symbols | orderable_zero_symbols | absent_symbols
            reconciliatory_symbols = no_balance_symbols & recent_ack_symbols & closed_symbols
            if no_balance_symbols and no_balance_symbols <= reconciliatory_symbols:
                return "warning"
            if not no_balance_symbols:
                reason = str(tick_result.get("primary_reject_reason") or tick_result.get("reason") or tick_result.get("error") or "")
                if is_no_balance_sell_reject(reason):
                    recent_ack = bool(tick_result.get("recent_sell_ack_exists"))
                    no_balance_count = int(tick_result.get("no_balance_sell_reject_count") or 0)
                    qty_zero = bool(tick_result.get("balance_qty_zero") or tick_result.get("orderable_qty_zero"))
                    if recent_ack and no_balance_count > 0 and qty_zero:
                        return "warning"
    else:
        status = str(tick_result or "")
    if status in SUCCESS_STATUSES:
        return "success"
    if status in WARNING_STATUSES:
        return "warning"
    return "fatal"
