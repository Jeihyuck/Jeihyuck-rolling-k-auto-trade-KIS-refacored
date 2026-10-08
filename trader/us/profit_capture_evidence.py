"""Proof gate for repairing legacy US TP lifecycle counters.

Never infer a fill from ACK, DONE, cancelled, position deltas or zero-fill
evidence. The order and lifecycle must agree on immutable identity and epoch.
"""
from __future__ import annotations

from typing import Any

_ACTUAL_FILL_EVIDENCE = frozenset({
    "KIS_ORDER_CUMULATIVE_ACTUAL", "KIS_EXECUTION_ACTUAL",
    "KIS_TERMINAL_CANCEL",
})


def authoritative_tp_fill_for_backfill(stage: dict[str, Any], order: dict[str, Any]) -> int | None:
    """Return provable cumulative quantity or None (fail closed)."""
    if not isinstance(stage, dict) or not isinstance(order, dict):
        return None
    tp = str(stage.get("stage") or "").lower()
    if tp not in {"tp1", "tp2", "tp3"}:
        return None
    if str(stage.get("stage_status") or "").upper() != "DONE":
        return None
    if int(stage.get("cumulative_filled_qty") or 0) != 0:
        return None
    key = str(stage.get("client_order_key") or "")
    if not key or key != str(order.get("client_order_key") or ""):
        return None
    if str(stage.get("symbol") or "").upper() != str(order.get("symbol") or "").upper():
        return None
    if str(stage.get("trade_date") or "") != str(order.get("trade_date") or ""):
        return None
    epoch = str(stage.get("trading_epoch_id") or "")
    if not epoch or epoch != str(order.get("trading_epoch_id") or ""):
        return None
    meta = order.get("meta") if isinstance(order.get("meta"), dict) else {}
    stage_lifecycle = str(stage.get("position_lifecycle_id") or "").strip()
    order_lifecycle = str(meta.get("position_lifecycle_id") or "").strip()
    if not stage_lifecycle or stage_lifecycle != order_lifecycle:
        return None
    if str(meta.get("profit_capture_stage") or "").lower() != tp:
        return None
    if str(meta.get("fill_evidence_type") or "").upper() not in _ACTUAL_FILL_EVIDENCE:
        return None
    if str(order.get("side") or "").upper() != "SELL":
        return None
    if str(order.get("status") or "").upper() != "FILLED":
        return None
    try:
        requested = int(stage.get("requested_qty") or 0)
        order_requested = int(order.get("qty_requested") or 0)
        filled = int(order.get("qty_filled") or 0)
        reported = int(meta.get("cumulative_filled_qty") or 0)
    except (TypeError, ValueError):
        return None
    if requested <= 0 or requested != order_requested or filled != requested or reported != filled:
        return None
    return filled
