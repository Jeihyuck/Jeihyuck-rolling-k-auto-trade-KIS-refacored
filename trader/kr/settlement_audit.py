"""Read-only, order-cycle-specific KR persisted fill evidence audit.

A FILLED/RECONCILED order status is not interchangeable with persisted fill
price evidence.  This module never mutates orders, positions, or KIS state.
"""
from __future__ import annotations

from math import isfinite
from typing import Any, Mapping


def _quantity(raw: Any) -> int | None:
    if isinstance(raw, bool):
        return None
    try:
        numeric = float(raw)
    except (ValueError, TypeError, OverflowError):
        return None
    if not isfinite(numeric) or numeric < 0 or not numeric.is_integer():
        return None
    return int(numeric)


def audit_persisted_fill_evidence(
    order: Mapping[str, Any], fills: list[Mapping[str, Any]]
) -> dict[str, Any]:
    """Audit one *order_id*, not same-symbol fills across historical cycles.

    Missing DB evidence is an audit gap, NEVER evidence the broker did not fill.
    """
    order_id = str(order.get("order_id") or "")
    code = str(order.get("code") or "")
    side = str(order.get("side") or "").upper()
    cycle = str(order.get("position_cycle_id") or "")
    quantity = _quantity(order.get("qty"))
    status = str(order.get("status") or "").upper()
    result = {
        "order_id": order_id, "code": code, "side": side,
        "status": status, "requested_qty": quantity,
        "db_fill_qty": 0, "fill_evidence": "UNKNOWN",
        "gross_proceeds_or_cost": None, "risk": "AUDIT_ONLY",
    }
    if not order_id or not code or side not in {"BUY", "SELL"} or quantity is None:
        result["fill_evidence"] = "INVALID_ORDER_PROVENANCE"
        return result
    observed_qty = 0
    observed_notional = 0.0
    for fill in fills:
        if str(fill.get("order_id") or "") != order_id:
            result["fill_evidence"] = "CROSS_ORDER_FILL_PROVENANCE"
            return result
        if str(fill.get("code") or "") != code or str(fill.get("side") or "").upper() != side:
            result["fill_evidence"] = "FILL_OWNER_MISMATCH"
            return result
        fill_cycle = str(fill.get("position_cycle_id") or "")
        if cycle and fill_cycle and fill_cycle != cycle:
            result["fill_evidence"] = "FILL_CYCLE_MISMATCH"
            return result
        filled_qty = _quantity(fill.get("qty"))
        try:
            price = float(fill.get("price"))
        except (ValueError, TypeError, OverflowError):
            price = float("nan")
        if filled_qty is None or filled_qty == 0 or not isfinite(price) or price <= 0:
            result["fill_evidence"] = "INVALID_DB_FILL_PROOF"
            return result
        observed_qty += filled_qty
        observed_notional += filled_qty * price
        if not isfinite(observed_notional):
            result["fill_evidence"] = "NONFINITE_FILL_PROCEEDS"
            return result
    result["db_fill_qty"] = observed_qty
    if observed_qty > quantity:
        result["fill_evidence"] = "DB_FILL_QTY_EXCEEDS_ORDER"
    elif observed_qty == 0 and status in {"FILLED", "RECONCILED"}:
        result["fill_evidence"] = "STATUS_WITHOUT_DB_FILL"
    elif observed_qty == 0:
        result["fill_evidence"] = "NO_DB_FILL"
    elif observed_qty < quantity:
        result["fill_evidence"] = "PARTIAL_DB_FILL"
    else:
        result["fill_evidence"] = "FULL_DB_FILL_PROVEN"
    if observed_qty:
        result["gross_proceeds_or_cost"] = observed_notional
    return result
