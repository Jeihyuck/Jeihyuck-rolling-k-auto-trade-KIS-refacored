"""Read-only KR PB1 broker-vs-DB position snapshot audit.

No DB updates, no broker submit/cancel, and no assumption that a successful
broker ACK equals a fill. Broker-only symbols may belong to other sleeves.
"""
from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, InvalidOperation
from typing import Any


def _nonnegative_int(value: Any) -> int | None:
    try:
        value = str(value).replace(",", "").strip()
        if not value or "." in value:
            return None
        parsed = int(value)
        return parsed if parsed >= 0 else None
    except (TypeError, ValueError):
        return None


def _amount(value: Any) -> Decimal | None:
    try:
        result = Decimal(str(value).replace(",", ""))
    except (TypeError, ValueError, InvalidOperation):
        return None
    return result if result.is_finite() and result >= 0 else None


def compare_pb1_db_to_kis(
    *,
    positions: list[dict[str, Any]],
    broker_snapshot: dict[str, Any] | None,
    source: str,
    require_economics: bool = False,
) -> dict[str, Any]:
    """Pure snapshot comparison. Only the caller can prove source/epoch/freshness."""
    if (
        source != "api"
        or not isinstance(broker_snapshot, dict)
        or broker_snapshot.get("rt_cd") != "0"
        or not isinstance(broker_snapshot.get("output1"), list)
    ):
        return {"status": "UNVERIFIED_BROKER_SNAPSHOT", "mismatches": [],
                "verified_match": False}

    broker_qty: dict[str, int] = {}
    broker_cost: dict[str, Decimal | None] = {}
    for row in broker_snapshot["output1"]:
        if not isinstance(row, dict):
            return {"status": "UNVERIFIED_BROKER_SNAPSHOT", "mismatches": [],
                    "verified_match": False}
        code = str(row.get("pdno") or row.get("code") or "").strip().lstrip("A")
        qty = _nonnegative_int(row.get("hldg_qty"))
        if len(code) != 6 or not code.isdigit() or qty is None or code in broker_qty:
            return {"status": "UNVERIFIED_BROKER_SNAPSHOT", "mismatches": [],
                    "verified_match": False}
        broker_qty[code] = qty
        broker_cost[code] = _amount(row.get("pchs_amt"))

    by_code: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in positions:
        if str(row.get("status") or "").upper() != "OPEN":
            continue
        code = str(row.get("code") or "").strip().lstrip("A")
        if len(code) != 6 or not code.isdigit():
            return {"status": "INVALID_DB_POSITION_IDENTITY", "mismatches": [],
                    "verified_match": False}
        by_code[code].append(row)

    mismatches = []
    for code, rows in sorted(by_code.items()):
        if len(rows) != 1:
            mismatches.append({"code": code, "reason": "MULTIPLE_OPEN_LIFECYCLES",
                               "db_position_rows": len(rows)})
            continue
        db_qty = _nonnegative_int(rows[0].get("qty"))
        actual_qty = broker_qty.get(code, 0)
        if db_qty is None or db_qty != actual_qty:
            mismatches.append({"code": code, "reason": "DB_KIS_QTY_MISMATCH",
                               "db_qty": db_qty, "kis_qty": actual_qty})
            continue
        if require_economics and db_qty > 0:
            amount = broker_cost.get(code)
            db_amount = _amount(rows[0].get("total_cost"))
            if amount is None or db_amount is None:
                mismatches.append({"code": code, "reason": "COST_PROOF_UNAVAILABLE"})
            elif abs(amount - db_amount) > Decimal("1.00"):
                mismatches.append({"code": code, "reason": "DB_KIS_COST_MISMATCH",
                                   "db_cost": str(db_amount),
                                   "kis_cost": str(amount)})
    return {
        "status": "MISMATCH" if mismatches else "MATCH_FOR_PB1_OPEN_SYMBOLS",
        "verified_match": not mismatches,
        "positions_checked": len(by_code),
        "mismatches": mismatches,
        # Broker-only symbols are deliberately not asserted to be PB1-owned.
        "broker_only_symbols_unattributed": sorted(set(broker_qty) - set(by_code)),
    }
