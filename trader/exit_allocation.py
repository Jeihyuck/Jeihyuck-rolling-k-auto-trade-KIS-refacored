from __future__ import annotations

from typing import Any, Dict, List, Optional

from trader.ledger import apply_sell_fill_fifo, dominant_strategy_for

def _strategy_qty_map(lot_state: Dict[str, Any], code: str) -> Dict[str, int]:
    lots = lot_state.get("lots", [])
    code_key = str(code).zfill(6)
    totals: Dict[str, int] = {}
    if not isinstance(lots, list):
        return totals
    for lot in lots:
        if str(lot.get("pdno") or "").zfill(6) != code_key:
            continue
        remaining = int(lot.get("remaining_qty") or 0)
        if remaining <= 0:
            continue
        sid = lot.get("strategy_id")
        if sid is None:
            continue
        sid_key = str(sid)
        totals[sid_key] = totals.get(sid_key, 0) + remaining
    return totals


def allocate_sell_qty(
    lot_state: Dict[str, Any],
    code: str,
    requested_qty: int,
    *,
    scope: str,
    trigger_strategy_id: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    code = str(code).zfill(6)
    requested_qty = int(requested_qty)
    if requested_qty <= 0:
        return []

    if scope == "strategy":
        if trigger_strategy_id is None:
            return []
        totals = _strategy_qty_map(lot_state, code)
        trigger_key = str(trigger_strategy_id)
        qty = min(requested_qty, int(totals.get(trigger_key, 0)))
        return (
            [{"strategy_id": trigger_key, "qty": int(qty)}]
            if qty > 0
            else []
        )

    totals = _strategy_qty_map(lot_state, code)
    if not totals:
        return []

    if scope == "dominant":
        dominant = dominant_strategy_for(lot_state, code)
        if dominant is None:
            return []
        dominant_key = str(dominant)
        qty = min(requested_qty, totals.get(dominant_key, 0))
        return [{"strategy_id": dominant_key, "qty": int(qty)}] if qty > 0 else []

    if scope != "proportional":
        return []

    total_qty = sum(totals.values())
    if total_qty <= 0:
        return []
    requested_qty = min(requested_qty, total_qty)
    allocations: List[Dict[str, Any]] = []
    remaining = requested_qty
    strategy_ids = sorted(totals.keys())
    for sid in strategy_ids[:-1]:
        ratio_qty = int(requested_qty * (totals[sid] / total_qty))
        qty = min(ratio_qty, totals[sid], remaining)
        if qty > 0:
            allocations.append({"strategy_id": sid, "qty": int(qty)})
            remaining -= qty
    if remaining > 0:
        last_sid = strategy_ids[-1]
        qty = min(remaining, totals[last_sid])
        if qty > 0:
            allocations.append({"strategy_id": last_sid, "qty": int(qty)})
    return allocations


def apply_sell_allocation(
    lot_state: Dict[str, Any],
    code: str,
    allocations: List[Dict[str, Any]],
    sell_ts: str,
    *,
    allow_blocked: bool = False,
) -> int:
    sold_total = 0
    for alloc in allocations:
        qty = int(alloc.get("qty") or 0)
        sid = alloc.get("strategy_id", None)
        if qty <= 0:
            continue
        apply_sell_fill_fifo(
            lot_state,
            pdno=str(code).zfill(6),
            qty_filled=qty,
            sell_ts=sell_ts,
            strategy_id=sid,
            allow_blocked=allow_blocked,
        )
        sold_total += qty
    return sold_total


def run_allocation_self_checks() -> None:
    lot_state = {
        "lots": [
            {"pdno": "000001", "strategy_id": 1, "remaining_qty": 7},
            {"pdno": "000001", "strategy_id": 5, "remaining_qty": 3},
            {"pdno": "000001", "strategy_id": "ORPHAN", "remaining_qty": 5},
        ]
    }
    allocations = allocate_sell_qty(
        lot_state, "000001", 10, scope="proportional", trigger_strategy_id=None
    )
    assert sum(a["qty"] for a in allocations) == 10
    allocations = allocate_sell_qty(
        lot_state, "000001", 5, scope="strategy", trigger_strategy_id=1
    )
    assert allocations and all(a["strategy_id"] == "1" for a in allocations)
    allocations = allocate_sell_qty(
        lot_state, "000001", 10, scope="proportional", trigger_strategy_id=None
    )
    allocated_strategies = {a["strategy_id"] for a in allocations}
    assert "ORPHAN" in allocated_strategies


if __name__ == "__main__":
    run_allocation_self_checks()
