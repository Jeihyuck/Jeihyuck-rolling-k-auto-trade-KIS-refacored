"""KR broker/ledger parity proof to avoid expensive *unneeded* historical repair.

The broker orders/fills and claim state must be reconciled FIRST. Historical
position import/entry-metadata repair is only bypassed if every actual KIS
holding is already represented by a unique active strategy-owned lifecycle
with the exact quantity and frozen exit contract.

Any doubt returns False, never fabricated parity. This is not permission to
skip authoritative KIS daily execution reads or submit with unresolved ACK.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Callable


def historical_repair_unnecessary(
    *,
    holdings: list[dict],
    positions: list[dict],
    reconcile_ok: bool,
    holdings_error: str | None,
    order_or_fill_changes: int,
    unresolved: bool,
    expected_strategy: str,
    reserved_owner_codes: frozenset[str] = frozenset({"122630"}),
) -> tuple[bool, str]:
    if not reconcile_ok or holdings_error:
        return False, "BROKER_EVIDENCE_UNRESOLVED"
    if unresolved or order_or_fill_changes:
        return False, "NEW_OR_UNRESOLVED_EXECUTION"
    if not isinstance(holdings, list) or not isinstance(positions, list):
        return False, "INVALID_INPUT"
    actual: dict[str, int] = {}
    for row in holdings:
        if not isinstance(row, dict):
            return False, "MALFORMED_HOLDING"
        code = str(row.get("pdno") or row.get("code") or "").strip().removeprefix("A").zfill(6)
        try:
            qty = int(float(row.get("hldg_qty") if row.get("hldg_qty") is not None else row.get("qty") or 0))
        except (TypeError, ValueError):
            return False, "INVALID_BROKER_QUANTITY"
        if qty < 0 or not code or code == "000000":
            return False, "INVALID_BROKER_HOLDING"
        if code in reserved_owner_codes:
            continue
        if qty == 0:
            continue
        if code in actual:
            return False, "DUPLICATE_BROKER_HOLDING"
        actual[code] = qty
    by_code: dict[str, dict] = {}
    for row in positions:
        if not isinstance(row, dict):
            return False, "MALFORMED_POSITION"
        if str(row.get("status") or "").upper() != "OPEN":
            continue
        code = str(row.get("code") or "").strip().zfill(6)
        if code in reserved_owner_codes:
            continue
        if code in by_code:
            return False, "AMBIGUOUS_ACTIVE_CYCLE"
        by_code[code] = row
    if set(by_code) != set(actual):
        return False, "KIS_LEDGER_SYMBOL_MISMATCH"
    for code, qty in actual.items():
        row = by_code[code]
        try:
            local_qty = int(row.get("qty") or 0)
            price = float(row.get("avg_buy_price") or 0)
        except (TypeError, ValueError):
            return False, "INVALID_POSITION_ECONOMICS"
        if local_qty != qty or local_qty <= 0 or price <= 0:
            return False, "KIS_LEDGER_QUANTITY_MISMATCH"
        if str(row.get("strategy") or "") != expected_strategy:
            return False, "STRATEGY_OWNER_MISMATCH"
        if not str(row.get("position_cycle_id") or "").strip():
            return False, "MISSING_POSITION_CYCLE"
        meta = row.get("entry_meta_json")
        plan = row.get("entry_exit_plan_json")
        if not isinstance(meta, dict) or not isinstance(plan, dict) or not plan:
            return False, "MISSING_FROZEN_EXIT_CONTRACT"
        if not str(row.get("exit_policy_family") or meta.get("exit_policy_family") or ""):
            return False, "MISSING_POLICY_FAMILY"
    return True, "EXACT_ACTIVE_CYCLE_PARITY"
