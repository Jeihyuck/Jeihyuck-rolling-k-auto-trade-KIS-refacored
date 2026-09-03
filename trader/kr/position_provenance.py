"""Fail-closed reconstruction of legacy PB1 position provenance.

This module intentionally contains no market-data dependency.  A repair may use
only broker balance evidence and persisted executions from the current (last
flat) lifecycle.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any, Iterable, Mapping

from trader.position_age import calc_position_age
from trader.trade_plan import seed_plan_fields_for_entry_style


class Confidence(str, Enum):
    CONFIRMED = "CONFIRMED"
    PARTIAL = "PARTIAL"
    AMBIGUOUS = "AMBIGUOUS"
    UNRECOVERABLE = "UNRECOVERABLE"


@dataclass(frozen=True)
class ProvenanceAudit:
    symbol: str
    confidence: Confidence
    reason: str
    current_qty: int
    current_avg: float
    reconstructed_qty: int = 0
    reconstructed_avg: float | None = None
    original_buy_id: str | int | None = None
    updates: dict[str, Any] | None = None


def _side(row: Mapping[str, Any]) -> str:
    return str(row.get("side") or row.get("order_side") or "").upper()


def _qty(row: Mapping[str, Any]) -> int:
    return int(row.get("filled_qty") or row.get("qty") or row.get("quantity") or 0)


def _price(row: Mapping[str, Any]) -> float:
    return float(row.get("fill_price") or row.get("price") or row.get("avg_price") or 0)


def audit_position(position: Mapping[str, Any], fills: Iterable[Mapping[str, Any]], *,
                   trade_date: date | str | None = None, avg_tolerance_pct: float = 1.0) -> ProvenanceAudit:
    """Reconstruct the last-flat lifecycle and return updates only when proven."""
    symbol = str(position.get("symbol") or position.get("code") or "").strip()
    qty, avg = int(position.get("qty") or 0), float(position.get("average_price") or position.get("avg_price") or 0)
    if symbol == "122630":
        return ProvenanceAudit(symbol, Confidence.UNRECOVERABLE, "OWNER_EXCLUDED_KR_INFINITE", qty, avg)
    owner = str(position.get("strategy_owner") or "PB1").upper()
    if owner in {"KR_INFINITE", "KR_INFINITE_V1"} or qty <= 0 or avg <= 0:
        return ProvenanceAudit(symbol, Confidence.UNRECOVERABLE, "POSITION_NOT_ELIGIBLE", qty, avg)
    rows = [r for r in fills if str(r.get("symbol") or r.get("code") or "").strip() == symbol
            and str(r.get("account") or "") == str(position.get("account") or "")
            and str(r.get("env") or "") == str(position.get("env") or "")]
    rows.sort(key=lambda r: str(r.get("filled_at") or r.get("created_at") or r.get("trade_date") or ""))
    net, start = 0, 0
    for index, row in enumerate(rows):
        net += _qty(row) if _side(row) == "BUY" else -_qty(row) if _side(row) == "SELL" else 0
        if net < 0:
            return ProvenanceAudit(symbol, Confidence.AMBIGUOUS, "NEGATIVE_FILL_CHAIN", qty, avg)
        if net == 0:
            start = index + 1
    active = rows[start:]
    buys = [r for r in active if _side(r) == "BUY" and _qty(r) > 0 and _price(r) > 0]
    reconstructed_qty = sum(_qty(r) if _side(r) == "BUY" else -_qty(r) if _side(r) == "SELL" else 0 for r in active)
    buy_qty = sum(_qty(r) for r in buys)
    reconstructed_avg = sum(_qty(r) * _price(r) for r in buys) / buy_qty if buy_qty else None
    if not buys:
        return ProvenanceAudit(symbol, Confidence.UNRECOVERABLE, "NO_CURRENT_LIFECYCLE_BUY", qty, avg, reconstructed_qty)
    if reconstructed_qty != qty:
        return ProvenanceAudit(symbol, Confidence.AMBIGUOUS, "QTY_MISMATCH", qty, avg, reconstructed_qty, reconstructed_avg)
    diff = abs(reconstructed_avg - avg) / avg * 100 if reconstructed_avg is not None else 100
    if diff > avg_tolerance_pct:
        return ProvenanceAudit(symbol, Confidence.AMBIGUOUS, "AVG_MISMATCH", qty, avg, reconstructed_qty, reconstructed_avg)
    first = buys[0]
    meta = first.get("meta") if isinstance(first.get("meta"), Mapping) else first
    style = meta.get("entry_style_selected") or meta.get("entry_style") or meta.get("entry_reason")
    if not style:
        return ProvenanceAudit(symbol, Confidence.PARTIAL, "ENTRY_METADATA_MISSING", qty, avg, reconstructed_qty, reconstructed_avg)
    # Canonical contract, never a repair-local mapping.
    try:
        plan_dict = seed_plan_fields_for_entry_style(str(style))
        if not plan_dict.get("exit_policy_family"):
            raise ValueError("unmapped style")
    except Exception:
        return ProvenanceAudit(symbol, Confidence.PARTIAL, "CANONICAL_POLICY_UNRESOLVED", qty, avg, reconstructed_qty, reconstructed_avg)
    opened = first.get("filled_at") or first.get("created_at") or first.get("trade_date")
    opened_date = str(opened)[:10]
    updates = {key: meta.get(key) for key in (
        "entry_reason", "entry_style_selected", "entry_style", "entry_thesis", "trade_horizon",
        "exit_policy_family", "entry_exit_plan", "initial_stop", "stop_price_at_entry", "pivot_price_at_entry")
        if meta.get(key) is not None}
    for key in ("entry_thesis", "trade_horizon", "exit_policy_family"):
        updates.setdefault(key, plan_dict.get(key))
    updates.update(opened_at=opened, entry_ts=opened, opened_trade_date=opened_date,
                   provenance_verified=True)
    if trade_date:
        age = calc_position_age(opened, trade_date)
        updates.update(holding_days=age.days_held, same_day=age.days_held == 0)
    return ProvenanceAudit(symbol, Confidence.CONFIRMED, "EXACT_FILL_CHAIN", qty, avg,
                           reconstructed_qty, reconstructed_avg,
                           first.get("id") or first.get("fill_id") or first.get("order_id"), updates)
