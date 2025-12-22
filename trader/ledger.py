from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from .config import KST
from .code_utils import normalize_code


def _ensure_state(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    lots = state.get("lots")
    if not isinstance(lots, list):
        lots = []
        state["lots"] = lots
    return lots


def _norm_sid(value: int | str | None) -> int | str | None:
    if value is None:
        return None
    text = str(value)
    return int(text) if text.isdigit() else text


def record_buy_fill(
    state: Dict[str, Any],
    *,
    lot_id: str,
    pdno: str,
    strategy_id: int | str,
    engine: str,
    entry_ts: str,
    entry_price: float,
    qty: int,
    meta: Dict[str, Any] | None,
) -> None:
    lots = _ensure_state(state)
    if any(lot.get("lot_id") == lot_id for lot in lots):
        return
    lots.append(
        {
            "lot_id": lot_id,
            "pdno": normalize_code(pdno),
            "strategy_id": strategy_id,
            "engine": engine,
            "entry_ts": entry_ts,
            "entry_price": float(entry_price),
            "qty": int(qty),
            "remaining_qty": int(qty),
            "meta": meta or {},
        }
    )


def apply_sell_fill_fifo(
    state: Dict[str, Any],
    *,
    pdno: str,
    qty_filled: int,
    sell_ts: str,
    strategy_id: int | str | None = None,
    allow_blocked: bool = False,
) -> None:
    lots = _ensure_state(state)
    remaining = int(qty_filled)
    if remaining <= 0:
        return

    req_sid = _norm_sid(strategy_id)

    def _consume(remaining_qty: int, sid_filter: int | str | None) -> int:
        for lot in lots:
            if normalize_code(lot.get("pdno")) != normalize_code(pdno):
                continue
            if not allow_blocked and lot.get("meta", {}).get("sell_blocked") is True:
                continue

            lot_sid = _norm_sid(lot.get("strategy_id"))
            if sid_filter is not None and lot_sid != sid_filter:
                continue

            lot_remaining = int(lot.get("remaining_qty") or 0)
            if lot_remaining <= 0:
                continue

            delta = min(lot_remaining, remaining_qty)
            lot["remaining_qty"] = int(lot_remaining - delta)
            if delta > 0:
                lot["last_sell_ts"] = sell_ts

            remaining_qty -= delta
            if remaining_qty <= 0:
                break
        return remaining_qty

    _consume(remaining, req_sid)


def owned_lots_by_strategy(state: Dict[str, Any], strategy_id: int | str) -> List[Dict[str, Any]]:
    lots = _ensure_state(state)
    return [
        lot
        for lot in lots
        if int(lot.get("remaining_qty") or 0) > 0
        and _norm_sid(lot.get("strategy_id")) == _norm_sid(strategy_id)
    ]


def remaining_qty_for_strategy(state: Dict[str, Any], pdno: str, strategy_id: int | str) -> int:
    lots = _ensure_state(state)
    total = 0
    for lot in lots:
        if normalize_code(lot.get("pdno")) != normalize_code(pdno):
            continue
        if int(lot.get("remaining_qty") or 0) <= 0:
            continue
        if _norm_sid(lot.get("strategy_id")) != _norm_sid(strategy_id):
            continue
        total += int(lot.get("remaining_qty") or 0)
    return total


def dominant_strategy_for(state: Dict[str, Any], pdno: str) -> int | None:
    lots = _ensure_state(state)
    totals: Dict[int, int] = {}
    for lot in lots:
        if normalize_code(lot.get("pdno")) != normalize_code(pdno):
            continue
        remaining = int(lot.get("remaining_qty") or 0)
        if remaining <= 0:
            continue
        sid = _norm_sid(lot.get("strategy_id"))
        if isinstance(sid, int) and 1 <= sid <= 5:
            totals[sid] = totals.get(sid, 0) + remaining
    if not totals:
        return None
    return max(totals.items(), key=lambda item: item[1])[0]


def strategy_avg_price(
    state: Dict[str, Any], pdno: str, strategy_id: int | str
) -> float | None:
    lots = _ensure_state(state)
    total_qty = 0
    total_cost = 0.0
    for lot in lots:
        if normalize_code(lot.get("pdno")) != normalize_code(pdno):
            continue
        if _norm_sid(lot.get("strategy_id")) != _norm_sid(strategy_id):
            continue
        remaining = int(lot.get("remaining_qty") or 0)
        if remaining <= 0:
            continue
        entry_price = float(lot.get("entry_price") or 0.0)
        total_qty += remaining
        total_cost += entry_price * remaining
    if total_qty <= 0:
        return None
    return total_cost / total_qty


def reconcile_with_broker_holdings(state: Dict[str, Any], holdings: List[Dict[str, Any]]) -> None:
    lots = _ensure_state(state)
    holdings_map: Dict[str, Dict[str, Any]] = {}
    for row in holdings:
        code = normalize_code(row.get("code") or row.get("pdno") or "")
        if not code:
            continue
        qty = int(row.get("qty") or 0)
        avg_price = row.get("avg_price")
        existing = holdings_map.get(code)
        if existing:
            existing["qty"] += qty
            if existing.get("avg_price") is None:
                existing["avg_price"] = avg_price
        else:
            holdings_map[code] = {"qty": qty, "avg_price": avg_price}

    now_ts = datetime.now(KST).isoformat()

    for lot in lots:
        if str(lot.get("strategy_id")) in {"ORPHAN", "UNKNOWN"}:
            lot["strategy_id"] = "MANUAL"

    for lot in lots:
        pdno = normalize_code(lot.get("pdno"))
        if pdno not in holdings_map or holdings_map[pdno]["qty"] <= 0:
            if int(lot.get("remaining_qty") or 0) > 0:
                lot["remaining_qty"] = 0

    for pdno, payload in holdings_map.items():
        hold_qty = int(payload.get("qty") or 0)
        if hold_qty <= 0:
            continue
        total_remaining = sum(
            int(lot.get("remaining_qty") or 0)
            for lot in lots
            if normalize_code(lot.get("pdno")) == pdno
        )
        if total_remaining < hold_qty:
            diff = hold_qty - total_remaining
            lots.append(
                {
                    "lot_id": f"{pdno}-RECON-{now_ts}",
                    "pdno": pdno,
                    "strategy_id": "MANUAL",
                    "engine": "reconcile",
                    "entry_ts": now_ts,
                    "entry_price": float(payload.get("avg_price") or 0.0),
                    "qty": int(diff),
                    "remaining_qty": int(diff),
                    "meta": {"reconciled": True, "manual": True},
                }
            )
        elif total_remaining > hold_qty:
            extra = total_remaining - hold_qty
            for lot in reversed(lots):
                if normalize_code(lot.get("pdno")) != pdno:
                    continue
                lot_remaining = int(lot.get("remaining_qty") or 0)
                if lot_remaining <= 0:
                    continue
                delta = min(lot_remaining, extra)
                lot["remaining_qty"] = int(lot_remaining - delta)
                extra -= delta
                if extra <= 0:
                    break
