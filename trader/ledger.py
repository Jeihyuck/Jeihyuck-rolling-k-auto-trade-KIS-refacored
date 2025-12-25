from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from .code_utils import normalize_code
from .config import KST
from .paths import LOG_DIR
from .strategy_ids import STRATEGY_INT_IDS
from .strategy_registry import normalize_sid

LEDGER_PATH = LOG_DIR / "ledger.jsonl"


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        f.flush()
        try:
            os.fsync(f.fileno())
        except Exception:
            pass


def append_ledger_event(event_type: str, payload: Dict[str, Any]) -> None:
    entry = {"event": str(event_type or "").upper(), **payload}
    _append_jsonl(LEDGER_PATH, entry)


def record_trade_ledger(
    *,
    timestamp: str,
    code: str,
    strategy_id: int | str | None,
    side: str,
    qty: int,
    price: float,
    meta: Dict[str, Any] | None = None,
    path: Path | None = None,
) -> Dict[str, Any]:
    entry = {
        "timestamp": timestamp,
        "code": normalize_code(code),
        "strategy_id": strategy_id,
        "side": str(side).upper(),
        "qty": int(qty),
        "price": float(price),
        "meta": meta or {},
    }
    append_ledger_event(
        "EXIT" if str(side).upper() == "SELL" else "FILL",
        {**entry, "sid": normalize_sid(strategy_id)} if path is None else {**entry, "path_override": str(path)},
    )
    return entry


def load_ledger_entries(path: Path | None = None) -> List[Dict[str, Any]]:
    path = path or LEDGER_PATH
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except Exception:
        return rows
    return rows


def strategy_map_from_ledger(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    mapping: Dict[str, Any] = {}
    for entry in entries:
        code = normalize_code(entry.get("code") or entry.get("pdno") or "")
        if not code:
            continue
        side = str(entry.get("side") or "").upper()
        sid = entry.get("strategy_id")
        if side == "BUY" and sid is not None:
            mapping[code] = sid
        elif code not in mapping and sid is not None:
            mapping[code] = sid
    return mapping


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


def _summarize_positions(lots: List[Dict[str, Any]]) -> Dict[str, Any]:
    positions: Dict[str, Any] = {}
    for lot in lots:
        pdno = normalize_code(lot.get("pdno") or "")
        remaining = int(lot.get("remaining_qty") or 0)
        if not pdno or remaining <= 0:
            continue
        sid = _norm_sid(lot.get("sid") or lot.get("strategy_id"))
        entry_price = float(lot.get("entry_price") or 0.0)
        pos = positions.setdefault(pdno, {"qty": 0, "avg_price": 0.0, "by_sid": {}})
        pos["qty"] += remaining
        pos["avg_price"] += entry_price * remaining
        by_sid = pos.setdefault("by_sid", {})
        bucket = by_sid.setdefault(str(sid), {"qty": 0, "avg_price": 0.0})
        bucket["qty"] += remaining
        bucket["avg_price"] += entry_price * remaining
    for pos in positions.values():
        qty = pos.get("qty") or 0
        pos["avg_price"] = (pos["avg_price"] / qty) if qty else 0.0
        for sid, bucket in (pos.get("by_sid") or {}).items():
            bqty = bucket.get("qty") or 0
            bucket["avg_price"] = (bucket["avg_price"] / bqty) if bqty else 0.0
    return positions


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
    code_key = normalize_code(pdno)
    if not code_key:
        return
    if any(lot.get("lot_id") == lot_id for lot in lots):
        return
    lots.append(
        {
            "lot_id": lot_id,
            "pdno": code_key,
            "strategy_id": strategy_id,
            "sid": normalize_sid(strategy_id),
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
    pdno_key = normalize_code(pdno)
    if not pdno_key:
        return

    req_sid = _norm_sid(strategy_id)

    def _consume(remaining_qty: int, sid_filter: int | str | None) -> int:
        for lot in lots:
            if normalize_code(lot.get("pdno")) != pdno_key:
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
    pdno_key = normalize_code(pdno)
    if not pdno_key:
        return 0
    total = 0
    for lot in lots:
        if normalize_code(lot.get("pdno")) != pdno_key:
            continue
        if int(lot.get("remaining_qty") or 0) <= 0:
            continue
        if _norm_sid(lot.get("strategy_id")) != _norm_sid(strategy_id):
            continue
        total += int(lot.get("remaining_qty") or 0)
    return total


def dominant_strategy_for(state: Dict[str, Any], pdno: str) -> int | None:
    lots = _ensure_state(state)
    pdno_key = normalize_code(pdno)
    if not pdno_key:
        return None
    totals: Dict[int, int] = {}
    for lot in lots:
        if normalize_code(lot.get("pdno")) != pdno_key:
            continue
        remaining = int(lot.get("remaining_qty") or 0)
        if remaining <= 0:
            continue
        sid = _norm_sid(lot.get("strategy_id"))
        if isinstance(sid, int) and sid in STRATEGY_INT_IDS:
            totals[sid] = totals.get(sid, 0) + remaining
    if not totals:
        return None
    return max(totals.items(), key=lambda item: item[1])[0]


def strategy_avg_price(
    state: Dict[str, Any], pdno: str, strategy_id: int | str
) -> float | None:
    lots = _ensure_state(state)
    pdno_key = normalize_code(pdno)
    if not pdno_key:
        return None
    total_qty = 0
    total_cost = 0.0
    for lot in lots:
        if normalize_code(lot.get("pdno")) != pdno_key:
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
    now_ts = datetime.now(KST)
    normalized_holdings = []
    for row in holdings:
        code = normalize_code(row.get("code") or row.get("pdno") or "")
        if not code:
            continue
        normalized_holdings.append(
            {"pdno": code, "qty": int(row.get("qty") or 0), "avg_price": float(row.get("avg_price") or row.get("pchs_avg_pric") or 0.0)}
        )

    from .order_map_store import load_order_map_index
    from .paths import REPO_ROOT
    from .strategy_recovery import recover_lots_from_sources

    lots, diagnostics = recover_lots_from_sources(
        normalized_holdings,
        state,
        load_order_map_index(),
        load_ledger_entries(),
        sorted((REPO_ROOT / "rebalance_results").glob("*.json")),
        LOG_DIR,
    )
    state["lots"] = lots
    state["positions"] = _summarize_positions(lots)
    state.setdefault("meta", {})["diagnostics"] = diagnostics
    append_ledger_event(
        "RECOVERY",
        {
            "ts": now_ts.isoformat(),
            "holdings": normalized_holdings,
            "stats": diagnostics.get("recovery_stats", {}),
        },
    )
    logger.info(
        "[RECONCILE] holdings=%d lots_after=%d stats=%s",
        len(normalized_holdings),
        len(lots),
        diagnostics.get("recovery_stats", {}),
    )
