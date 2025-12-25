from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .code_utils import normalize_code
from .config import KST
from .io_atomic import atomic_write_json
from .ledger import load_ledger_entries
from .order_map_store import ORDERS_MAP_PATH, append_order_map, load_order_map_index
from .paths import BOT_STATE_MIRROR_DIR, FILLS_DIR, LOG_DIR, REPO_ROOT, STATE_DIR, ensure_dirs
from .strategy_recovery import StrategyRecovery, recover_lots_from_sources
from .strategy_registry import normalize_sid

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 3
STATE_PATH = STATE_DIR / "state.json"


@dataclass
class Holding:
    pdno: str
    qty: int
    avg_price: float


def _blank_state(now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or datetime.now(KST)
    return {
        "schema_version": SCHEMA_VERSION,
        "updated_at": now.isoformat(),
        "lots": [],
        "orders": {},
        "positions": {},
        "meta": {"created_at": now.isoformat(), "schema": "v3"},
    }


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)


def ensure_placeholders() -> None:
    """Create minimal directories/files so runtime and workflows never start empty."""
    ensure_dirs()
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    FILLS_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    if not STATE_PATH.exists():
        atomic_write_json(STATE_PATH, _blank_state())
    _touch(ORDERS_MAP_PATH)
    _touch(LOG_DIR / "ledger.jsonl")
    _touch(LOG_DIR / "engine_events.jsonl")


# Backward compatible name used by trader.py
def ensure_minimum_files() -> None:  # pragma: no cover - alias
    ensure_placeholders()


def _load_json(path: Path) -> Dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else None
    except Exception:
        logger.exception("[STATE] failed to load %s", path)
        return None


def _legacy_candidates() -> List[Path]:
    return [
        STATE_PATH,
        Path("state.json"),
        Path("bot_state/state.json"),
        BOT_STATE_MIRROR_DIR / "state.json",
        BOT_STATE_MIRROR_DIR / "state" / "state.json",
        Path("bot_state/trader_state/state/state.json"),
        Path("bot_state/trader_state/trader/state/state.json"),
    ]


def _normalize_lot_from_legacy(
    lot: Dict[str, Any],
    *,
    asof: datetime,
    recovery: StrategyRecovery,
) -> Dict[str, Any]:
    pdno = normalize_code(lot.get("pdno") or lot.get("code") or "")
    qty = int(lot.get("remaining_qty") or lot.get("qty") or 0)
    avg_price = float(lot.get("avg_price") or lot.get("entry_price") or lot.get("pchs_avg_pric") or 0.0)
    sid_raw = lot.get("strategy_id") or lot.get("sid") or "UNKNOWN"
    sid = normalize_sid(sid_raw)
    meta = {**(lot.get("meta") or {}), "migrated": True}
    if sid in {"UNKNOWN", ""}:
        recovered = recovery.recover(pdno, qty, avg_price, {"source": "legacy"})
        if recovered:
            sid = recovered[0]["sid"]
            meta.update(recovered[0].get("meta", {}))
        else:
            sid = "MANUAL"
    entry_ts = lot.get("entry_ts") or asof.isoformat()
    lot_id = lot.get("lot_id") or f"{pdno}-{sid}-{entry_ts}"
    return {
        "lot_id": lot_id,
        "pdno": pdno,
        "sid": sid,
        "strategy_id": sid,
        "engine": lot.get("engine") or "migrated",
        "entry_ts": entry_ts,
        "entry_price": avg_price,
        "qty": qty,
        "remaining_qty": qty,
        "meta": meta,
    }


def migrate_state_to_v3(old_state: Dict[str, Any]) -> Dict[str, Any]:
    now = datetime.now(KST)
    recovery = StrategyRecovery(now_ts=now)
    migrated = _blank_state(now)
    lots_raw = old_state.get("lots") if isinstance(old_state, dict) else []
    for lot in lots_raw or []:
        if not isinstance(lot, dict):
            continue
        migrated["lots"].append(_normalize_lot_from_legacy(lot, asof=now, recovery=recovery))
    migrated["meta"]["migrated_from"] = old_state.get("schema_version") or old_state.get("version") or "legacy"
    migrated["meta"]["recovery_stats"] = recovery.stats
    return migrated


def load_state_v3(path: Path | None = None) -> Dict[str, Any]:
    ensure_placeholders()
    state_path = path or STATE_PATH
    payload = _load_json(state_path)
    if payload is None:
        for cand in _legacy_candidates():
            if cand == state_path:
                continue
            legacy = _load_json(cand)
            if legacy:
                payload = migrate_state_to_v3(legacy)
                break
    if payload is None:
        payload = _blank_state()
    if int(payload.get("schema_version") or 0) != SCHEMA_VERSION:
        payload = migrate_state_to_v3(payload)
    payload.setdefault("lots", [])
    payload.setdefault("orders", {})
    payload.setdefault("positions", {})
    payload.setdefault("meta", {})
    payload["schema_version"] = SCHEMA_VERSION
    payload["updated_at"] = payload.get("updated_at") or datetime.now(KST).isoformat()
    return payload


# Backward compatible alias
def load_state() -> Dict[str, Any]:
    return load_state_v3()


def save_state_atomic(state: Dict[str, Any], path: Path | None = None) -> None:
    try:
        path = path or STATE_PATH
        payload = dict(state)
        payload["schema_version"] = SCHEMA_VERSION
        payload["updated_at"] = datetime.now(KST).isoformat()
        atomic_write_json(path, payload)
    except Exception:
        logger.exception("[STATE] failed to save %s", path)


def save_state(state: Dict[str, Any]) -> None:  # pragma: no cover - alias for existing callers
    save_state_atomic(state, STATE_PATH)


def _order_bucket(state: Dict[str, Any], symbol: str, side: str) -> Dict[str, Any]:
    orders = state.setdefault("order_windows", {})
    symbol_key = normalize_code(symbol)
    symbol_bucket = orders.setdefault(symbol_key, {})
    return symbol_bucket.setdefault(side.upper(), {})


def should_block_order(
    state: Dict[str, Any],
    symbol: str,
    side: str,
    now_ts: str,
    *,
    window_sec: int = 300,
    max_attempts: int = 2,
) -> bool:
    bucket = _order_bucket(state, symbol, side)
    last_ts = bucket.get("last_ts")
    attempts = int(bucket.get("attempts") or 0)
    if attempts >= max_attempts:
        return True
    if isinstance(last_ts, str):
        try:
            last_dt = datetime.fromisoformat(last_ts)
            now_dt = datetime.fromisoformat(now_ts)
            if (now_dt - last_dt).total_seconds() <= window_sec:
                return True
        except Exception:
            return False
    return False


def mark_order(
    state: Dict[str, Any],
    symbol: str,
    side: str,
    strategy_id: Any,
    qty: int,
    price: float,
    ts: str,
    order_id: str | None = None,
    status: str = "submitted",
    reason: str = "strategy",
    update_window: bool = True,
    run_id: str | None = None,
    rejection_reason: str | None = None,
) -> str:
    pdno = normalize_code(symbol)
    sid = normalize_sid(strategy_id)
    record = {
        "pdno": pdno,
        "sid": sid,
        "side": side.upper(),
        "qty": int(qty),
        "price": float(price),
        "ts": ts,
        "status": status,
        "reason": reason,
    }
    entry = append_order_map(
        order_id,
        pdno,
        sid,
        side,
        qty,
        price,
        reason,
        ts,
        run_id,
        status=status,
        rejection_reason=rejection_reason,
    )
    oid = entry["order_id"]
    if rejection_reason:
        record["rejection_reason"] = rejection_reason
    state.setdefault("orders", {})[oid] = record
    if update_window and str(status).lower() != "rejected":
        bucket = _order_bucket(state, symbol, side)
        bucket["last_ts"] = ts
        bucket["last_order_id"] = oid
        bucket["attempts"] = int(bucket.get("attempts") or 0) + 1
    save_state_atomic(state)
    try:
        from .ledger import append_ledger_event  # lazy import to avoid cycle

        append_ledger_event(
            "ORDER_REJECTED" if str(status).lower() == "rejected" else "ORDER_SUBMITTED",
            {
                "ts": ts,
                "order_id": oid,
                "pdno": pdno,
                "sid": sid,
                "side": side.upper(),
                "qty": int(qty),
                "price": float(price),
                "reason": reason,
                "run_id": run_id,
                "rejection_reason": rejection_reason,
            },
        )
    except Exception:
        logger.debug("[ORDER_LOG] ledger append failed", exc_info=True)
    logger.info(
        "[ORDER_%s] odno=%s pdno=%s sid=%s qty=%s price=%s reason=%s",
        str(status).upper(),
        oid,
        pdno,
        sid,
        qty,
        price,
        reason,
    )
    return oid


def _lot_sid(lot: Dict[str, Any]) -> str:
    return normalize_sid(lot.get("sid") or lot.get("strategy_id"))


def _apply_fill_to_lots(
    lots: List[Dict[str, Any]],
    *,
    pdno: str,
    sid: str,
    side: str,
    qty: int,
    price: float,
    ts: str,
    allow_manual: bool = False,
) -> Tuple[int, int]:
    pdno_key = normalize_code(pdno)
    remaining_before = sum(int(lot.get("remaining_qty") or 0) for lot in lots if normalize_code(lot.get("pdno")) == pdno_key)
    if side.upper() == "BUY":
        lot_id = f"{pdno_key}-{sid}-{ts}"
        meta: Dict[str, Any] = {"reconciled": False, "confidence": 1.0, "sources": ["fill"]}
        lots.append(
            {
                "lot_id": lot_id,
                "pdno": pdno_key,
                "sid": sid,
                "strategy_id": sid,
                "engine": "fill",
                "entry_ts": ts,
                "entry_price": float(price),
                "qty": int(qty),
                "remaining_qty": int(qty),
                "meta": meta,
            }
        )
    else:
        remaining_to_sell = int(qty)
        for lot in sorted(lots, key=lambda x: x.get("entry_ts") or ""):
            if normalize_code(lot.get("pdno")) != pdno_key:
                continue
            lot_sid = _lot_sid(lot)
            if lot_sid != sid and not (lot_sid == "MANUAL" and allow_manual):
                continue
            if lot_sid == "MANUAL" and lot.get("meta", {}).get("sell_blocked") and not allow_manual:
                continue
            lot_remaining = int(lot.get("remaining_qty") or 0)
            if lot_remaining <= 0:
                continue
            delta = min(lot_remaining, remaining_to_sell)
            lot["remaining_qty"] = lot_remaining - delta
            lot["last_sell_ts"] = ts
            remaining_to_sell -= delta
            if remaining_to_sell <= 0:
                break
    remaining_after = sum(int(lot.get("remaining_qty") or 0) for lot in lots if normalize_code(lot.get("pdno")) == pdno_key)
    return remaining_before, remaining_after


def mark_fill(
    state: Dict[str, Any],
    symbol: str,
    side: str,
    strategy_id: Any,
    qty: int,
    price: float,
    ts: str,
    order_id: str | None = None,
    status: str = "filled",
    source: str = "mark_fill",
    run_id: str | None = None,
) -> None:
    pdno = normalize_code(symbol)
    sid = normalize_sid(strategy_id)
    lots = state.setdefault("lots", [])
    if sid == "UNKNOWN" and order_id:
        cached = state.get("orders", {}).get(order_id, {})
        sid = normalize_sid(cached.get("sid") or cached.get("strategy_id"))
        if sid == "UNKNOWN":
            om = load_order_map_index()
            if order_id in om:
                sid = normalize_sid(om[order_id].get("sid"))
    if sid == "UNKNOWN":
        recovery = StrategyRecovery(now_ts=datetime.fromisoformat(ts))
        recovered = recovery.recover(pdno, qty, price, {"source": "fill"})
        sid = recovered[0]["sid"] if recovered else "MANUAL"
    before, after = _apply_fill_to_lots(
        lots,
        pdno=pdno,
        sid=sid,
        side=side,
        qty=qty,
        price=price,
        ts=ts,
        allow_manual=os.getenv("FORCE_SELL_MANUAL") == "1",
    )
    from .ledger import append_ledger_event  # lazy import to avoid cycle

    append_ledger_event(
        event_type="FILL",
        payload={
            "ts": ts,
            "order_id": order_id,
            "pdno": pdno,
            "sid": sid,
            "side": side,
            "qty": qty,
            "price": price,
            "source": source,
            "note": status,
            "run_id": run_id,
        },
    )
    orders = state.setdefault("orders", {})
    if order_id and order_id in orders:
        orders[order_id]["status"] = status
    save_state_atomic(state)
    logger.info(
        "[FILL_APPLIED] odno=%s pdno=%s sid=%s side=%s qty=%s remaining_before=%s remaining_after=%s",
        order_id,
        pdno,
        sid,
        side,
        qty,
        before,
        after,
    )


def _summarize_positions(lots: List[Dict[str, Any]]) -> Dict[str, Any]:
    positions: Dict[str, Any] = {}
    for lot in lots:
        pdno = normalize_code(lot.get("pdno") or "")
        remaining = int(lot.get("remaining_qty") or 0)
        if not pdno or remaining <= 0:
            continue
        sid = _lot_sid(lot)
        entry_price = float(lot.get("entry_price") or 0.0)
        pos = positions.setdefault(pdno, {"qty": 0, "avg_price": 0.0, "by_sid": {}})
        pos["qty"] += remaining
        pos["avg_price"] += entry_price * remaining
        by_sid = pos.setdefault("by_sid", {})
        bucket = by_sid.setdefault(sid, {"qty": 0, "avg_price": 0.0})
        bucket["qty"] += remaining
        bucket["avg_price"] += entry_price * remaining
    for pos in positions.values():
        qty = pos.get("qty") or 0
        pos["avg_price"] = (pos["avg_price"] / qty) if qty else 0.0
        for sid, bucket in (pos.get("by_sid") or {}).items():
            bqty = bucket.get("qty") or 0
            bucket["avg_price"] = (bucket["avg_price"] / bqty) if bqty else 0.0
    return positions


def _normalize_holdings(balance: Dict[str, Any]) -> List[Holding]:
    positions = balance.get("positions") or []
    holdings: List[Holding] = []
    for row in positions:
        pdno = normalize_code(row.get("pdno") or row.get("code") or "")
        if not pdno:
            continue
        qty = int(float(row.get("hldg_qty") or row.get("qty") or 0))
        if qty <= 0:
            continue
        avg_price = float(row.get("pchs_avg_pric") or row.get("avg_price") or 0.0)
        holdings.append(Holding(pdno=pdno, qty=qty, avg_price=avg_price))
    return holdings


def _reduce_excess(lots: List[Dict[str, Any]], pdno: str, target_qty: int) -> int:
    pdno_key = normalize_code(pdno)
    remaining_total = sum(int(lot.get("remaining_qty") or 0) for lot in lots if normalize_code(lot.get("pdno")) == pdno_key)
    excess = remaining_total - target_qty
    if excess <= 0:
        return 0
    for lot in sorted(lots, key=lambda x: x.get("entry_ts") or "", reverse=True):
        if normalize_code(lot.get("pdno")) != pdno_key:
            continue
        lot_remaining = int(lot.get("remaining_qty") or 0)
        if lot_remaining <= 0:
            continue
        delta = min(lot_remaining, excess)
        lot["remaining_qty"] = lot_remaining - delta
        excess -= delta
        if excess <= 0:
            break
    return remaining_total - target_qty


def reconcile_with_kis_balance(
    kis_balance: Dict[str, Any],
    now_ts: Optional[datetime] = None,
    *,
    preferred_strategy: Optional[Dict[str, Any]] = None,
    state_path: Path | None = None,
    state: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Reconcile runtime state with KIS reported holdings using strategy recovery."""

    ensure_placeholders()
    state = state or load_state_v3(state_path)
    now = now_ts or datetime.now(KST)
    holdings = _normalize_holdings(kis_balance)
    order_map_index = load_order_map_index()
    ledger_rows = load_ledger_entries()
    rebalance_dir = REPO_ROOT / "rebalance_results"
    rebalance_files = sorted(rebalance_dir.glob("*.json"))

    lots, diagnostics = recover_lots_from_sources(
        [{"pdno": h.pdno, "qty": h.qty, "avg_price": h.avg_price} for h in holdings],
        state,
        order_map_index,
        ledger_rows,
        rebalance_files,
        LOG_DIR,
        preferred_strategy=preferred_strategy or {},
    )

    state["lots"] = lots
    state["positions"] = _summarize_positions(lots)
    meta = state.setdefault("meta", {})
    meta["recovery_stats"] = diagnostics.get("recovery_stats", {})
    meta["diagnostics"] = diagnostics
    meta.setdefault("created_at", now.isoformat())
    save_state_atomic(state, state_path)
    logger.info(
        "[RECONCILE] holdings=%d lots=%d recovered=%d sources=%s",
        len(holdings),
        len(lots),
        diagnostics.get("recovered") and len(diagnostics["recovered"]),
        {k: v for k, v in diagnostics.items() if k != "recovered"},
    )
    return state
