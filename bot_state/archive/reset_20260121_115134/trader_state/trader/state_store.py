from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .code_utils import normalize_code
from .config import KST
from .fill_store import append_fill as append_fill_jsonl
from .io_atomic import atomic_write_json
from .order_map_store import ORDERS_MAP_PATH, append_order_map, load_order_map_index
from .paths import BOT_STATE_MIRROR_DIR, LOG_DIR, STATE_DIR, ensure_dirs
from .strategy_recovery import recover_sid_for_holding
from .strategy_registry import normalize_sid

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 2
STATE_PATH = STATE_DIR / "state.json"


def _default_state() -> Dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "updated_at": datetime.now(KST).isoformat(),
        "lots": [],
        "orders": {},
        "positions": {},
        "meta": {"created_by": "state_store"},
    }


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)


def ensure_minimum_files() -> None:
    ensure_dirs()
    if not STATE_PATH.exists():
        save_state(_default_state())
    _touch(ORDERS_MAP_PATH)
    _touch(LOG_DIR / "ledger.jsonl")


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


def _normalize_lot_from_legacy(lot: Dict[str, Any], asof: datetime, evidence: Dict[str, Any]) -> Dict[str, Any]:
    pdno = normalize_code(lot.get("pdno") or lot.get("code") or "")
    qty = int(lot.get("remaining_qty") or lot.get("qty") or 0)
    avg_price = float(lot.get("avg_price") or lot.get("entry_price") or 0.0)
    sid = normalize_sid(lot.get("strategy_id") or lot.get("sid"))
    if sid in {"UNKNOWN", ""}:
        sid, conf, reasons = recover_sid_for_holding(pdno, qty, avg_price, asof, evidence)
        logger.info("[STATE_MIGRATE] recovered sid=%s conf=%.2f reasons=%s pdno=%s qty=%s", sid, conf, reasons, pdno, qty)
    entry_ts = lot.get("entry_ts") or asof.isoformat()
    meta = {"migrated": True, **(lot.get("meta") or {})}
    if sid == "MANUAL":
        meta["sell_blocked"] = True
    return {
        "lot_id": lot.get("lot_id") or f"{pdno}-{sid}-{entry_ts}",
        "pdno": pdno,
        "strategy_id": sid,
        "engine": lot.get("engine") or "migrated",
        "entry_ts": entry_ts,
        "entry_price": avg_price,
        "qty": qty,
        "remaining_qty": qty,
        "meta": meta,
    }


def _migrate_legacy_state() -> Dict[str, Any]:
    candidates = [
        Path("bot_state/state.json"),
        BOT_STATE_MIRROR_DIR / "state.json",
        BOT_STATE_MIRROR_DIR / "state" / "state.json",
    ]
    asof = datetime.now(KST)
    evidence = {
        "orders_map": ORDERS_MAP_PATH,
        "ledger_path": LOG_DIR / "ledger.jsonl",
        "fills_dir": STATE_DIR.parent / "fills",
        "log_dir": LOG_DIR,
        "rebalance_dir": Path("rebalance_results"),
    }
    for path in candidates:
        payload = _load_json(path)
        if not payload:
            continue
        lots_raw = payload.get("lots") or []
        migrated_lots = [_normalize_lot_from_legacy(lot, asof, evidence) for lot in lots_raw if isinstance(lot, dict)]
        state = _default_state()
        state["lots"] = migrated_lots
        state["meta"]["migrated_from"] = str(path)
        save_state(state)
        logger.info("[STATE_MIGRATE] migrated legacy lots=%d from %s", len(migrated_lots), path)
        return state
    return _default_state()


def load_state() -> Dict[str, Any]:
    ensure_dirs()
    payload = _load_json(STATE_PATH)
    if payload is None:
        payload = _migrate_legacy_state()
    payload.setdefault("schema_version", SCHEMA_VERSION)
    payload.setdefault("lots", [])
    payload.setdefault("orders", {})
    payload.setdefault("positions", {})
    payload.setdefault("meta", {})
    payload["updated_at"] = payload.get("updated_at") or datetime.now(KST).isoformat()

    # Normalize any lingering UNKNOWN to MANUAL
    for lot in payload.get("lots", []):
        sid = normalize_sid(lot.get("strategy_id"))
        if sid == "UNKNOWN":
            lot["strategy_id"] = "MANUAL"
            lot.setdefault("meta", {})["sell_blocked"] = True
    return payload


def save_state(state: Dict[str, Any]) -> None:
    try:
        payload = dict(state)
        payload["schema_version"] = SCHEMA_VERSION
        payload["updated_at"] = datetime.now(KST).isoformat()
        atomic_write_json(STATE_PATH, payload)
    except Exception:
        logger.exception("[STATE] failed to save %s", STATE_PATH)


def get_position(state: Dict[str, Any], symbol: str) -> Dict[str, Any] | None:
    positions = state.get("positions", {})
    if not isinstance(positions, dict):
        return None
    return positions.get(normalize_code(symbol))


def upsert_position(state: Dict[str, Any], symbol: str, fields: Dict[str, Any]) -> None:
    positions = state.setdefault("positions", {})
    key = normalize_code(symbol)
    pos = positions.setdefault(key, {})
    for field, value in fields.items():
        pos[field] = value


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
    run_id: str | None = None,
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
    entry = append_order_map(order_id, pdno, sid, side, qty, price, reason, ts, run_id)
    oid = entry["order_id"]
    state.setdefault("orders", {})[oid] = record
    bucket = _order_bucket(state, symbol, side)
    bucket["last_ts"] = ts
    bucket["last_order_id"] = oid
    bucket["attempts"] = int(bucket.get("attempts") or 0) + 1
    save_state(state)
    logger.info("[ORDER_SENT] odno=%s pdno=%s sid=%s qty=%s price=%s reason=%s", oid, pdno, sid, qty, price, reason)
    return oid


def _apply_fill_to_lots(
    lots: List[Dict[str, Any]],
    *,
    pdno: str,
    sid: str,
    side: str,
    qty: int,
    price: float,
    ts: str,
) -> Tuple[int, int]:
    pdno_key = normalize_code(pdno)
    remaining_before = sum(int(lot.get("remaining_qty") or 0) for lot in lots if normalize_code(lot.get("pdno")) == pdno_key)
    if side.upper() == "BUY":
        lot_id = f"{pdno_key}-{sid}-{ts}"
        meta: Dict[str, Any] = {}
        if sid == "MANUAL":
            meta["sell_blocked"] = True
        lots.append(
            {
                "lot_id": lot_id,
                "pdno": pdno_key,
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
        allow_manual = os.getenv("FORCE_SELL_MANUAL") == "1"
        remaining_to_sell = int(qty)
        for lot in lots:
            if normalize_code(lot.get("pdno")) != pdno_key:
                continue
            lot_sid = normalize_sid(lot.get("strategy_id"))
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
        try:
            asof = datetime.fromisoformat(ts)
        except Exception:
            asof = datetime.now(KST)
        recovered_sid, conf, reasons = recover_sid_for_holding(
            pdno,
            qty,
            price,
            asof,
            {"state_lots": lots},
        )
        if recovered_sid != "MANUAL" and conf >= 0.80:
            sid = recovered_sid
        else:
            sid = "MANUAL"
        logger.info("[FILL_RECOVERY] pdno=%s sid=%s conf=%.2f reasons=%s", pdno, sid, conf, reasons)
    before, after = _apply_fill_to_lots(
        lots,
        pdno=pdno,
        sid=sid,
        side=side,
        qty=qty,
        price=price,
        ts=ts,
    )
    append_fill_jsonl(
        ts=ts,
        order_id=order_id,
        pdno=pdno,
        sid=sid,
        side=side,
        qty=qty,
        price=price,
        source=source,
        note=status,
        run_id=run_id,
    )
    orders = state.setdefault("orders", {})
    if order_id and order_id in orders:
        orders[order_id]["status"] = status
    save_state(state)
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
