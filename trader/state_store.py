from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from .config import KST, STATE_PATH

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 2
RUNTIME_STATE_DIR = Path(".runtime")
RUNTIME_STATE_PATH = RUNTIME_STATE_DIR / "state.json"
_LOT_ID_PREFIX = "LOT"


def _normalize_code(symbol: str | int | None) -> str:
    return str(symbol or "").zfill(6)


def _default_runtime_state() -> Dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "updated_at": None,
        "positions": {},
        "orders": {},
        "lots": [],
        "memory": {"last_price": {}, "last_seen": {}, "last_strategy_id": {}},
    }


def load_state() -> Dict[str, Any]:
    if not RUNTIME_STATE_PATH.exists():
        return _default_runtime_state()
    try:
        with open(RUNTIME_STATE_PATH, "r", encoding="utf-8") as f:
            state = json.load(f)
        if not isinstance(state, dict):
            logger.warning("[RUNTIME_STATE] invalid state format: %s", type(state))
            return _default_runtime_state()
        state.setdefault("schema_version", SCHEMA_VERSION)
        state.setdefault("positions", {})
        state.setdefault("orders", {})
        memory = state.get("memory")
        if not isinstance(memory, dict):
            memory = {}
            state["memory"] = memory
        memory.setdefault("last_price", {})
        memory.setdefault("last_seen", {})
        memory.setdefault("last_strategy_id", {})
        state.setdefault("updated_at", None)
        lots = state.get("lots")
        if not isinstance(lots, list):
            lots = []
            state["lots"] = lots
        positions = state.get("positions")
        if isinstance(positions, dict):
            for sym, payload in list(positions.items()):
                if not isinstance(payload, dict):
                    positions[sym] = {}
        return state
    except Exception:
        logger.exception("[RUNTIME_STATE] failed to load %s", RUNTIME_STATE_PATH)
        return _default_runtime_state()


def save_state(state: Dict[str, Any]) -> None:
    try:
        RUNTIME_STATE_DIR.mkdir(parents=True, exist_ok=True)
        payload = dict(state)
        payload.setdefault("schema_version", SCHEMA_VERSION)
        payload.setdefault("positions", {})
        payload.setdefault("orders", {})
        payload.setdefault("lots", [])
        payload.setdefault("memory", {"last_price": {}, "last_seen": {}, "last_strategy_id": {}})
        payload["updated_at"] = datetime.now(KST).isoformat()
        tmp_path = RUNTIME_STATE_PATH.with_name(f"{RUNTIME_STATE_PATH.name}.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, RUNTIME_STATE_PATH)
        try:
            size = RUNTIME_STATE_PATH.stat().st_size
            logger.info("[STATE][SAVE] path=%s bytes=%d", RUNTIME_STATE_PATH, size)
        except Exception:
            logger.info("[STATE][SAVE] path=%s", RUNTIME_STATE_PATH)
        try:
            STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp_state_path = STATE_PATH.with_name(f"{STATE_PATH.name}.tmp")
            with open(tmp_state_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_state_path, STATE_PATH)
        except Exception:
            logger.exception("[STATE][SAVE] failed to mirror %s", STATE_PATH)
    except Exception:
        logger.exception("[RUNTIME_STATE] failed to save %s", RUNTIME_STATE_PATH)


def _ensure_lots(state: Dict[str, Any]) -> list[dict[str, Any]]:
    lots = state.get("lots")
    if not isinstance(lots, list):
        lots = []
        state["lots"] = lots
    return lots


def _generate_lot_id(code: str, ts: str | None = None) -> str:
    suffix = ts or datetime.now(KST).strftime("%Y%m%d%H%M%S%f")
    return f"{_LOT_ID_PREFIX}-{_normalize_code(code)}-{suffix}-{uuid.uuid4().hex[:6]}"


def _norm_sid(value: Any) -> Any:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return str(value)


def record_lot_open(
    state: Dict[str, Any],
    *,
    code: str,
    sid: Any,
    strategy: str,
    engine: str,
    qty: int,
    entry_price: float,
    entry_ts: str | None = None,
    order_id: str | None = None,
    lot_id: str | None = None,
) -> Dict[str, Any]:
    lots = _ensure_lots(state)
    ts = entry_ts or datetime.now(KST).isoformat()
    lot_identifier = lot_id or _generate_lot_id(code, ts)
    payload = {
        "lot_id": lot_identifier,
        "code": _normalize_code(code),
        "pdno": _normalize_code(code),
        "sid": sid,
        "strategy_id": sid,
        "strategy": strategy,
        "engine": engine,
        "qty": int(qty),
        "remaining_qty": int(qty),
        "entry_ts": ts,
        "entry_price": float(entry_price),
        "order_id": order_id,
        "status": "OPEN",
    }
    lots.append(payload)
    logger.info(
        "[LOT-OPEN] lot_id=%s code=%s sid=%s strategy=%s qty=%s entry_px=%s",
        lot_identifier,
        payload["code"],
        sid,
        strategy,
        qty,
        entry_price,
    )
    return payload


def _apply_sell_to_lots(
    state: Dict[str, Any],
    *,
    code: str,
    qty: int,
    strategy_id: Any = None,
    order_id: str | None = None,
    price: float | None = None,
    ts: str | None = None,
) -> None:
    lots = _ensure_lots(state)
    remaining = int(qty)
    if remaining <= 0:
        return
    ts_val = ts or datetime.now(KST).isoformat()
    target_code = _normalize_code(code)
    sid_filter = _norm_sid(strategy_id)

    def _consume(filter_sid: Any | None, remaining_qty: int) -> int:
        for lot in lots:
            if str(lot.get("status") or "OPEN").upper() != "OPEN":
                continue
            if _normalize_code(lot.get("code") or lot.get("pdno")) != target_code:
                continue
            lot_sid = _norm_sid(lot.get("sid") or lot.get("strategy_id"))
            if filter_sid is not None and lot_sid != filter_sid:
                continue
            lot_rem = int(lot.get("remaining_qty") or lot.get("qty") or 0)
            if lot_rem <= 0:
                continue
            delta = min(lot_rem, remaining_qty)
            lot["remaining_qty"] = int(lot_rem - delta)
            lot["qty"] = int(max(0, int(lot.get("qty") or 0) - delta))
            lot["last_order_id"] = order_id or lot.get("last_order_id")
            lot["last_exit_ts"] = ts_val
            if price is not None:
                lot["last_exit_price"] = float(price)
            if int(lot.get("remaining_qty") or 0) <= 0:
                lot["status"] = "CLOSED"
            remaining_qty -= delta
            if remaining_qty <= 0:
                break
        return remaining_qty

    remaining = _consume(sid_filter, remaining)
    if remaining > 0 and sid_filter is not None:
        remaining = _consume(None, remaining)
    if remaining > 0:
        logger.warning(
            "[RUNTIME_STATE][LOT_SELL_MISMATCH] code=%s sid=%s remaining_unmatched=%s",
            target_code,
            sid_filter,
            remaining,
        )

def get_position(state: Dict[str, Any], symbol: str) -> Dict[str, Any] | None:
    positions = state.get("positions", {})
    if not isinstance(positions, dict):
        return None
    return positions.get(str(symbol).zfill(6))


def upsert_position(state: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    positions = state.setdefault("positions", {})
    key = str(symbol).zfill(6)
    pos = positions.get(key)
    if not isinstance(pos, dict):
        pos = {}
    positions[key] = pos
    return pos


def update_position_fields(state: Dict[str, Any], symbol: str, fields: Dict[str, Any]) -> Dict[str, Any]:
    pos = upsert_position(state, symbol)
    for field, value in fields.items():
        pos[field] = value
    return pos


def _order_bucket(state: Dict[str, Any], symbol: str, side: str) -> Dict[str, Any]:
    orders = state.setdefault("orders", {})
    symbol_key = str(symbol).zfill(6)
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
) -> None:
    bucket = _order_bucket(state, symbol, side)
    bucket["last_ts"] = ts
    bucket["last_order_id"] = order_id
    bucket["attempts"] = int(bucket.get("attempts") or 0) + 1
    update_position_fields(
        state,
        symbol,
        {
            "strategy_id": strategy_id,
            "last_action": side.upper(),
            "last_action_ts": ts,
            "last_order_status": status,
            "last_order_qty": int(qty),
            "last_order_price": float(price),
        },
    )


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
) -> None:
    pos = get_position(state, symbol) or {}
    try:
        if side.upper() == "BUY":
            record_lot_open(
                state,
                code=symbol,
                sid=strategy_id,
                strategy=str(strategy_id),
                engine=str(pos.get("engine") or "unknown"),
                qty=int(qty),
                entry_price=float(price),
                entry_ts=ts,
                order_id=order_id,
            )
        else:
            _apply_sell_to_lots(
                state,
                code=symbol,
                qty=int(qty),
                strategy_id=strategy_id,
                order_id=order_id,
                price=float(price),
                ts=ts,
            )
    except Exception:
        logger.exception("[RUNTIME_STATE] lot update failed for %s", symbol)
    cur_qty = int(pos.get("qty") or 0)
    cur_avg = float(pos.get("avg_price") or 0.0)
    if side.upper() == "BUY":
        total_qty = cur_qty + int(qty)
        avg_price = (
            (cur_avg * cur_qty + float(price) * int(qty)) / total_qty
            if total_qty > 0
            else 0.0
        )
        pos.update({"qty": total_qty, "avg_price": avg_price, "last_buy_ts": ts})
    else:
        pos.update({"qty": max(0, cur_qty - int(qty)), "last_sell_ts": ts})
    pos["strategy_id"] = strategy_id
    pos["last_order_id"] = order_id
    pos["last_action"] = side.upper()
    pos["last_action_ts"] = ts
    pos["last_order_status"] = status
    update_position_fields(state, symbol, pos)
    try:
        if side.upper() == "BUY":
            save_state(state)
    except Exception:
        logger.exception("[RUNTIME_STATE] failed to persist after lot open for %s", symbol)


def reconcile_with_kis_balance(
    state: Dict[str, Any],
    balance: Dict[str, Any],
    *,
    preferred_strategy: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    preferred_strategy = preferred_strategy or {}
    positions = state.setdefault("positions", {})
    lots = _ensure_lots(state)
    open_qty_by_code: Dict[str, int] = {}
    for lot in lots:
        if str(lot.get("status") or "OPEN").upper() != "OPEN":
            continue
        code_key = _normalize_code(lot.get("code") or lot.get("pdno"))
        rem = int(lot.get("remaining_qty") or lot.get("qty") or 0)
        if not code_key or rem <= 0:
            continue
        open_qty_by_code[code_key] = open_qty_by_code.get(code_key, 0) + rem
    balance_positions = balance.get("positions") if isinstance(balance, dict) else None
    if not isinstance(balance_positions, list):
        return state
    seen = set()
    for row in balance_positions:
        symbol = str(row.get("code") or row.get("pdno") or "").zfill(6)
        if not symbol:
            continue
        qty = int(row.get("qty") or 0)
        if qty <= 0:
            continue
        seen.add(symbol)
        pos = upsert_position(state, symbol)
        strategy_id = pos.get("strategy_id") or preferred_strategy.get(symbol) or "UNKNOWN"
        pos.update(
            {
                "strategy_id": strategy_id,
                "qty": qty,
                "avg_price": float(row.get("avg_price") or 0.0),
                "last_action": "RECONCILE",
            }
        )
        open_qty = open_qty_by_code.get(symbol, 0)
        if open_qty < qty:
            orphan_qty = qty - open_qty
            try:
                record_lot_open(
                    state,
                    code=symbol,
                    sid="UNKNOWN",
                    strategy="ORPHAN",
                    engine="reconcile",
                    qty=orphan_qty,
                    entry_price=float(row.get("avg_price") or 0.0),
                    entry_ts=datetime.now(KST).isoformat(),
                )
                logger.warning(
                    "[ORPHAN-LOT] code=%s qty=%s reason=%s",
                    symbol,
                    orphan_qty,
                    "RECONCILE_NO_LOT",
                )
            except Exception:
                logger.exception("[ORPHAN-LOT][FAIL] code=%s qty=%s", symbol, orphan_qty)
    for symbol, pos in list(positions.items()):
        if symbol not in seen:
            pos["qty"] = 0
            pos["last_action"] = "RECONCILE"
            positions[symbol] = pos
    return state


def _default_lot_state() -> Dict[str, Any]:
    return {"version": SCHEMA_VERSION, "lots": [], "updated_at": None}


def load_lot_state(path_json: str) -> Dict[str, Any]:
    path = Path(path_json)
    if not path.exists():
        return _default_lot_state()
    try:
        with open(path, "r", encoding="utf-8") as f:
            state = json.load(f)
        if not isinstance(state, dict):
            logger.warning("[STATE_STORE] invalid state format: %s", type(state))
            return _default_lot_state()
        state.setdefault("version", SCHEMA_VERSION)
        state.setdefault("lots", [])
        state.setdefault("updated_at", None)
        return state
    except Exception:
        logger.exception("[STATE_STORE] failed to load %s", path_json)
        return _default_lot_state()


def save_lot_state(path_json: str, state: Dict[str, Any]) -> None:
    path = Path(path_json)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(state)
        payload.setdefault("version", SCHEMA_VERSION)
        payload.setdefault("lots", [])
        payload["updated_at"] = datetime.now(KST).isoformat()
        tmp_path = path.with_name(f"{path.name}.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)
    except Exception:
        logger.exception("[STATE_STORE] failed to save %s", path_json)
