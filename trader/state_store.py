from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from .config import KST
from .code_utils import normalize_code
from .state_io import atomic_write_json

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
RUNTIME_STATE_DIR = Path(".runtime")
RUNTIME_STATE_PATH = RUNTIME_STATE_DIR / "state.json"


def _default_runtime_state() -> Dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "updated_at": None,
        "positions": {},
        "orders": {},
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
        state.setdefault("updated_at", None)
        return state
    except Exception:
        logger.exception("[RUNTIME_STATE] failed to load %s", RUNTIME_STATE_PATH)
        return _default_runtime_state()


def save_state(state: Dict[str, Any]) -> None:
    try:
        payload = dict(state)
        payload.setdefault("schema_version", SCHEMA_VERSION)
        payload.setdefault("positions", {})
        payload.setdefault("orders", {})
        payload["updated_at"] = datetime.now(KST).isoformat()
        atomic_write_json(RUNTIME_STATE_PATH, payload)
    except Exception:
        logger.exception("[RUNTIME_STATE] failed to save %s", RUNTIME_STATE_PATH)


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
    orders = state.setdefault("orders", {})
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
) -> None:
    bucket = _order_bucket(state, symbol, side)
    bucket["last_ts"] = ts
    bucket["last_order_id"] = order_id
    bucket["attempts"] = int(bucket.get("attempts") or 0) + 1
    upsert_position(
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
    upsert_position(state, symbol, pos)


def reconcile_with_kis_balance(
    state: Dict[str, Any],
    balance: Dict[str, Any],
    *,
    preferred_strategy: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    preferred_strategy = preferred_strategy or {}
    positions = state.setdefault("positions", {})
    balance_positions = balance.get("positions") if isinstance(balance, dict) else None
    if not isinstance(balance_positions, list):
        return state
    seen = set()
    for row in balance_positions:
        symbol = normalize_code(row.get("code") or row.get("pdno") or "")
        if not symbol:
            continue
        qty = int(row.get("qty") or 0)
        if qty <= 0:
            continue
        seen.add(symbol)
        pos = positions.setdefault(symbol, {})
        strategy_id = pos.get("strategy_id") or preferred_strategy.get(symbol) or "MANUAL"
        pos.update(
            {
                "strategy_id": strategy_id,
                "qty": qty,
                "avg_price": float(row.get("avg_price") or 0.0),
                "last_action": "RECONCILE",
            }
        )
        positions[symbol] = pos
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
