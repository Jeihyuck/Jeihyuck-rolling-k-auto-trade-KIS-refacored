from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable

from .config import KST

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1


def _empty_state() -> Dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "updated_at": None,
        "positions": {},
        "memory": {"last_price": {}, "last_seen": {}},
    }


def _coerce_state(state: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(state, dict):
        return _empty_state()
    state.setdefault("schema_version", SCHEMA_VERSION)
    state.setdefault("updated_at", None)
    positions = state.get("positions")
    if not isinstance(positions, dict):
        positions = {}
        state["positions"] = positions
    memory = state.get("memory")
    if not isinstance(memory, dict):
        memory = {}
        state["memory"] = memory
    memory.setdefault("last_price", {})
    memory.setdefault("last_seen", {})
    for code, payload in positions.items():
        if not isinstance(payload, dict):
            positions[code] = {
                "entries": {},
                "broker_qty": None,
                "broker_avg_price": None,
                "miss_count": 0,
            }
            continue
        payload.setdefault("entries", {})
        payload.setdefault("flags", {"bear_s1_done": False, "bear_s2_done": False})
        payload.setdefault("broker_qty", None)
        payload.setdefault("broker_avg_price", None)
        payload.setdefault("miss_count", 0)
    return state


def _backup_corrupt(path: Path) -> None:
    timestamp = datetime.now(KST).strftime("%Y%m%d%H%M%S")
    backup = path.with_name(f"{path.name}.broken-{timestamp}")
    try:
        os.replace(path, backup)
    except Exception:
        logger.exception("[STATE] failed to backup corrupt file: %s", path)


def load_position_state(path: str) -> Dict[str, Any]:
    path_obj = Path(path)
    if not path_obj.exists():
        logger.info("[STATE] no file, start fresh path=%s", path_obj)
        return _empty_state()
    try:
        with open(path_obj, "r", encoding="utf-8") as f:
            payload = json.load(f)
        state = _coerce_state(payload)
        logger.info(
            "[STATE] loaded path=%s positions=%s updated_at=%s",
            path_obj,
            len(state.get("positions", {})),
            state.get("updated_at"),
        )
        return state
    except json.JSONDecodeError:
        logger.warning("[STATE] corrupted json, backing up: %s", path_obj)
        _backup_corrupt(path_obj)
        return _empty_state()
    except Exception:
        logger.exception("[STATE] failed to load %s", path_obj)
        return _empty_state()


def save_position_state(path: str, state: Dict[str, Any]) -> None:
    path_obj = Path(path)
    try:
        path_obj.parent.mkdir(parents=True, exist_ok=True)
        payload = _coerce_state(dict(state))
        payload["updated_at"] = datetime.now(KST).isoformat()
        tmp_path = path_obj.with_name(f"{path_obj.name}.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path_obj)
    except Exception:
        logger.exception("[STATE] failed to save %s", path_obj)


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text else ""


def _orphan_entry(code: str, qty: int, avg_price: float | None) -> Dict[str, Any]:
    now_ts = datetime.now(KST).isoformat()
    return {
        "qty": int(qty),
        "avg_price": float(avg_price or 0.0),
        "entry": {
            "time": now_ts,
            "strategy_id": "ORPHAN",
            "engine": "unknown",
            "entry_reason": "RECONCILE",
            "order_type": "unknown",
            "best_k": None,
            "tgt_px": None,
            "gap_pct_at_entry": None,
        },
        "meta": {
            "pullback_peak_price": None,
            "pullback_reversal_price": None,
            "pullback_reason": None,
        },
    }


def reconcile_with_broker(
    state: Dict[str, Any], broker_positions: Iterable[Dict[str, Any]]
) -> Dict[str, Any]:
    state = _coerce_state(state)
    positions = state["positions"]
    memory = state["memory"]

    broker_map: Dict[str, Dict[str, Any]] = {}
    for row in broker_positions:
        code = _normalize_code(row.get("code") or row.get("pdno") or "")
        if not code:
            continue
        qty = int(row.get("qty") or 0)
        if qty <= 0:
            continue
        broker_map[code] = {
            "qty": qty,
            "avg_price": row.get("avg_price"),
        }

    for code, payload in broker_map.items():
        qty = int(payload.get("qty") or 0)
        avg_price = payload.get("avg_price")
        if code not in positions:
            positions[code] = {
                "entries": {},
                "flags": {"bear_s1_done": False, "bear_s2_done": False},
                "broker_qty": int(qty),
                "broker_avg_price": float(avg_price or 0.0),
                "miss_count": 0,
            }
            continue
        pos = positions[code]
        pos["broker_qty"] = int(qty)
        pos["broker_avg_price"] = (
            float(avg_price) if avg_price is not None else pos.get("broker_avg_price")
        )
        pos["miss_count"] = 0

    for code in list(positions.keys()):
        if code in broker_map:
            continue
        pos = positions.get(code) or {}
        miss_count = int(pos.get("miss_count") or 0) + 1
        pos["miss_count"] = miss_count
        positions[code] = pos
        if miss_count >= 3:
            positions.pop(code, None)
            memory.get("last_price", {}).pop(code, None)
            memory.get("last_seen", {}).pop(code, None)

    return state


def run_reconcile_self_checks() -> None:
    state = _empty_state()
    state["positions"]["000001"] = {
        "entries": {},
        "flags": {"bear_s1_done": True, "bear_s2_done": False},
        "broker_qty": 5,
        "broker_avg_price": 100.0,
        "miss_count": 2,
    }
    state = reconcile_with_broker(state, [])
    assert state["positions"]["000001"]["miss_count"] == 3
    state = reconcile_with_broker(state, [])
    assert "000001" not in state["positions"]


if __name__ == "__main__":
    run_reconcile_self_checks()
