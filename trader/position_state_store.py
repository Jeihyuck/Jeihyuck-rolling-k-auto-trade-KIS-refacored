from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable

from .config import KST

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 2


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
    state["schema_version"] = SCHEMA_VERSION
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
    for code, payload in list(positions.items()):
        if not isinstance(payload, dict):
            positions[code] = {"strategies": {}}
            continue
        if "strategies" not in payload and "entries" in payload:
            entries = payload.get("entries") or {}
            flags = payload.get("flags") or {}
            strategies: Dict[str, Any] = {}
            if isinstance(entries, dict):
                for sid, entry in entries.items():
                    if not isinstance(entry, dict):
                        continue
                    strategies[str(sid)] = {
                        "qty": int(entry.get("qty") or 0),
                        "avg_price": float(entry.get("avg_price") or 0.0),
                        "entry": entry.get("entry") or {},
                        "meta": entry.get("meta") or {},
                        "flags": {
                            "bear_s1_done": bool(flags.get("bear_s1_done", False)),
                            "bear_s2_done": bool(flags.get("bear_s2_done", False)),
                            "sold_p1": bool(entry.get("sold_p1", False)),
                            "sold_p2": bool(entry.get("sold_p2", False)),
                        },
                    }
            positions[code] = {"strategies": strategies}
        else:
            payload.setdefault("strategies", {})
        strategies = positions[code].get("strategies")
        if not isinstance(strategies, dict):
            positions[code]["strategies"] = {}
            strategies = positions[code]["strategies"]
        for sid, entry in list(strategies.items()):
            if not isinstance(entry, dict):
                strategies.pop(sid, None)
                continue
            entry.setdefault("qty", 0)
            entry.setdefault("avg_price", 0.0)
            entry.setdefault("entry", {})
            entry.setdefault("meta", {})
            meta = entry["meta"]
            avg_price = float(entry.get("avg_price") or 0.0)
            if not meta.get("high") or float(meta.get("high") or 0.0) <= 0:
                meta["high"] = avg_price
            meta["high"] = max(float(meta.get("high") or 0.0), avg_price)
            entry.setdefault(
                "flags",
                {"bear_s1_done": False, "bear_s2_done": False, "sold_p1": False, "sold_p2": False},
            )
    return state


def migrate_position_state(state: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(state, dict):
        return _empty_state()
    schema_version = int(state.get("schema_version") or 1)
    if schema_version >= SCHEMA_VERSION:
        return state
    positions = state.get("positions")
    if not isinstance(positions, dict):
        state["positions"] = {}
        state["schema_version"] = SCHEMA_VERSION
        return state
    for code, payload in list(positions.items()):
        if not isinstance(payload, dict):
            positions[code] = {"strategies": {}}
            continue
        if "strategies" in payload:
            continue
        entries = payload.get("entries") or {}
        legacy_flags = payload.get("flags") or {}
        strategies: Dict[str, Any] = {}
        if isinstance(entries, dict) and entries:
            for sid, entry in entries.items():
                if not isinstance(entry, dict):
                    continue
                entry_meta = entry.get("meta") or {}
                avg_price = float(entry.get("avg_price") or 0.0)
                entry_meta.setdefault("high", avg_price)
                entry_meta["high"] = max(float(entry_meta.get("high") or 0.0), avg_price)
                strategies[str(sid)] = {
                    "qty": int(entry.get("qty") or 0),
                    "avg_price": avg_price,
                    "entry": entry.get("entry") or {},
                    "meta": entry_meta,
                    "flags": {
                        "bear_s1_done": bool(legacy_flags.get("bear_s1_done", False)),
                        "bear_s2_done": bool(legacy_flags.get("bear_s2_done", False)),
                        "sold_p1": bool(entry.get("sold_p1", False)),
                        "sold_p2": bool(entry.get("sold_p2", False)),
                    },
                }
        else:
            strategies["1"] = {
                "qty": 0,
                "avg_price": 0.0,
                "entry": {},
                "meta": {"high": 0.0},
                "flags": {
                    "bear_s1_done": bool(legacy_flags.get("bear_s1_done", False)),
                    "bear_s2_done": bool(legacy_flags.get("bear_s2_done", False)),
                    "sold_p1": False,
                    "sold_p2": False,
                },
            }
        positions[code] = {"strategies": strategies}
    state["schema_version"] = SCHEMA_VERSION
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
        migrated = migrate_position_state(payload)
        state = _coerce_state(migrated)
        if int(payload.get("schema_version") or 1) < SCHEMA_VERSION:
            save_position_state(path, state)
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
    state: Dict[str, Any],
    broker_positions: Iterable[Dict[str, Any]],
    *,
    lot_state: Dict[str, Any],
) -> Dict[str, Any]:
    from .ledger import remaining_qty_for_strategy

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

    def _strategies_for_code(code: str) -> Dict[str, int]:
        strategies: Dict[str, int] = {}
        lots = lot_state.get("lots", [])
        if not isinstance(lots, list):
            return strategies
        for lot in lots:
            if _normalize_code(lot.get("pdno")) != code:
                continue
            remaining = int(lot.get("remaining_qty") or 0)
            if remaining <= 0:
                continue
            sid = lot.get("strategy_id")
            if sid is None:
                continue
            key = str(sid)
            strategies[key] = strategies.get(key, 0) + remaining
        return strategies

    active_codes = set()
    for code in set(list(broker_map.keys()) + list(positions.keys())):
        code_key = _normalize_code(code)
        strategies = _strategies_for_code(code_key)
        if not strategies and broker_map.get(code_key):
            orphan_qty = int(broker_map[code_key].get("qty") or 0)
            if orphan_qty > 0:
                strategies = {"ORPHAN": orphan_qty}
                logger.warning(
                    "[STATE] broker has qty but ledger empty: code=%s qty=%s -> ORPHAN",
                    code_key,
                    orphan_qty,
                )
        if not strategies:
            positions.pop(code_key, None)
            memory.get("last_price", {}).pop(code_key, None)
            memory.get("last_seen", {}).pop(code_key, None)
            continue

        pos = positions.setdefault(code_key, {"strategies": {}})
        entries = pos.setdefault("strategies", {})
        for sid, entry in list(entries.items()):
            if sid not in strategies:
                entries.pop(sid, None)
                continue
            if not isinstance(entry, dict):
                entries.pop(sid, None)
                continue
            ledger_qty = int(remaining_qty_for_strategy(lot_state, code_key, sid))
            if int(entry.get("qty") or 0) > ledger_qty:
                logger.warning(
                    "[STATE] qty exceeds ledger: code=%s sid=%s state=%s ledger=%s",
                    code_key,
                    sid,
                    entry.get("qty"),
                    ledger_qty,
                )
                entry["qty"] = int(ledger_qty)

        for sid, qty in strategies.items():
            entry = entries.get(sid)
            if not isinstance(entry, dict):
                if sid == "ORPHAN":
                    entry = _orphan_entry(
                        code_key, qty, broker_map.get(code_key, {}).get("avg_price")
                    )
                else:
                    now_ts = datetime.now(KST).isoformat()
                    entry = {
                        "qty": int(qty),
                        "avg_price": float(
                            broker_map.get(code_key, {}).get("avg_price") or 0.0
                        ),
                        "entry": {
                            "time": now_ts,
                            "strategy_id": sid,
                            "engine": "reconcile",
                            "entry_reason": "RECONCILE",
                            "order_type": "unknown",
                            "best_k": None,
                            "tgt_px": None,
                            "gap_pct_at_entry": None,
                        },
                        "meta": {},
                    }
                entry["flags"] = {
                    "bear_s1_done": False,
                    "bear_s2_done": False,
                    "sold_p1": False,
                    "sold_p2": False,
                }
                entries[sid] = entry
            entry["qty"] = int(qty)
        active_codes.add(code_key)

    for code in list(positions.keys()):
        if code not in active_codes:
            positions.pop(code, None)
            memory.get("last_price", {}).pop(code, None)
            memory.get("last_seen", {}).pop(code, None)

    return state


def run_reconcile_self_checks() -> None:
    state = _empty_state()
    lot_state = {
        "lots": [{"pdno": "000001", "strategy_id": 1, "remaining_qty": 5}]
    }
    state["positions"]["000001"] = {
        "strategies": {
            "1": {
                "qty": 7,
                "avg_price": 100.0,
                "entry": {},
                "meta": {},
                "flags": {},
            }
        }
    }
    state = reconcile_with_broker(state, [], lot_state=lot_state)
    assert state["positions"]["000001"]["strategies"]["1"]["qty"] == 5


if __name__ == "__main__":
    run_reconcile_self_checks()
