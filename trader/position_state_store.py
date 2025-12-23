from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
import re
from typing import Any, Dict, Iterable

from .config import KST, STATE_PATH
from .code_utils import normalize_code
from .strategy_ids import SID_BREAKOUT, STRATEGY_INT_IDS
from .state_io import atomic_write_json

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
        code_key = normalize_code(code)
        if code_key and code_key != code:
            positions.pop(code, None)
            positions[code_key] = payload
        elif not code_key:
            positions.pop(code, None)
            continue
        if not isinstance(payload, dict):
            positions[code_key] = {"strategies": {}}
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
            positions[code_key] = {"strategies": strategies}
        else:
            payload.setdefault("strategies", {})
        strategies = positions[code_key].get("strategies")
        if not isinstance(strategies, dict):
            positions[code_key]["strategies"] = {}
            strategies = positions[code_key]["strategies"]
        for sid, entry in list(strategies.items()):
            if not isinstance(entry, dict):
                strategies.pop(sid, None)
                continue
            if sid in {"ORPHAN", "UNKNOWN"}:
                # legacy migration only: map deprecated sid to MANUAL
                strategies.pop(sid, None)
                sid = "MANUAL"
                strategies.setdefault(sid, entry)
            entry.setdefault("qty", 0)
            entry.setdefault("avg_price", 0.0)
            entry.setdefault("entry", {})
            entry.setdefault("meta", {})
            meta = entry["meta"]
            avg_price = float(entry.get("avg_price") or 0.0)
            if not meta.get("high") or float(meta.get("high") or 0.0) <= 0:
                meta["high"] = avg_price
            meta["high"] = max(float(meta.get("high") or 0.0), avg_price)
            entry.setdefault("code", code_key)
            entry.setdefault("sid", str(sid))
            entry.setdefault("engine", entry.get("entry", {}).get("engine") or "reconcile")
            entry.setdefault("entry_ts", entry.get("entry", {}).get("time"))
            entry.setdefault("high_watermark", float(meta.get("high") or avg_price))
            entry["high_watermark"] = max(
                float(entry.get("high_watermark") or 0.0),
                float(meta.get("high") or 0.0),
                avg_price,
            )
            entry.setdefault("last_update_ts", entry.get("entry", {}).get("time"))
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
        payload = _coerce_state(dict(state))
        payload["updated_at"] = datetime.now(KST).isoformat()
        atomic_write_json(path_obj, payload)
    except Exception:
        logger.exception("[STATE] failed to save %s", path_obj)


def load_state(path: str | None = None) -> Dict[str, Any]:
    path_value = path or str(STATE_PATH)
    return load_position_state(path_value)


def save_state_atomic(state: Dict[str, Any], path: str | None = None) -> None:
    path_value = path or str(STATE_PATH)
    save_position_state(path_value, state)


def _orphan_entry(code: str, qty: int, avg_price: float | None) -> Dict[str, Any]:
    now_ts = datetime.now(KST).isoformat()
    return {
        "qty": int(qty),
        "avg_price": float(avg_price or 0.0),
        "entry": {
            "time": now_ts,
            "strategy_id": "MANUAL",
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
        "code": code,
        "sid": "MANUAL",
        "engine": "unknown",
        "entry_ts": now_ts,
        "high_watermark": float(avg_price or 0.0),
        "last_update_ts": now_ts,
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
        code = normalize_code(row.get("code") or row.get("pdno") or "")
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
            if normalize_code(lot.get("pdno")) != code:
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
        code_key = normalize_code(code)
        strategies = _strategies_for_code(code_key)
        if not strategies and broker_map.get(code_key):
            orphan_qty = int(broker_map[code_key].get("qty") or 0)
            if orphan_qty > 0:
                strategies = {"MANUAL": orphan_qty}
                logger.warning(
                    "[STATE] broker has qty but ledger empty: code=%s qty=%s -> MANUAL",
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
                if sid == "MANUAL":
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
                        "code": code_key,
                        "sid": str(sid),
                        "engine": "reconcile",
                        "entry_ts": now_ts,
                        "high_watermark": float(
                            broker_map.get(code_key, {}).get("avg_price") or 0.0
                        ),
                        "last_update_ts": now_ts,
                    }
                entry["flags"] = {
                    "bear_s1_done": False,
                    "bear_s2_done": False,
                    "sold_p1": False,
                    "sold_p2": False,
                }
                entries[sid] = entry
            entry["qty"] = int(qty)
            entry.setdefault("code", code_key)
            entry.setdefault("sid", str(sid))
            entry.setdefault("engine", entry.get("entry", {}).get("engine"))
            entry.setdefault("entry_ts", entry.get("entry", {}).get("time"))
            entry.setdefault("high_watermark", float(entry.get("avg_price") or 0.0))
            entry["last_update_ts"] = datetime.now(KST).isoformat()
        active_codes.add(code_key)

    for code in list(positions.keys()):
        if code not in active_codes:
            positions.pop(code, None)
            memory.get("last_price", {}).pop(code, None)
            memory.get("last_seen", {}).pop(code, None)

    return state


def _normalize_strategy_id(value: Any) -> str | None:
    if value is None:
        return None
    try:
        num = int(value)
    except Exception:
        num = None
    if num is not None and num in STRATEGY_INT_IDS:
        return str(num)
    text = str(value).strip()
    if text.upper().startswith("STRAT_"):
        text = text.split("_", 1)[-1]
    match = re.search(r"([1-5])", text)
    if match:
        return match.group(1)
    return None


def _latest_trade_sid(
    trade_log: Iterable[Dict[str, Any]], code: str
) -> tuple[str | None, str | None]:
    code_key = normalize_code(code)
    for entry in reversed(list(trade_log)):
        if normalize_code(entry.get("code")) != code_key:
            continue
        if str(entry.get("side") or "").upper() != "BUY":
            continue
        status = str(entry.get("status") or "").lower()
        result = entry.get("result") or {}
        if status not in ("", "filled") and not (
            isinstance(result, dict) and result.get("rt_cd") == "0"
        ):
            continue
        sid = _normalize_strategy_id(entry.get("strategy_id"))
        engine = entry.get("engine") or "trade_log"
        return sid, str(engine)
    return None, None


def reconcile_positions(
    kis_holdings: Iterable[Dict[str, Any]],
    state: Dict[str, Any],
    trade_log: Iterable[Dict[str, Any]],
    todays_targets: Iterable[str],
) -> Dict[str, Any]:
    state = _coerce_state(state)
    positions = state["positions"]
    today_tag = datetime.now(KST).strftime("%Y%m%d")
    targets = {normalize_code(code) for code in todays_targets if normalize_code(code)}

    for row in kis_holdings:
        code_key = normalize_code(row.get("code") or row.get("pdno") or "")
        if not code_key:
            continue
        qty = int(row.get("qty") or row.get("hldg_qty") or 0)
        if qty <= 0:
            continue
        avg_price = float(row.get("avg_price") or row.get("pchs_avg_pric") or 0.0)
        pos = positions.setdefault(code_key, {"strategies": {}})
        strategies = pos.setdefault("strategies", {})

        def _fallback_sid() -> tuple[str, str]:
            sid, engine = _latest_trade_sid(trade_log, code_key)
            if sid:
                return sid, engine or "trade_log"
            if code_key in targets:
                return f"REB_{today_tag}", "reconcile"
            return "MANUAL", "reconcile"

        if not strategies:
            sid_key, engine = _fallback_sid()
            now_ts = datetime.now(KST).isoformat()
            strategies[sid_key] = {
                "qty": int(qty),
                "avg_price": float(avg_price),
                "entry": {
                    "time": now_ts,
                    "strategy_id": sid_key,
                    "engine": engine,
                    "entry_reason": "RECONCILE",
                    "order_type": "unknown",
                    "best_k": None,
                    "tgt_px": None,
                    "gap_pct_at_entry": None,
                },
                "meta": {},
                "flags": {
                    "bear_s1_done": False,
                    "bear_s2_done": False,
                    "sold_p1": False,
                    "sold_p2": False,
                },
                "code": code_key,
                "sid": sid_key,
                "engine": engine,
                "entry_ts": now_ts,
                "high_watermark": float(avg_price),
                "last_update_ts": now_ts,
            }

        for sid in list(strategies.keys()):
            if sid in {"ORPHAN", "UNKNOWN"}:
                # legacy migration only: map deprecated sid to MANUAL
                strategies.pop(sid, None)
                sid_key, engine = _fallback_sid()
                now_ts = datetime.now(KST).isoformat()
                strategies[sid_key] = {
                    "qty": int(qty),
                    "avg_price": float(avg_price),
                    "entry": {
                        "time": now_ts,
                        "strategy_id": sid_key,
                        "engine": engine,
                        "entry_reason": "RECONCILE",
                        "order_type": "unknown",
                        "best_k": None,
                        "tgt_px": None,
                        "gap_pct_at_entry": None,
                    },
                    "meta": {},
                    "flags": {
                        "bear_s1_done": False,
                        "bear_s2_done": False,
                        "sold_p1": False,
                        "sold_p2": False,
                    },
                    "code": code_key,
                    "sid": sid_key,
                    "engine": engine,
                    "entry_ts": now_ts,
                    "high_watermark": float(avg_price),
                    "last_update_ts": now_ts,
                }

        total_qty = sum(int(entry.get("qty") or 0) for entry in strategies.values())
        if total_qty != qty:
            if len(strategies) == 1:
                only_entry = next(iter(strategies.values()))
                only_entry["qty"] = int(qty)
                only_entry["avg_price"] = float(avg_price)
            else:
                base_total = total_qty or len(strategies)
                adjusted_total = 0
                entries = list(strategies.values())
                for entry in entries:
                    portion = (int(entry.get("qty") or 0) / base_total) if base_total else 0
                    new_qty = int(round(qty * portion))
                    entry["qty"] = int(new_qty)
                    entry["avg_price"] = float(avg_price) if avg_price else entry.get("avg_price")
                    adjusted_total += new_qty
                diff = int(qty) - adjusted_total
                if diff and entries:
                    entries[0]["qty"] = int(entries[0].get("qty") or 0) + diff

        for sid_key, entry in strategies.items():
            entry.setdefault("code", code_key)
            entry.setdefault("sid", str(sid_key))
            entry.setdefault("engine", entry.get("entry", {}).get("engine") or "reconcile")
            entry.setdefault("entry_ts", entry.get("entry", {}).get("time"))
            entry.setdefault(
                "high_watermark",
                max(float(entry.get("high_watermark") or 0.0), float(avg_price or 0.0)),
            )
            entry["last_update_ts"] = datetime.now(KST).isoformat()
            entry.setdefault(
                "flags",
                {"bear_s1_done": False, "bear_s2_done": False, "sold_p1": False, "sold_p2": False},
            )

    return state


def run_reconcile_self_checks() -> None:
    state = _empty_state()
    lot_state = {
        "lots": [{"pdno": "000001", "strategy_id": SID_BREAKOUT, "remaining_qty": 5}]
    }
    state["positions"]["000001"] = {
        "strategies": {
            str(SID_BREAKOUT): {
                "qty": 7,
                "avg_price": 100.0,
                "entry": {},
                "meta": {},
                "flags": {},
            }
        }
    }
    state = reconcile_with_broker(state, [], lot_state=lot_state)
    assert state["positions"]["000001"]["strategies"][str(SID_BREAKOUT)]["qty"] == 5


if __name__ == "__main__":
    run_reconcile_self_checks()
