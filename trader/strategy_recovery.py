from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .code_utils import normalize_code
from .config import KST
from .fill_store import load_fills_index
from .order_map_store import ORDERS_MAP_PATH, load_order_map_index
from .paths import LOG_DIR, REPO_ROOT
from .strategy_registry import normalize_sid

logger = logging.getLogger(__name__)

RECOVERY_SCHEMA_VERSION = 1


@dataclass
class RecoveredLot:
    sid: str
    qty: int
    entry_price: float
    meta: Dict[str, Any] = field(default_factory=dict)


def _normalize_qty(value: Any) -> int:
    try:
        return int(float(value))
    except Exception:
        return 0


def _load_rebalance_candidates(rebalance_files: Sequence[Path]) -> Dict[str, str]:
    """Return pdno -> sid map from rebalance result files (latest file wins)."""
    mapping: Dict[str, str] = {}
    for path in rebalance_files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for sid_raw, rows in payload.items():
            if not isinstance(rows, list):
                continue
            sid = normalize_sid(sid_raw if str(sid_raw).upper().startswith("S") else f"S{sid_raw}")
            for row in rows:
                code = normalize_code(row.get("code") or row.get("pdno") or "")
                if code:
                    mapping[code] = sid
    return mapping


def _normalize_holdings(kis_positions: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    holdings: List[Dict[str, Any]] = []
    for row in kis_positions or []:
        pdno = normalize_code(row.get("pdno") or row.get("code") or "")
        if not pdno:
            continue
        qty = _normalize_qty(row.get("hldg_qty") or row.get("qty"))
        if qty <= 0:
            continue
        avg_price = float(row.get("pchs_avg_pric") or row.get("avg_price") or 0.0)
        holdings.append({"pdno": pdno, "qty": qty, "avg_price": avg_price})
    return holdings


def recover_lots_from_sources(
    kis_positions: Iterable[Dict[str, Any]],
    state: Dict[str, Any],
    orders_map: Dict[str, Dict[str, Any]],
    ledger_rows: List[Dict[str, Any]],
    rebalance_files: Sequence[Path],
    logs_dir: Path,
    *,
    preferred_strategy: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Recover lots per KIS holding using multi-source hints.

    Priority: orders_map -> ledger -> rebalance -> logs -> manual fallback.
    """

    now = datetime.now(KST)
    holdings = _normalize_holdings(kis_positions)
    diagnostics: Dict[str, Any] = {"recovered": {}, "missing": []}
    recovered_lots: List[Dict[str, Any]] = []

    engine_events = _load_json_lines(logs_dir / "engine_events.jsonl")
    rebalance_map = _load_rebalance_candidates(rebalance_files)

    recovery_helper = StrategyRecovery(
        now_ts=now,
        preferred_strategy=preferred_strategy,
    )
    recovery_helper.orders_map = orders_map or {}
    recovery_helper.ledger_rows = ledger_rows or []
    recovery_helper.engine_events = engine_events

    for holding in holdings:
        pdno = holding["pdno"]
        target_qty = int(holding.get("qty") or 0)
        avg_price = float(holding.get("avg_price") or 0.0)
        remaining = target_qty
        pd_diagnostics = {"target_qty": target_qty, "steps": []}

        def _append_lot(sid: str, qty: int, source: str, confidence: float, meta_extra: Optional[Dict[str, Any]] = None) -> None:
            nonlocal remaining
            if qty <= 0 or remaining <= 0:
                return
            take = min(qty, remaining)
            lot_meta = {
                "recovery_source": source,
                "recovery_confidence": float(confidence),
                "confidence": float(confidence),
                "created_at": now.isoformat(),
                "last_update": now.isoformat(),
                "reconciled": True,
            }
            lot_meta.update(meta_extra or {})
            if sid in {"MANUAL", "UNKNOWN"}:
                lot_meta.setdefault("safe_exit_required", True)
            lot_id = f"{pdno}-{sid}-{int(now.timestamp())}-{len(recovered_lots)}"
            recovered_lots.append(
                {
                    "lot_id": lot_id,
                    "pdno": pdno,
                    "sid": sid,
                    "strategy_id": sid,
                    "engine": "recovery",
                    "entry_ts": now.isoformat(),
                    "entry_price": avg_price,
                    "qty": take,
                    "remaining_qty": take,
                    "meta": lot_meta,
                }
            )
            remaining -= take
            pd_diagnostics["steps"].append({"source": source, "sid": sid, "qty": take, "confidence": confidence})

        # 1) orders_map
        order_rows = [
            row
            for row in (orders_map or {}).values()
            if normalize_code(row.get("pdno") or "") == pdno
            and str(row.get("side") or "").upper() == "BUY"
            and str(row.get("status") or "submitted").lower() != "rejected"
        ]
        order_rows.sort(key=lambda r: r.get("ts") or r.get("timestamp") or "")
        for row in order_rows:
            qty = _normalize_qty(row.get("qty"))
            sid = normalize_sid(row.get("sid"))
            price = float(row.get("price") or avg_price)
            _append_lot(sid, qty, "orders_map", 0.95, {"order_price": price})
            if remaining <= 0:
                break

        # 2) ledger rows (BUY/FILL)
        if remaining > 0:
            ledger_candidates = []
            for row in ledger_rows or []:
                code = normalize_code(row.get("code") or row.get("pdno") or "")
                if code != pdno:
                    continue
                side = str(row.get("side") or row.get("event") or "").upper()
                event = str(row.get("event") or "").upper()
                if side not in {"BUY", ""} and event not in {"FILL", "TRADE"}:
                    continue
                qty = _normalize_qty(row.get("qty") or row.get("remaining_qty"))
                if qty <= 0:
                    continue
                ts_val = row.get("ts") or row.get("timestamp") or ""
                ledger_candidates.append((ts_val, row))
            ledger_candidates.sort(key=lambda x: x[0])
            for _ts, row in ledger_candidates:
                qty = _normalize_qty(row.get("qty"))
                sid = normalize_sid(row.get("sid") or row.get("strategy_id"))
                _append_lot(sid, qty, "ledger", 0.82, {"ledger_event": row.get("event")})
                if remaining <= 0:
                    break

        # 3) rebalance hint
        if remaining > 0 and pdno in rebalance_map:
            sid = rebalance_map[pdno]
            _append_lot(sid, remaining, "rebalance", 0.40, {"hint": "rebalance_results"})

        # 4) engine logs hint
        if remaining > 0:
            for row in reversed(engine_events):
                code = normalize_code(row.get("pdno") or row.get("code") or "")
                if code != pdno:
                    continue
                side = str(row.get("side") or "").upper()
                if side and side != "BUY":
                    continue
                sid = normalize_sid(row.get("sid") or row.get("strategy_id"))
                _append_lot(sid, remaining, "engine_logs", 0.35, {"event": row.get("event")})
                break

        # 5) fallback to heuristic/manual
        if remaining > 0:
            lots = recovery_helper.recover(pdno, remaining, avg_price, {"source": "fallback"})
            if not lots:
                lots = [
                    {
                        "sid": "MANUAL",
                        "qty": remaining,
                        "entry_price": avg_price,
                        "meta": {"recovery_source": "none", "recovery_confidence": 0.0, "safe_exit_required": True},
                    }
                ]
            for lot in lots:
                _append_lot(
                    normalize_sid(lot.get("sid")),
                    _normalize_qty(lot.get("qty") or remaining),
                    lot.get("meta", {}).get("recovery_source", "heuristic"),
                    float(lot.get("meta", {}).get("recovery_confidence", lot.get("meta", {}).get("confidence", 0.5))),
                    lot.get("meta"),
                )

        if remaining > 0:
            pd_diagnostics["remaining"] = remaining
            diagnostics["missing"].append({"pdno": pdno, "remaining": remaining})
        diagnostics["recovered"][pdno] = pd_diagnostics

    diagnostics["recovery_stats"] = recovery_helper.stats
    return recovered_lots, diagnostics


def _load_json_lines(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
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
        logger.exception("[RECOVERY] failed reading %s", path)
    return rows


class StrategyRecovery:
    """Multi-source recovery of strategy id (sid) for UNKNOWN/MANUAL holdings."""

    def __init__(self, now_ts: Optional[datetime] = None, *, preferred_strategy: Optional[Dict[str, Any]] = None) -> None:
        self.now_ts = now_ts or datetime.now()
        self.preferred_strategy = preferred_strategy or {}
        self.stats: Dict[str, int] = {rule: 0 for rule in ["A", "B", "C", "D", "E", "F"]}
        self.orders_map = load_order_map_index(ORDERS_MAP_PATH)
        self.ledger_rows = _load_json_lines(LOG_DIR / "ledger.jsonl")
        self.engine_events = _load_json_lines(LOG_DIR / "engine_events.jsonl")
        self.rebalance_dir = REPO_ROOT / "rebalance_results"
        self.fills_rows = load_fills_index()

    def _candidate_from_fills(self, pdno: str) -> Tuple[Optional[str], float, Dict[str, Any]]:
        pdno_key = normalize_code(pdno)
        latest_ts: Optional[datetime] = None
        qty_by_sid: Dict[str, int] = {}
        evidence: Dict[str, Any] = {}
        for row in self.fills_rows:
            code = normalize_code(row.get("pdno") or row.get("code") or "")
            if code != pdno_key:
                continue
            if str(row.get("side") or "").upper() != "BUY":
                continue
            ts_val = row.get("ts") or row.get("timestamp")
            try:
                ts = datetime.fromisoformat(str(ts_val))
            except Exception:
                ts = None
            sid = normalize_sid(row.get("sid") or row.get("strategy_id"))
            if sid == "UNKNOWN":
                oid = row.get("order_id") or row.get("client_order_id")
                if oid and oid in self.orders_map:
                    om_row = self.orders_map[oid]
                    if str(om_row.get("status") or "").lower() != "rejected":
                        sid = normalize_sid(om_row.get("sid"))
                elif isinstance(oid, str) and oid.startswith("client-") and "-" in oid:
                    sid = normalize_sid(oid.split("-")[1])
            qty_by_sid[sid] = qty_by_sid.get(sid, 0) + int(row.get("qty") or 0)
            if ts and (latest_ts is None or ts > latest_ts):
                latest_ts = ts
        if not qty_by_sid:
            return None, 0.0, evidence
        best_sid = max(qty_by_sid.items(), key=lambda kv: kv[1])[0]
        confidence = 0.95
        evidence = {"qty_by_sid": qty_by_sid, "source": "fills"}
        return best_sid, confidence, evidence

    def _candidate_from_orders(self, pdno: str) -> Tuple[Optional[str], float, Dict[str, Any]]:
        pdno_key = normalize_code(pdno)
        lookback = self.now_ts - timedelta(days=30)
        qty_by_sid: Dict[str, int] = {}
        for payload in self.orders_map.values():
            code = normalize_code(payload.get("pdno") or "")
            if code != pdno_key:
                continue
            status = str(payload.get("status") or "submitted").lower()
            if status == "rejected":
                continue
            if str(payload.get("side") or "").upper() != "BUY":
                continue
            try:
                ts_val = payload.get("ts") or payload.get("timestamp")
                ts = datetime.fromisoformat(str(ts_val))
            except Exception:
                ts = None
            if ts and ts < lookback:
                continue
            sid = normalize_sid(payload.get("sid"))
            qty_by_sid[sid] = qty_by_sid.get(sid, 0) + int(payload.get("qty") or 0)
        if not qty_by_sid:
            return None, 0.0, {}
        best_sid = max(qty_by_sid.items(), key=lambda kv: kv[1])[0]
        return best_sid, 0.80, {"qty_by_sid": qty_by_sid, "source": "orders_map"}

    def _candidate_from_ledger(self, pdno: str) -> Tuple[Optional[str], float, Dict[str, Any]]:
        pdno_key = normalize_code(pdno)
        latest_ts: Optional[datetime] = None
        chosen_sid: Optional[str] = None
        for row in self.ledger_rows:
            code = normalize_code(row.get("code") or row.get("pdno") or "")
            if code != pdno_key:
                continue
            side = str(row.get("side") or "").upper()
            if side != "BUY":
                continue
            sid = normalize_sid(row.get("strategy_id") or row.get("sid"))
            try:
                ts_val = row.get("timestamp") or row.get("ts")
                ts = datetime.fromisoformat(str(ts_val))
            except Exception:
                ts = None
            if latest_ts is None or (ts and ts > latest_ts):
                latest_ts = ts
                chosen_sid = sid
        if not chosen_sid:
            return None, 0.0, {}
        return chosen_sid, 0.82, {"source": "ledger"}

    def _candidate_from_rebalance(self, pdno: str) -> Tuple[Optional[str], float, Dict[str, Any]]:
        pdno_key = normalize_code(pdno)
        files = sorted(self.rebalance_dir.glob("*.json"))
        if not files:
            return None, 0.0, {}
        latest = files[-1]
        try:
            payload = json.loads(latest.read_text(encoding="utf-8"))
        except Exception:
            return None, 0.0, {}
        for sid_raw, rows in payload.items():
            if not isinstance(rows, list):
                continue
            for row in rows:
                code = normalize_code(row.get("code") or row.get("pdno") or "")
                if code == pdno_key:
                    sid = normalize_sid(sid_raw if str(sid_raw).upper().startswith("S") else f"S{sid_raw}")
                    return sid, 0.65, {"file": latest.name, "source": "rebalance_results"}
        return None, 0.0, {}

    def _candidate_from_engine_events(self, pdno: str) -> Tuple[Optional[str], float, Dict[str, Any]]:
        pdno_key = normalize_code(pdno)
        for row in reversed(self.engine_events):
            code = normalize_code(row.get("pdno") or row.get("code") or "")
            if code != pdno_key:
                continue
            if str(row.get("side") or "").upper() != "BUY":
                continue
            sid = normalize_sid(row.get("sid") or row.get("strategy_id"))
            return sid, 0.60, {"source": "engine_events"}
        return None, 0.0, {}

    def _candidate_from_preference(self, pdno: str) -> Tuple[Optional[str], float, Dict[str, Any]]:
        pdno_key = normalize_code(pdno)
        sid_pref = self.preferred_strategy.get(pdno_key)
        if sid_pref:
            return normalize_sid(sid_pref), 0.82, {"source": "preferred_strategy"}
        return None, 0.0, {}

    def recover(self, pdno: str, hldg_qty: int, pchs_avg_pric: float, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        pdno_key = normalize_code(pdno)
        candidates: List[Tuple[str, float, Dict[str, Any]]] = []

        for getter in (
            self._candidate_from_fills,
            self._candidate_from_orders,
            self._candidate_from_ledger,
            self._candidate_from_rebalance,
            self._candidate_from_engine_events,
            self._candidate_from_preference,
        ):
            sid, conf, evidence = getter(pdno_key)
            if sid:
                candidates.append((sid, conf, evidence))

        if not candidates:
            self.stats["F"] += 1
            return [
                {
                    "sid": "MANUAL",
                    "qty": int(hldg_qty),
                    "entry_price": float(pchs_avg_pric or 0.0),
                    "meta": {
                        "confidence": 0.10,
                        "sources": [],
                        "evidence": context,
                        "reconciled": True,
                        "sell_blocked": False,
                        "rule": "F",
                    },
                }
            ]

        merged: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
        for sid, conf, evidence in candidates:
            best_conf, evidences = merged.get(sid, (0.0, []))
            if conf > best_conf:
                merged[sid] = (conf, [evidence])
            elif conf == best_conf:
                evidences.append(evidence)
                merged[sid] = (conf, evidences)

        sorted_candidates = sorted(merged.items(), key=lambda kv: kv[1][0], reverse=True)
        best_sid, (best_conf, best_evidences) = sorted_candidates[0]
        if len(sorted_candidates) > 1:
            second_conf = sorted_candidates[1][1][0]
            if (best_conf - second_conf) <= 0.10:
                self.stats["E"] += 1
                first_qty = int(round(hldg_qty * 0.6))
                second_qty = hldg_qty - first_qty
                lots: List[Dict[str, Any]] = []
                for sid, qty in ((best_sid, first_qty), (sorted_candidates[1][0], second_qty)):
                    lots.append(
                        {
                            "sid": sid,
                            "qty": int(qty),
                            "entry_price": float(pchs_avg_pric or 0.0),
                            "meta": {
                                "confidence": 0.45,
                                "sources": ["ambiguous_split"],
                                "evidence": {"candidates": sorted_candidates, **context},
                                "reconciled": True,
                                "sell_blocked": False,
                                "rule": "E",
                                "ambiguous": True,
                            },
                        }
                    )
                return lots

        rule_key = "A"
        if best_conf >= 0.95:
            rule_key = "A"
        elif best_conf >= 0.80:
            rule_key = "B"
        elif best_conf >= 0.65:
            rule_key = "C"
        elif best_conf >= 0.60:
            rule_key = "D"
        self.stats[rule_key] += 1
        return [
            {
                "sid": best_sid,
                "qty": int(hldg_qty),
                "entry_price": float(pchs_avg_pric or 0.0),
                "meta": {
                    "confidence": best_conf,
                    "sources": [ev.get("source", "unknown") for ev in best_evidences if ev],
                    "evidence": {**context, "details": best_evidences},
                    "reconciled": True,
                    "sell_blocked": False,
                    "rule": rule_key,
                },
            }
        ]
