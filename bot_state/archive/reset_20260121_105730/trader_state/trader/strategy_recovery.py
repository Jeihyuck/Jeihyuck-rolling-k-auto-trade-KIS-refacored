from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from .fill_store import load_fills_index
from .order_map_store import ORDERS_MAP_PATH, load_order_map_index
from .paths import LOG_DIR, REPO_ROOT
from .strategy_registry import normalize_sid

logger = logging.getLogger(__name__)

DEFAULT_LOOKBACK_DAYS = 30


def _parse_ts(value: Any, default: datetime) -> datetime:
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value))
        except Exception:
            return default
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return default
    return default


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


def _rebalance_candidates(rebalance_dir: Path, pdno: str, asof: datetime) -> Tuple[str | None, float, List[str]]:
    files = sorted(rebalance_dir.glob("*.json"))
    chosen = None
    chosen_conf = 0.0
    reason: List[str] = []
    latest_ts = None
    for fp in files:
        try:
            tag = fp.stem
            ts = datetime.fromisoformat(tag) if len(tag) >= 8 else None
        except Exception:
            ts = None
        if ts and ts > asof:
            continue
        try:
            payload = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            continue
        strategy_hits: Dict[str, int] = {}
        for key, val in payload.items():
            if not isinstance(val, list):
                continue
            for row in val:
                code = str(row.get("code") or row.get("pdno") or "").strip()
                if code != pdno:
                    continue
                strategy_hits[key] = strategy_hits.get(key, 0) + 1
        if not strategy_hits:
            continue
        if len(strategy_hits) == 1:
            sid_raw = list(strategy_hits.keys())[0]
            sid = normalize_sid(sid_raw if sid_raw.upper().startswith("S") else 1)
            conf = 0.75
        else:
            sid = None
            conf = 0.55
        if latest_ts is None or (ts and ts > latest_ts):
            latest_ts = ts
            chosen = sid
            chosen_conf = conf
            reason = [f"rebalance:{fp.name}"]
    return chosen, chosen_conf, reason


def _scan_logs_for_sid(log_dir: Path, pdno: str, asof: datetime) -> Tuple[str | None, float, List[str]]:
    candidates: Dict[str, int] = {}
    reasons: List[str] = []
    for fp in log_dir.glob("*.log"):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                for line in f:
                    if pdno not in line:
                        continue
                    line_upper = line.upper()
                    if "SID=" in line_upper:
                        token = line_upper.split("SID=")[1].split()[0].strip(",;]")
                        sid = normalize_sid(token)
                        candidates[sid] = candidates.get(sid, 0) + 1
                    elif "STRATEGY_ID" in line_upper:
                        token = line_upper.split("STRATEGY_ID")[1].split("=")[1].split()[0].strip(",;]")
                        sid = normalize_sid(token)
                        candidates[sid] = candidates.get(sid, 0) + 1
        except Exception:
            continue
    if not candidates:
        return None, 0.0, reasons
    if len(candidates) == 1:
        sid = list(candidates.keys())[0]
        return sid, 0.85, [f"log:{next(iter(log_dir.glob('*.log')), None)}"]
    sorted_hits = sorted(candidates.items(), key=lambda x: x[1], reverse=True)
    sid, _ = sorted_hits[0]
    reasons.append(f"log_ambiguous:{candidates}")
    return sid, 0.7, reasons


def recover_sid_for_holding(
    pdno: str,
    qty: int,
    avg_price: float | None,
    asof_ts: datetime | None = None,
    evidence_dirs: Dict[str, Any] | None = None,
) -> Tuple[str, float, List[str]]:
    asof = asof_ts or datetime.now()
    evidence_dirs = evidence_dirs or {}
    reasons: List[str] = []
    candidates: List[Tuple[str, float, List[str]]] = []

    # Current state lots (for remainder allocation)
    state_lots: List[Dict[str, Any]] = evidence_dirs.get("state_lots") or []
    for lot in state_lots:
        lot_pdno = str(lot.get("pdno") or "").strip()
        if lot_pdno != pdno:
            continue
        sid = normalize_sid(lot.get("strategy_id") or lot.get("sid"))
        if sid in {"UNKNOWN", "MANUAL"}:
            continue
        rem = int(lot.get("remaining_qty") or lot.get("qty") or 0)
        if rem > 0:
            candidates.append((sid, 0.95, ["existing_open_lot"]))
            break

    # E1 orders_map recent buy
    orders_path = evidence_dirs.get("orders_map") or ORDERS_MAP_PATH
    om_index = load_order_map_index(orders_path) if isinstance(orders_path, Path) or orders_path else load_order_map_index()
    lookback = timedelta(days=DEFAULT_LOOKBACK_DAYS)
    now_dt = asof
    for payload in om_index.values():
        if str(payload.get("pdno") or "").strip() != pdno:
            continue
        if str(payload.get("side") or "").upper() != "BUY":
            continue
        ts = _parse_ts(payload.get("ts"), now_dt)
        if now_dt - ts > lookback:
            continue
        sid = normalize_sid(payload.get("sid"))
        recency = max(0.0, 1.0 - (now_dt - ts).total_seconds() / lookback.total_seconds())
        conf = 0.90 + (0.09 * recency)
        candidates.append((sid, conf, ["orders_map_recent_buy"]))

    # E2 fills/ledger
    fills_dir = evidence_dirs.get("fills_dir")
    fills_rows = load_fills_index() if fills_dir is None else load_fills_index(Path(fills_dir))  # type: ignore[arg-type]
    latest_fill = None
    for row in fills_rows:
        if str(row.get("pdno") or "").strip() != pdno:
            continue
        if str(row.get("side") or "").upper() != "BUY":
            continue
        ts = _parse_ts(row.get("ts"), now_dt)
        if latest_fill is None or ts > latest_fill[0]:
            latest_fill = (ts, row)
    if latest_fill:
        ts, row = latest_fill
        if now_dt - ts <= timedelta(days=DEFAULT_LOOKBACK_DAYS):
            sid = normalize_sid(row.get("sid"))
            candidates.append((sid, 0.95, ["fills_recent_buy"]))
    ledger_path = Path(evidence_dirs.get("ledger_path") or LOG_DIR / "ledger.jsonl")
    ledger_rows = _load_json_lines(ledger_path)
    for row in ledger_rows:
        code = str(row.get("code") or row.get("pdno") or "").strip()
        if code != pdno:
            continue
        sid = normalize_sid(row.get("strategy_id"))
        side = str(row.get("side") or "").upper()
        if sid not in {"UNKNOWN", "MANUAL"} and side in {"BUY", "SELL"}:
            candidates.append((sid, 0.9, [f"ledger:{ledger_path.name}"]))
            break

    # E3 logs
    log_dir = Path(evidence_dirs.get("log_dir") or LOG_DIR)
    sid_from_logs, log_conf, log_reason = _scan_logs_for_sid(log_dir, pdno, asof)
    if sid_from_logs:
        candidates.append((sid_from_logs, log_conf, log_reason or ["logs"]))

    # E4 rebalance json
    rebalance_dir = Path(evidence_dirs.get("rebalance_dir") or REPO_ROOT / "rebalance_results")
    reb_sid, reb_conf, reb_reason = _rebalance_candidates(rebalance_dir, pdno, asof)
    if reb_sid:
        candidates.append((reb_sid, reb_conf, reb_reason))

    # E5 proportional allocation (weak)
    if not candidates and state_lots:
        allocations: Dict[str, int] = {}
        for lot in state_lots:
            if str(lot.get("pdno") or "").strip() != pdno:
                continue
            sid = normalize_sid(lot.get("strategy_id") or lot.get("sid"))
            rem = int(lot.get("remaining_qty") or lot.get("qty") or 0)
            allocations[sid] = allocations.get(sid, 0) + rem
        if allocations:
            sid = max(allocations.items(), key=lambda x: x[1])[0]
            candidates.append((sid, 0.7, ["allocation_heuristic"]))

    if not candidates:
        return "MANUAL", 0.4, ["no_evidence"]

    # Aggregate per sid choose best confidence
    merged: Dict[str, Tuple[float, List[str]]] = {}
    for sid, conf, rs in candidates:
        best_conf, best_reasons = merged.get(sid, (0.0, []))
        if conf > best_conf:
            merged[sid] = (conf, rs)
        elif conf == best_conf:
            merged[sid] = (conf, best_reasons + rs)

    sorted_candidates = sorted(merged.items(), key=lambda x: x[1][0], reverse=True)
    best_sid, (best_conf, best_reasons) = sorted_candidates[0]
    if len(sorted_candidates) > 1:
        second_conf = sorted_candidates[1][1][0]
        if best_conf - second_conf < 0.15:
            conflict_reasons = [f"conflict:{[(sid, conf) for sid, (conf, _) in sorted_candidates[:3]]}"]
            return "MANUAL", 0.5, best_reasons + conflict_reasons

    final_sid = best_sid if best_conf >= 0.80 else "MANUAL"
    if final_sid == "MANUAL" and "confidence_low" not in best_reasons:
        best_reasons.append("confidence_low")
    return final_sid, best_conf, best_reasons
