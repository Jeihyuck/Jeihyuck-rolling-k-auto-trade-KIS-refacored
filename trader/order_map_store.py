from __future__ import annotations

import json
import logging
import uuid
from pathlib import Path
from typing import Any, Dict

from .io_atomic import append_jsonl
from .paths import STATE_DIR, ensure_dirs
from .strategy_registry import normalize_sid

logger = logging.getLogger(__name__)

ORDERS_MAP_PATH = STATE_DIR / "orders_map.jsonl"


def append_order_map(
    order_id: str | None,
    pdno: str,
    sid: Any,
    side: str,
    qty: int,
    price: float,
    reason: str,
    ts: str,
    run_id: str | None = None,
    *,
    status: str = "submitted",
    rejection_reason: str | None = None,
) -> Dict[str, Any]:
    ensure_dirs()
    normalized_sid = normalize_sid(sid)
    oid = order_id or f"client-{normalized_sid}-{uuid.uuid4().hex}"
    client_prefix = f"client-{normalized_sid}-"
    if not oid.startswith(client_prefix) and order_id is None:
        oid = f"{client_prefix}{uuid.uuid4().hex}"
    status_norm = str(status or "submitted").lower()
    record = {
        "order_id": oid,
        "pdno": pdno,
        "sid": normalized_sid,
        "side": side.upper(),
        "qty": int(qty),
        "price": float(price),
        "ts": ts,
        "reason": reason,
        "status": status_norm,
    }
    if rejection_reason:
        record["rejection_reason"] = rejection_reason
    if run_id:
        record["run_id"] = run_id
    if order_id is None:
        record["client_generated"] = True
        logger.warning("[ORDER_MAP] missing order_id -> generated client id %s for %s/%s", oid, pdno, normalized_sid)
    append_jsonl(ORDERS_MAP_PATH, record)
    return record


def load_order_map_index(path: Path | None = None) -> Dict[str, Dict[str, Any]]:
    path = path or ORDERS_MAP_PATH
    if not path.exists():
        return {}
    index: Dict[str, Dict[str, Any]] = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                oid = payload.get("order_id") or payload.get("client_id")
                if not oid:
                    continue
                index[str(oid)] = payload
    except Exception:
        logger.exception("[ORDER_MAP] failed to load index from %s", path)
    return index
