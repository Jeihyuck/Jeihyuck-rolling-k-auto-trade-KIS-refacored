from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from .io_atomic import append_jsonl
from .paths import FILLS_DIR, ensure_dirs
from .strategy_registry import normalize_sid

logger = logging.getLogger(__name__)


def _fills_path(ts: datetime | None = None) -> Path:
    ts = ts or datetime.now()
    day_tag = ts.strftime("%Y%m%d")
    return FILLS_DIR / f"fills_{day_tag}.jsonl"


def append_fill(
    *,
    ts: str,
    order_id: str | None,
    pdno: str,
    sid: Any,
    side: str,
    qty: int,
    price: float,
    source: str,
    note: str = "",
    run_id: str | None = None,
) -> Dict[str, Any]:
    ensure_dirs()
    record = {
        "ts": ts,
        "order_id": order_id,
        "pdno": pdno,
        "sid": normalize_sid(sid),
        "side": side.upper(),
        "qty": int(qty),
        "price": float(price),
        "source": source,
        "note": note,
    }
    if run_id:
        record["run_id"] = run_id
    append_jsonl(_fills_path(), record)
    return record


def load_fills_index(path: Path | None = None) -> List[Dict[str, Any]]:
    ensure_dirs()
    rows: List[Dict[str, Any]] = []
    if path:
        p = Path(path)
        if p.is_dir():
            paths = sorted(p.glob("fills_*.jsonl"))
        else:
            paths = [p]
    else:
        paths = sorted(FILLS_DIR.glob("fills_*.jsonl"))
    for p in paths:
        try:
            with open(p, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        except FileNotFoundError:
            continue
        except Exception:
            logger.exception("[FILL_STORE] failed to read %s", p)
    return rows
