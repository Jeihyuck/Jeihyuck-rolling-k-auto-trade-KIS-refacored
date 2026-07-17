"""Append-only, fsync'd order journal used before any broker submission."""
from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def journal_path(trade_date: str) -> Path:
    root = Path(os.getenv("US_ORDER_JOURNAL_DIR", "runtime/us/order_journal"))
    return root / f"{trade_date}.jsonl"


def append_order_event(event_type: str, intent: dict, *, context: Any | None = None,
                       broker_order_no: str = "", broker_status: str = "", raw_response: Any = None) -> dict:
    td = str(intent.get("trade_date") or getattr(context, "trade_date", ""))
    if not td:
        raise OSError("journal event requires trade_date")
    raw = json.dumps(raw_response or {}, sort_keys=True, ensure_ascii=False, default=str)
    event = {
        "event_id": str(uuid.uuid4()), "event_type": event_type, "trade_date": td,
        "session": intent.get("session") or getattr(context, "session", ""),
        "session_run_id": intent.get("session_run_id") or getattr(context, "session_run_id", ""),
        "session_generation": intent.get("session_generation") or getattr(context, "session_generation", 0),
        "tick_id": intent.get("tick_id") or getattr(context, "tick_id", ""),
        "prep_run_id": intent.get("prep_run_id") or getattr(context, "prep_run_id", ""),
        "client_order_key": intent.get("client_order_key", ""), "symbol": intent.get("symbol", ""),
        "exchange": intent.get("exchange", ""), "side": intent.get("side", ""), "qty": intent.get("qty", 0),
        "broker_order_no": broker_order_no, "broker_status": broker_status,
        "timestamp": datetime.now(timezone.utc).isoformat(), "raw_response_hash": hashlib.sha256(raw.encode()).hexdigest(),
    }
    path = journal_path(td)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(event, ensure_ascii=False, sort_keys=True, default=str) + "\n"
    fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(fd, line.encode())
        os.fsync(fd)
    finally:
        os.close(fd)
    return event


def load_order_events(trade_date: str, *, session_run_id: str | None = None) -> list[dict]:
    path = journal_path(trade_date)
    if not path.exists():
        return []
    events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return [e for e in events if not session_run_id or e.get("session_run_id") == session_run_id]
