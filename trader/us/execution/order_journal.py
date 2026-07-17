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
        "run_source": intent.get("run_source") or (intent.get("meta") or {}).get("run_source") or os.getenv("US_RUN_SOURCE", ""),
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


def replay_order_journal(trade_date: str, session_run_id: str | None = None,
                         tick_id: str | None = None, provider: Any | None = None) -> dict:
    """Idempotently restore broker ACK evidence; never resubmit an order."""
    events = load_order_events(trade_date, session_run_id=session_run_id)
    if tick_id:
        events = [e for e in events if e.get("tick_id") == tick_id]
    grouped: dict[str, list[dict]] = {}
    for event in events:
        key = str(event.get("client_order_key") or "").strip()
        if key:
            grouped.setdefault(key, []).append(event)
    restored = filled = unresolved = failed = 0
    unresolved_symbol_sides: list[list[str]] = []
    from trader.us.db.repos import save_order_ack, mark_order_filled_by_reconcile
    for key, order_events in grouped.items():
        latest = order_events[-1]
        types = {e.get("event_type") for e in order_events}
        if "BROKER_SUBMIT_STARTED" not in types:
            continue
        append_order_event("JOURNAL_REPLAY_STARTED", latest)
        ack_event = next((e for e in reversed(order_events) if e.get("event_type") == "BROKER_ACK_RECEIVED"), None)
        if not ack_event:
            unresolved += 1
            unresolved_symbol_sides.append([str(latest.get("symbol") or ""), str(latest.get("side") or "")])
            append_order_event("JOURNAL_REPLAY_UNRESOLVED", latest)
            continue
        order_no = str(ack_event.get("broker_order_no") or "")
        try:
            saved = save_order_ack({
                "client_order_key": key, "symbol": ack_event.get("symbol"),
                "exchange": ack_event.get("exchange"), "side": ack_event.get("side"),
                "qty_requested": int(ack_event.get("qty") or 0), "qty_filled": 0,
                "order_no": order_no, "status": "ACK",
                "meta": {k: ack_event.get(k) for k in ("session", "session_run_id", "session_generation", "tick_id", "prep_run_id")},
            }, trade_date=trade_date)
            if not saved:
                raise RuntimeError("DB ACK restore rejected")
            restored += 1
            append_order_event("JOURNAL_REPLAY_DB_ACK_RESTORED", ack_event, broker_order_no=order_no)
            if provider is not None and order_no:
                broker_fill = provider.get_fills_by_order_no(order_no=order_no, symbol=ack_event.get("symbol"))
                if isinstance(broker_fill, dict) and int(broker_fill.get("filled_qty") or 0) > 0:
                    result = mark_order_filled_by_reconcile(
                        order_no=order_no, client_order_key=key, symbol=str(ack_event.get("symbol") or ""),
                        side=str(ack_event.get("side") or ""), filled_qty=int(broker_fill["filled_qty"]),
                        avg_price_usd=float(broker_fill.get("avg_price") or 0), source="journal_replay_kis_fill",
                        trade_date=trade_date, meta={"journal_replay": True},
                    )
                    if result.get("status") == "OK":
                        filled += 1
                        append_order_event("JOURNAL_REPLAY_FILL_CONFIRMED", ack_event, broker_order_no=order_no)
        except Exception as exc:
            failed += 1
            append_order_event("JOURNAL_REPLAY_FAILED", latest, raw_response={"error": str(exc)})
    status = "ERROR" if failed else "UNRESOLVED" if unresolved else "OK"
    return {"status": status, "restored_ack_count": restored, "filled_count": filled,
            "unresolved_count": unresolved, "failed_count": failed,
            "unresolved_symbol_sides": unresolved_symbol_sides, "events_scanned": len(events)}
