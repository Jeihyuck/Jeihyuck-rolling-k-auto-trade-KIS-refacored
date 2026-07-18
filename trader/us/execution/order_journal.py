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
    """Restore DB evidence and independently classify broker truth; never resubmit."""
    events = load_order_events(trade_date, session_run_id=session_run_id)
    if tick_id:
        events = [e for e in events if e.get("tick_id") == tick_id]
    grouped: dict[str, list[dict]] = {}
    for event in events:
        key = str(event.get("client_order_key") or "").strip()
        if key: grouped.setdefault(key, []).append(event)
    counts = {"db_ack_restored_count": 0, "broker_full_fill_count": 0,
              "broker_partial_fill_count": 0, "broker_unfilled_ack_count": 0,
              "broker_rejected_count": 0, "broker_unknown_count": 0,
              "identity_mismatch_count": 0, "failed_count": 0}
    unresolved_symbol_sides: list[list[str]] = []
    from trader.us.db.repos import save_order_ack, mark_order_filled_by_reconcile
    # Query authoritative snapshots once per timeout, not once per journal row.
    balance = None
    today_orders = None
    all_fills = None
    if provider is not None:
        try: balance = provider.get_balance(force_refresh=True)
        except TypeError: balance = provider.get_balance()
        except Exception: balance = None
        try:
            method = getattr(provider, "get_today_orders", None)
            today_orders = method(trade_date=trade_date) if callable(method) else None
        except Exception: today_orders = None
        try:
            from trader.us.execution.fills import get_fills_today
            response = get_fills_today(provider=provider, trade_date=trade_date)
            all_fills = response.get("fills") if response.get("status") == "OK" else None
        except Exception: all_fills = None
    positions_by_symbol = {}
    if isinstance(balance, dict):
        for pos in balance.get("positions") or []:
            positions_by_symbol[str(pos.get("symbol") or pos.get("pdno") or "").upper()] = pos

    for key, order_events in grouped.items():
        latest = order_events[-1]; types = {e.get("event_type") for e in order_events}
        if "BROKER_SUBMIT_STARTED" not in types: continue
        append_order_event("JOURNAL_REPLAY_STARTED", latest)
        ack = next((e for e in reversed(order_events) if e.get("event_type") == "BROKER_ACK_RECEIVED"), None)
        symbol, side, requested = str(latest.get("symbol") or "").upper(), str(latest.get("side") or "").upper(), int(latest.get("qty") or 0)
        if not ack or not str(ack.get("broker_order_no") or ""):
            counts["broker_unknown_count"] += 1; unresolved_symbol_sides.append([symbol, side])
            append_order_event("JOURNAL_REPLAY_UNRESOLVED", latest, broker_status="BROKER_SUBMIT_RESULT_UNKNOWN"); continue
        order_no = str(ack["broker_order_no"])
        try:
            if save_order_ack({"client_order_key": key,"symbol": symbol,"exchange": ack.get("exchange"),"side": side,
                "qty_requested": requested,"qty_filled": 0,"order_no": order_no,"status": "ACK",
                "meta": {k: ack.get(k) for k in ("session","session_run_id","session_generation","tick_id","prep_run_id")}}, trade_date=trade_date):
                counts["db_ack_restored_count"] += 1
                append_order_event("JOURNAL_REPLAY_DB_ACK_RESTORED", ack, broker_order_no=order_no, broker_status="JOURNAL_ACK_DB_RESTORED")
            broker_fill = None
            if provider is not None:
                detail = getattr(provider, "get_fills_by_order_no", None)
                if callable(detail): broker_fill = detail(order_no=order_no, symbol=symbol)
                if not broker_fill and isinstance(all_fills, list):
                    matched = [f for f in all_fills if str(f.get("order_no") or "") == order_no]
                    if matched:
                        broker_fill = {"filled_qty": sum(int(f.get("qty") or 0) for f in matched),
                                       "avg_price": float(matched[-1].get("price") or matched[-1].get("price_usd") or 0),
                                       "symbol": matched[-1].get("symbol"), "side": matched[-1].get("side")}
            if broker_fill and int(broker_fill.get("filled_qty") or 0) > 0:
                if str(broker_fill.get("symbol") or symbol).upper()!=symbol or str(broker_fill.get("side") or side).upper()!=side:
                    counts["identity_mismatch_count"] += 1; unresolved_symbol_sides.append([symbol,side]); continue
                cumulative=int(broker_fill["filled_qty"])
                result=mark_order_filled_by_reconcile(order_no=order_no,client_order_key=key,symbol=symbol,side=side,
                    filled_qty=cumulative,requested_qty=requested,cumulative_filled_qty=cumulative,
                    avg_price_usd=float(broker_fill.get("avg_price") or 0),source="fills_by_order_no",
                    evidence_type="KIS_ORDER_DETAIL_ACTUAL",trade_date=trade_date,meta={"journal_replay":True})
                if result.get("status") != "OK": raise RuntimeError(result.get("status"))
                bucket="broker_full_fill_count" if cumulative>=requested else "broker_partial_fill_count"; counts[bucket]+=1
                append_order_event("JOURNAL_REPLAY_FILL_CONFIRMED",ack,broker_order_no=order_no,
                                   broker_status="BROKER_FILL_CONFIRMED" if cumulative>=requested else "BROKER_PARTIAL_FILL_CONFIRMED")
            else:
                broker_order = next((o for o in (today_orders or []) if str(o.get("order_no") or o.get("odno") or "")==order_no),None)
                if broker_order:
                    broker_symbol=str(broker_order.get("symbol") or broker_order.get("pdno") or symbol).upper()
                    broker_side=str(broker_order.get("side") or side).upper()
                    if broker_symbol!=symbol or broker_side!=side:
                        counts["identity_mismatch_count"]+=1; unresolved_symbol_sides.append([symbol,side])
                    elif str(broker_order.get("status") or "").upper() in {"REJECTED","REJECT","CANCELLED","CANCELED"}:
                        counts["broker_rejected_count"]+=1
                    else: counts["broker_unfilled_ack_count"]+=1
                else:
                    pre_qty_raw = latest.get("pre_order_position_qty") or latest.get("pre_qty") or (latest.get("meta") or {}).get("pre_order_position_qty") if isinstance(latest.get("meta"), dict) else None
                    post_pos = positions_by_symbol.get(symbol) if positions_by_symbol else None
                    post_qty_raw = (post_pos or {}).get("qty") or (post_pos or {}).get("holding_qty") or (post_pos or {}).get("ovrs_cblc_qty")
                    if pre_qty_raw is not None and post_qty_raw is not None and requested > 0:
                        try:
                            pre_qty = int(float(pre_qty_raw)); post_qty = int(float(post_qty_raw))
                            delta = (pre_qty - post_qty) if side == "SELL" else (post_qty - pre_qty)
                        except Exception:
                            delta = 0
                        if delta > 0:
                            cumulative = min(delta, requested)
                            result=mark_order_filled_by_reconcile(order_no=order_no,client_order_key=key,symbol=symbol,side=side,
                                filled_qty=cumulative,requested_qty=requested,cumulative_filled_qty=cumulative,
                                avg_price_usd=float(latest.get("limit_price") or latest.get("price") or 0),source="journal_replay_balance_delta",
                                evidence_type="BALANCE_DELTA_SYNTHETIC",trade_date=trade_date,meta={"journal_replay":True})
                            if result.get("status") != "OK": raise RuntimeError(result.get("status"))
                            counts["broker_full_fill_count" if cumulative>=requested else "broker_partial_fill_count"] += 1
                            append_order_event("JOURNAL_REPLAY_FILL_CONFIRMED",ack,broker_order_no=order_no,
                                               broker_status="BROKER_FILL_CONFIRMED" if cumulative>=requested else "BROKER_PARTIAL_FILL_CONFIRMED")
                        else:
                            counts["broker_unknown_count"]+=1; unresolved_symbol_sides.append([symbol,side])
                    else:
                        counts["broker_unknown_count"]+=1; unresolved_symbol_sides.append([symbol,side])
        except Exception as exc:
            counts["failed_count"]+=1; unresolved_symbol_sides.append([symbol,side])
            append_order_event("JOURNAL_REPLAY_FAILED",latest,raw_response={"error":str(exc)})
    confirmed=sum(counts[k] for k in ("broker_full_fill_count","broker_partial_fill_count","broker_unfilled_ack_count","broker_rejected_count"))
    unresolved=counts["broker_unknown_count"]+counts["identity_mismatch_count"]+counts["failed_count"]
    status="ERROR" if counts["failed_count"] else "UNRESOLVED" if unresolved or (counts["db_ack_restored_count"] and not confirmed) else "OK"
    return {"status":status,**counts,"restored_ack_count":counts["db_ack_restored_count"],
            "filled_count":counts["broker_full_fill_count"]+counts["broker_partial_fill_count"],
            "broker_confirmed_count":confirmed,"unresolved_count":unresolved,
            "unresolved_symbol_sides":unresolved_symbol_sides,"events_scanned":len(events),
            "balance_fetched":balance is not None}
