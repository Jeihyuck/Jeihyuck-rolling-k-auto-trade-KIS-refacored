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
    meta = intent.get("meta") if isinstance(intent.get("meta"), dict) else {}
    pre_order_position_qty = intent.get("pre_order_position_qty")
    if pre_order_position_qty is None:
        pre_order_position_qty = meta.get("pre_order_position_qty")
    position_lifecycle_id = intent.get("position_lifecycle_id")
    if position_lifecycle_id is None:
        position_lifecycle_id = meta.get("position_lifecycle_id")
    limit_price = intent.get("limit_price")
    if limit_price is None:
        limit_price = intent.get("limit_price_usd")
    if limit_price is None:
        limit_price = meta.get("limit_price") or meta.get("limit_price_usd")
    event = {
        "event_id": str(uuid.uuid4()), "event_type": event_type, "trade_date": td,
        "session": intent.get("session") or getattr(context, "session", ""),
        "session_run_id": intent.get("session_run_id") or getattr(context, "session_run_id", ""),
        "session_generation": intent.get("session_generation") or getattr(context, "session_generation", 0),
        "tick_id": intent.get("tick_id") or getattr(context, "tick_id", ""),
        "prep_run_id": intent.get("prep_run_id") or getattr(context, "prep_run_id", ""),
        "run_source": intent.get("run_source") or (intent.get("meta") or {}).get("run_source") or os.getenv("US_RUN_SOURCE", ""),
        "client_order_key": intent.get("client_order_key", ""), "symbol": intent.get("symbol", ""),
        "submit_attempt_id": intent.get("submit_attempt_id") or meta.get("submit_attempt_id") or "",
        "exchange": intent.get("exchange", ""), "side": intent.get("side", ""), "qty": intent.get("qty", 0),
        "requested_qty": intent.get("qty", intent.get("requested_qty", 0)),
        "submitted_at_utc": intent.get("submitted_at_utc") or meta.get("submitted_at_utc") or datetime.now(timezone.utc).isoformat(),
        "profit_capture_stage": meta.get("profit_capture_stage"),
        "broker_order_no": broker_order_no, "broker_status": broker_status,
        "pre_order_position_qty": pre_order_position_qty,
        "pre_order_position_source": intent.get("pre_order_position_source") or meta.get("pre_order_position_source"),
        "position_lifecycle_id": position_lifecycle_id,
        "limit_price": limit_price, "order_price": intent.get("order_price") or meta.get("order_price"),
        "notional_usd": intent.get("notional_usd") or meta.get("notional_usd"),
        "meta": meta,
        "cumulative_filled_qty": (raw_response or {}).get("cumulative_filled_qty") if isinstance(raw_response, dict) else None,
        "fill_price": (raw_response or {}).get("fill_price") if isinstance(raw_response, dict) else None,
        "broker_execution_id": (raw_response or {}).get("broker_execution_id") if isinstance(raw_response, dict) else None,
        "execution_timestamp": (raw_response or {}).get("execution_timestamp") if isinstance(raw_response, dict) else None,
        "fees": (raw_response or {}).get("fees") if isinstance(raw_response, dict) else None,
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


def aggregate_order_events(trade_date: str, *, session_run_id: str | None = None) -> dict:
    """Aggregate immutable unique submit attempts; reconciliation never adds submits."""
    events = load_order_events(trade_date, session_run_id=session_run_id)
    submits: dict[str, dict] = {}
    lifecycle: dict[str, set[str]] = {}
    fill_execution_keys: set[tuple] = set()
    partial_observation_keys: set[tuple] = set()
    for event in events:
        attempt = str(event.get("submit_attempt_id") or "").strip()
        if not attempt:
            continue
        event_type = str(event.get("event_type") or "")
        if event_type == "BROKER_SUBMIT_STARTED":
            submits.setdefault(attempt, event)
        lifecycle.setdefault(attempt, set()).add(event_type)
        if event_type in {"ORDER_PARTIALLY_FILLED", "ORDER_FILLED"}:
            event_key = (attempt, event.get("broker_order_no"), event_type, event.get("raw_response_hash"))
            fill_execution_keys.add(event_key)
            if event_type == "ORDER_PARTIALLY_FILLED": partial_observation_keys.add(event_key)
    ack = {a for a, types in lifecycle.items() if types & {"BROKER_ACK_RECEIVED", "BROKER_ACK_RECOVERED"}}
    rejected = {a for a, types in lifecycle.items() if "ORDER_REJECTED" in types}
    filled = {a for a, types in lifecycle.items() if "ORDER_FILLED" in types}
    buy = sum(str(e.get("side") or "").upper() == "BUY" for e in submits.values())
    sell = sum(str(e.get("side") or "").upper() == "SELL" for e in submits.values())
    return {"scope": "trade_day", "orders_sent_total": len(submits), "buy_orders_count": buy,
            "sell_orders_count": sell, "orders_ack_total": len(ack), "orders_reject_total": len(rejected),
            "orders_fill_confirmed": len(filled), "fill_execution_row_count": len(fill_execution_keys),
            "partial_fill_observation_count": len(partial_observation_keys),
            "unique_submit_attempt_count": len(submits),
            "unique_client_order_count": len({e.get('client_order_key') for e in submits.values()}),
            "unique_broker_order_count": len({e.get('broker_order_no') for e in events if e.get('broker_order_no')})}


def order_audit_timelines(trade_date: str) -> dict[str, dict]:
    """Derive immutable submit/ACK/fill timestamps from production journal events."""
    timelines: dict[str, dict] = {}
    for event in load_order_events(trade_date):
        key = str(event.get("client_order_key") or "")
        if not key: continue
        row = timelines.setdefault(key, {})
        typ, ts = event.get("event_type"), event.get("timestamp")
        if typ == "BROKER_SUBMIT_STARTED": row.setdefault("submitted_at", ts)
        if typ in {"BROKER_ACK_RECEIVED", "BROKER_ACK_RECOVERED"}: row.setdefault("acknowledged_at", ts)
        if typ == "ORDER_FILLED":
            row["filled_at"] = event.get("execution_timestamp") or ts
            row["fill_price"] = event.get("fill_price")
            row["filled_qty"] = event.get("cumulative_filled_qty")
            row["fees"] = event.get("fees")
    return timelines


def match_ambiguous_submit_to_broker_order(*, trade_date: str, symbol: str, side: str,
        requested_qty: int, limit_price: float, submitted_at_utc: str,
        exchange: str, broker_rows: list[dict]) -> dict:
    """Return exactly one high-confidence broker candidate or fail closed."""
    from trader.us.symbols import normalize_us_exchange
    window = float(os.getenv("US_AMBIGUOUS_MATCH_WINDOW_SEC", "120"))
    price_tolerance = float(os.getenv("US_AMBIGUOUS_MATCH_PRICE_TOLERANCE", "0.02"))
    try: wanted_exchange = normalize_us_exchange(exchange)
    except Exception: wanted_exchange = str(exchange or "").upper()
    try: submitted = datetime.fromisoformat(str(submitted_at_utc).replace("Z", "+00:00"))
    except Exception: return {"status": "AMBIGUOUS_SUBMIT_IDENTITY_INVALID", "matches": []}
    matches = []
    for row in broker_rows or []:
        row_date = str(row.get("trade_date") or row.get("ord_dt") or "").replace(".", "-")
        if row_date and row_date.replace("-", "") != trade_date.replace("-", ""): continue
        if str(row.get("symbol") or row.get("pdno") or "").upper() != symbol.upper(): continue
        if str(row.get("side") or "").upper() != side.upper(): continue
        if int(row.get("requested_qty") or row.get("qty") or 0) != int(requested_qty): continue
        try: row_exchange = normalize_us_exchange(str(row.get("exchange") or row.get("raw_exchange") or exchange))
        except Exception: row_exchange = str(row.get("exchange") or row.get("raw_exchange") or "").upper()
        if wanted_exchange and row_exchange and row_exchange != wanted_exchange: continue
        row_price = float(row.get("limit_price") or row.get("order_price") or row.get("avg_price") or 0)
        if limit_price > 0 and (row_price <= 0 or abs(row_price - limit_price) > price_tolerance): continue
        time_raw = row.get("submitted_at_utc") or row.get("order_timestamp") or row.get("observed_at")
        if not time_raw: continue
        try:
            observed = datetime.fromisoformat(str(time_raw).replace("Z", "+00:00"))
            if observed.tzinfo is None: observed = observed.replace(tzinfo=timezone.utc)
            if abs((observed - submitted).total_seconds()) > window: continue
        except Exception: continue
        matches.append(row)
    if len(matches) == 1: return {"status": "MATCHED", "match": matches[0], "matches": matches}
    return {"status": "NO_MATCH" if not matches else "AMBIGUOUS_MULTIPLE_BROKER_CANDIDATES", "matches": matches}


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
    from trader.us.db.repos import save_order_ack, mark_order_filled_by_reconcile, apply_broker_order_observation
    from trader.us.utils.order_no import normalize_us_order_no
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
        ack = next((e for e in reversed(order_events) if e.get("event_type") in {"BROKER_ACK_RECEIVED", "BROKER_ACK_RECOVERED"}), None)
        symbol, side, requested = str(latest.get("symbol") or "").upper(), str(latest.get("side") or "").upper(), int(latest.get("qty") or 0)
        if not ack or not str(ack.get("broker_order_no") or ""):
            match = match_ambiguous_submit_to_broker_order(
                trade_date=trade_date, symbol=symbol, side=side, requested_qty=requested,
                limit_price=float(latest.get("limit_price") or 0),
                submitted_at_utc=str(latest.get("submitted_at_utc") or latest.get("timestamp") or ""),
                exchange=str(latest.get("exchange") or ""), broker_rows=today_orders or [])
            if match.get("status") != "MATCHED":
                counts["broker_unknown_count"] += 1; unresolved_symbol_sides.append([symbol, side])
                append_order_event("JOURNAL_REPLAY_UNRESOLVED", latest, broker_status=str(match.get("status")), raw_response={"candidate_count": len(match.get("matches") or [])}); continue
            matched = match["match"]
            recovered_no = str(matched.get("raw_order_no") or matched.get("order_no") or matched.get("odno") or "")
            ack = append_order_event("BROKER_ACK_RECOVERED", latest, broker_order_no=recovered_no,
                                     broker_status=str(matched.get("status") or "ACK"), raw_response=matched)
        order_no = str(ack["broker_order_no"])
        try:
            recovered_meta = {**dict(ack.get("meta") or {}),
                "session": ack.get("session"), "session_run_id": ack.get("session_run_id"),
                "session_generation": ack.get("session_generation"), "tick_id": ack.get("tick_id"),
                "prep_run_id": ack.get("prep_run_id"), "submit_attempt_id": ack.get("submit_attempt_id"),
                "position_lifecycle_id": ack.get("position_lifecycle_id") or (ack.get("meta") or {}).get("position_lifecycle_id")}
            if save_order_ack({"client_order_key": key,"symbol": symbol,"exchange": ack.get("exchange"),"side": side,
                "qty_requested": requested,"qty_filled": 0,"order_no": order_no,"status": "ACK",
                "meta": recovered_meta}, trade_date=trade_date):
                counts["db_ack_restored_count"] += 1
                append_order_event("JOURNAL_REPLAY_DB_ACK_RESTORED", ack, broker_order_no=order_no, broker_status="JOURNAL_ACK_DB_RESTORED")
            broker_fill = None
            if provider is not None:
                detail = getattr(provider, "get_fills_by_order_no", None)
                if callable(detail):
                    try:
                        broker_fill = detail(order_no=order_no, symbol=symbol, trade_date=trade_date)
                    except TypeError as exc:
                        counts["failed_count"] += 1
                        unresolved_symbol_sides.append([symbol, side])
                        append_order_event("JOURNAL_REPLAY_FAILED", ack, broker_order_no=order_no, broker_status="PROVIDER_CONTRACT_ERROR", raw_response={"error": str(exc)})
                        continue
                if not broker_fill and isinstance(all_fills, list):
                    matched = [f for f in all_fills if normalize_us_order_no(f.get("order_no")) == normalize_us_order_no(order_no) and str(f.get("symbol") or symbol).upper() == symbol and str(f.get("side") or side).upper() == side]
                    if matched:
                        def _cum(row):
                            return int(row.get("cumulative_filled_qty") or row.get("filled_qty") or row.get("qty") or 0)
                        def _obs(row):
                            return str(row.get("observed_at") or row.get("updated_at") or row.get("filled_at") or "")
                        best = max(matched, key=lambda row: (_cum(row), _obs(row)))
                        later_regression = any(_obs(row) > _obs(best) and _cum(row) < _cum(best) for row in matched)
                        broker_fill = {"filled_qty": _cum(best), "cumulative_filled_qty": _cum(best),
                                       "avg_price": float(best.get("avg_price") or best.get("price") or best.get("price_usd") or 0),
                                       "symbol": best.get("symbol"), "side": best.get("side"),
                                       "status": "EVIDENCE_QUANTITY_REGRESSION" if later_regression else best.get("status")}
            broker_fill_status = str(
                (broker_fill or {}).get("evidence_status")
                or (broker_fill or {}).get("status")
                or "OK"
            ).upper()
            if broker_fill_status in {
                "EVIDENCE_QUANTITY_REGRESSION",
                "EVIDENCE_QUANTITY_CONFLICT",
                "EVIDENCE_QUANTITY_OVERFLOW",
                "FILL_ACCOUNTING_INVARIANT_FAILED",
                "CONTRACT_ERROR",
                "RECONCILE_UPDATE_FAILED",
            }:
                counts["failed_count"] += 1
                unresolved_symbol_sides.append([symbol, side])
                append_order_event(
                    "JOURNAL_REPLAY_FAILED",
                    ack,
                    broker_order_no=order_no,
                    broker_status=broker_fill_status,
                    raw_response=broker_fill,
                )
                continue
            if broker_fill and int(broker_fill.get("filled_qty") or 0) > 0:
                if str(broker_fill.get("symbol") or symbol).upper()!=symbol or str(broker_fill.get("side") or side).upper()!=side:
                    counts["identity_mismatch_count"] += 1; unresolved_symbol_sides.append([symbol,side]); continue
                cumulative=int(broker_fill["filled_qty"])
                result=mark_order_filled_by_reconcile(order_no=order_no,client_order_key=key,symbol=symbol,side=side,
                    filled_qty=cumulative,requested_qty=requested,cumulative_filled_qty=cumulative,
                    avg_price_usd=float(broker_fill.get("avg_price") or 0),source="fills_by_order_no",
                    evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",trade_date=trade_date,meta={**recovered_meta, "journal_replay":True})
                if result.get("status") == "EVIDENCE_QUANTITY_CONFLICT":
                    counts["identity_mismatch_count"] += 1; unresolved_symbol_sides.append([symbol,side])
                    append_order_event("JOURNAL_REPLAY_FAILED", ack, broker_order_no=order_no, broker_status="EVIDENCE_QUANTITY_CONFLICT", raw_response=result)
                    continue
                if result.get("status") != "OK": raise RuntimeError(result.get("status"))
                bucket="broker_full_fill_count" if cumulative>=requested else "broker_partial_fill_count"; counts[bucket]+=1
                append_order_event("JOURNAL_REPLAY_FILL_CONFIRMED",ack,broker_order_no=order_no,
                                   broker_status="BROKER_FILL_CONFIRMED" if cumulative>=requested else "BROKER_PARTIAL_FILL_CONFIRMED")
                append_order_event("ORDER_FILLED" if cumulative>=requested else "ORDER_PARTIALLY_FILLED", ack,
                    broker_order_no=order_no, broker_status="FILLED" if cumulative>=requested else "PARTIALLY_FILLED",
                    raw_response={"cumulative_filled_qty": cumulative, "fill_price": float(broker_fill.get("avg_price") or 0),
                                  "execution_timestamp": broker_fill.get("execution_timestamp") or broker_fill.get("observed_at"),
                                  "broker_execution_id": broker_fill.get("broker_execution_id"), "fees": broker_fill.get("fees")})
            else:
                broker_order = next((o for o in (today_orders or []) if normalize_us_order_no(o.get("order_no") or o.get("odno"))==normalize_us_order_no(order_no) and str(o.get("symbol") or o.get("pdno") or symbol).upper()==symbol and str(o.get("side") or side).upper()==side),None)
                if broker_order:
                    broker_symbol=str(broker_order.get("symbol") or broker_order.get("pdno") or symbol).upper()
                    broker_side=str(broker_order.get("side") or side).upper()
                    if broker_symbol!=symbol or broker_side!=side:
                        counts["identity_mismatch_count"]+=1; unresolved_symbol_sides.append([symbol,side])
                    else:
                        observed_status = str(broker_order.get("status") or "ACK").upper()
                        result = apply_broker_order_observation(trade_date=trade_date, client_order_key=key,
                            raw_order_no=str(broker_order.get("raw_order_no") or broker_order.get("order_no") or order_no),
                            canonical_order_no=normalize_us_order_no(broker_order.get("order_no") or order_no),
                            symbol=symbol, side=side, requested_qty=requested,
                            filled_qty=int(broker_order.get("filled_qty") or 0),
                            remaining_qty=int(broker_order.get("remaining_qty") or requested),
                            broker_status=observed_status, evidence_type="KIS_ORDER_STATUS_ACTUAL",
                            observed_at=broker_order.get("observed_at"), raw_row=broker_order)
                        if result.get("status") != "OK": raise RuntimeError(result.get("status"))
                        if observed_status in {"REJECTED","REJECT","CANCELLED","CANCELED","EXPIRED"}: counts["broker_rejected_count"]+=1
                        else: counts["broker_unfilled_ack_count"]+=1
                else:
                    pre_qty_raw = latest.get("pre_order_position_qty")
                    if pre_qty_raw is None:
                        pre_qty_raw = latest.get("pre_qty")
                    if pre_qty_raw is None and isinstance(latest.get("meta"), dict):
                        pre_qty_raw = latest["meta"].get("pre_order_position_qty")
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
