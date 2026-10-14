"""Append-only, fsync'd order journal used before any broker submission."""
from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _replay_order_row_matches(row: dict, *, order_no: str, symbol: str, side: str,
                              requested_qty: int, source_trade_date: str,
                              journal_event: dict, lookup_context: dict | None = None) -> bool:
    """Use a memoized KIS order only when its complete available identity agrees.

    Rows lacking original requested quantity are NOT suitable for fast-path
    reconstruction; the caller falls back to an independent broker lookup.
    A KST broker date may be one day after its US trade date.
    """
    from trader.us.utils.order_no import normalize_us_order_no

    if not isinstance(row, dict) or requested_qty <= 0:
        return False
    if (normalize_us_order_no(row.get("order_no") or row.get("raw_order_no") or "")
            != normalize_us_order_no(order_no)
            or str(row.get("symbol") or "").upper() != symbol
            or str(row.get("side") or "").upper() != side):
        return False
    if (str(row.get("normalization_result") or "").lower() == "quarantined"
            or row.get("filled_qty_present") is False
            or row.get("requested_qty_present") is False):
        return False

    def integral(value):
        try:
            number = float(value)
            return int(number) if number.is_integer() and 0 <= number < float("inf") else None
        except (TypeError, ValueError, OverflowError):
            return None

    original_qty = next((row.get(k) for k in (
        "requested_qty", "qty_requested", "ord_qty", "ft_ord_qty"
    ) if row.get(k) not in (None, "")), None)
    if integral(original_qty) != requested_qty:
        return False
    filled = integral(row.get("filled_qty"))
    if filled is None or filled > requested_qty:
        return False
    remaining_raw = next((row.get(k) for k in ("broker_open_qty", "remaining_qty")
                          if row.get(k) not in (None, "")), None)
    if remaining_raw is not None:
        remaining = integral(remaining_raw)
        if remaining is None or filled + remaining > requested_qty:
            return False

    metadata = journal_event.get("meta") if isinstance(journal_event.get("meta"), dict) else {}
    ctx = lookup_context or {}
    scoped = (ctx.get("verified") is True
              and ctx.get("source") == "kis_authenticated_order_book"
              and str(ctx.get("query_trade_date") or "").replace("-", "")
                  == str(source_trade_date).replace("-", "")
              and str(ctx.get("market") or "").upper() == "US"
              and bool(ctx.get("account_id")))
    def recorded(key):
        return journal_event.get(key) or metadata.get(key)

    # In normalized KIS rows NASD/NASDAQ refer to the same exchange. Account,
    # market and owner fields can be provider-scoped rather than row-scoped;
    # whenever both sides carry one, a disagreement disqualifies reuse.
    def exchange_name(value):
        return {"NASD": "NASDAQ", "NAS": "NASDAQ", "NYSE": "NYSE",
                "AMEX": "AMEX", "AMS": "AMEX"}.get(str(value or "").upper(),
                                                str(value or "").upper())
    # KIS proves account + market + source day via the authenticated query.
    # It does NOT know the original local session/generation/epoch/owner.
    # Those identities belong to the immutable submit/ACK journal, NEVER
    # to the current tick's lookup context or a synthesized ORDER_OPEN row.
    def canonical_account(value):
        raw = str(value or "").strip().upper()
        # KIS identifies CANO while the durable execution claim stores a
        # SHA256 account fingerprint (env/CANO/product), not raw CANO.
        if scoped and raw == str(ctx.get("broker_account_no") or "").upper() and raw:
            return str(ctx["account_id"]).upper()
        return raw

    for field in ("exchange", "market", "account_id", "account_key", "cano"):
        local = recorded(field)
        actual = row.get(field)
        if actual in (None, "") and field == "market":
            if not scoped:
                return False
            actual = ctx["market"]
        elif actual in (None, "") and field == "account_id" and scoped:
            actual = ctx["account_id"]
        if actual in (None, ""):
            continue
        if local not in (None, ""):
            if field == "exchange":
                if exchange_name(local) != exchange_name(actual):
                    return False
            elif field in {"account_id", "account_key", "cano"}:
                if canonical_account(local) != canonical_account(actual):
                    return False
            elif str(local).upper() != str(actual).upper():
                return False
        if field in {"account_id", "account_key", "cano"} and scoped:
            if canonical_account(actual) != canonical_account(ctx["account_id"]):
                return False

    for field in ("session", "session_run_id", "session_generation",
                  "trading_epoch_id", "strategy_owner"):
        explicit_broker_value = row.get(field)
        immutable = recorded(field)
        if explicit_broker_value not in (None, ""):
            if immutable in (None, "") or str(immutable).upper() != str(explicit_broker_value).upper():
                return False
        # Missing KIS-internal metadata does not imply the CURRENT lookup
        # tick had the same session or epoch as the ORIGINAL submission.
    if row.get("trade_date") in (None, "") and not scoped:
        return False
    if (row.get("account_id") in (None, "") and row.get("cano") in (None, "")
            and not scoped):
        return False

    expected_date = str(source_trade_date).replace("-", "")
    explicit_us_date = row.get("us_trade_date") or row.get("original_us_trade_date")
    if explicit_us_date and str(explicit_us_date).replace("-", "") != expected_date:
        return False
    broker_date = row.get("trade_date")
    if broker_date:
        date_str = str(broker_date).replace("-", "")
        try:
            et_date = datetime.strptime(expected_date, "%Y%m%d")
            valid_source_dates = {
                expected_date, (et_date + timedelta(days=1)).strftime("%Y%m%d"),
            }
            if date_str not in valid_source_dates:
                return False
        except ValueError:
            return False
    return True



def _replay_detail_identity_conflicts(row: dict, *, order_no: str, symbol: str,
                                      side: str, requested_qty: int,
                                      source_trade_date: str, journal_event: dict,
                                      lookup_context: dict | None) -> bool:
    """Reject any explicit broker detail contradicting journal or scoped lookup."""
    from trader.us.utils.order_no import normalize_us_order_no
    if not isinstance(row, dict):
        return True
    metadata = journal_event.get("meta") if isinstance(journal_event.get("meta"), dict) else {}
    context = lookup_context or {}
    for key, target in (("order_no", normalize_us_order_no(order_no)),
                        ("raw_order_no", normalize_us_order_no(order_no))):
        value = row.get(key)
        if value and normalize_us_order_no(value) != target:
            return True
    if (row.get("symbol") and str(row["symbol"]).upper() != symbol
            or row.get("side") and str(row["side"]).upper() != side):
        return True
    for key in ("requested_qty", "qty_requested", "ord_qty", "ft_ord_qty"):
        value = row.get(key)
        if value not in (None, ""):
            try:
                if float(value) != requested_qty:
                    return True
            except (TypeError, ValueError):
                return True
            break
    for key in ("account_id", "account_key", "cano", "market", "session",
                "session_run_id", "session_generation", "trading_epoch_id",
                "strategy_owner", "exchange"):
        value = row.get(key)
        if value in (None, ""):
            continue
        expected = journal_event.get(key) or metadata.get(key)
        if expected in (None, ""):
            expected = context.get("account_id" if key in {"account_id", "account_key", "cano"} else key)
        if expected not in (None, ""):
            lhs, rhs = str(value).upper(), str(expected).upper()
            if key == "exchange":
                aliases = {"NASD": "NASDAQ", "NAS": "NASDAQ", "AMS": "AMEX"}
                lhs, rhs = aliases.get(lhs, lhs), aliases.get(rhs, rhs)
            if lhs != rhs:
                return True
    us_date = row.get("us_trade_date") or row.get("original_us_trade_date")
    if us_date and str(us_date).replace("-", "") != str(source_trade_date).replace("-", ""):
        return True
    broker_date = row.get("trade_date")
    if broker_date:
        try:
            base = datetime.strptime(str(source_trade_date).replace("-", ""), "%Y%m%d")
            valid_dates = {base.strftime("%Y%m%d"),
                           (base + timedelta(days=1)).strftime("%Y%m%d")}
            if str(broker_date).replace("-", "") not in valid_dates:
                return True
        except ValueError:
            return True
    return False


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
    canonical_no = ""
    if broker_order_no:
        from trader.us.utils.order_no import normalize_us_order_no
        canonical_no = normalize_us_order_no(broker_order_no)
    is_sell = str(intent.get("side") or "").upper() == "SELL"
    sell_audit = {
        key: (intent.get(key) if intent.get(key) is not None else meta.get(key)) if is_sell else None
        for key in (
            "exit_family", "broker_avg_price", "broker_avg_price_source", "entry_price",
            "decision_price", "executable_price", "return_rate_at_decision", "pnl_pct",
            "expected_realized_pnl",
        )
    }
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
        "pre_order_holding_qty": intent.get("pre_order_holding_qty", meta.get("pre_order_holding_qty", pre_order_position_qty)),
        "pre_order_orderable_qty": intent.get("pre_order_orderable_qty", meta.get("pre_order_orderable_qty")),
        "requested_sell_qty": meta.get("requested_sell_qty") if is_sell else None,
        "expected_post_order_qty": meta.get("expected_post_order_qty"),
        "pre_order_position_source": intent.get("pre_order_position_source") or meta.get("pre_order_position_source"),
        "position_lifecycle_id": position_lifecycle_id,
        "limit_price": limit_price, "order_price": intent.get("order_price") or meta.get("order_price"),
        "notional_usd": intent.get("notional_usd") or meta.get("notional_usd"),
        **sell_audit,
        "meta": meta,
        "cumulative_filled_qty": (raw_response or {}).get("cumulative_filled_qty") if isinstance(raw_response, dict) else None,
        "fill_price": (raw_response or {}).get("fill_price") if isinstance(raw_response, dict) else None,
        "broker_execution_id": (raw_response or {}).get("broker_execution_id") if isinstance(raw_response, dict) else None,
        "execution_timestamp": (raw_response or {}).get("execution_timestamp") if isinstance(raw_response, dict) else None,
        "fees": (raw_response or {}).get("fees") if isinstance(raw_response, dict) else None,
        "timestamp": datetime.now(timezone.utc).isoformat(), "raw_response_hash": hashlib.sha256(raw.encode()).hexdigest(),
    }
    idem_material = "|".join(str(v or "") for v in (
        td, event.get("submit_attempt_id"), canonical_no, event_type,
        event.get("cumulative_filled_qty"), event.get("broker_execution_id"),
        event.get("raw_response_hash")))
    event["idempotency_key"] = hashlib.sha256(idem_material.encode()).hexdigest()
    event["raw_broker_order_no"] = broker_order_no
    event["canonical_broker_order_no"] = canonical_no
    from trader.us.db.repos import append_us_order_event
    db_event = {
        "event_id": event["event_id"], "trade_date": td, "event_type": event_type,
        "event_timestamp": event["timestamp"], "session": event.get("session"),
        "session_run_id": event.get("session_run_id"), "tick_id": event.get("tick_id"),
        "client_order_key": event.get("client_order_key"), "submit_attempt_id": event.get("submit_attempt_id"),
        "symbol": event.get("symbol"), "side": event.get("side"), "requested_qty": event.get("requested_qty"),
        "raw_broker_order_no": broker_order_no, "canonical_broker_order_no": canonical_no,
        "position_lifecycle_id": event.get("position_lifecycle_id"),
        "profit_capture_stage": event.get("profit_capture_stage"),
        "cumulative_filled_qty": event.get("cumulative_filled_qty"),
        "payload": event, "idempotency_key": event["idempotency_key"],
    }
    db_saved = append_us_order_event(db_event)
    fallback_allowed = bool(os.getenv("PYTEST_CURRENT_TEST") or os.getenv("US_OFFLINE") == "1" or os.getenv("OFFLINE") == "1")
    if not db_saved and not fallback_allowed:
        raise OSError("durable_order_event_ledger_unavailable")
    event["ledger_source"] = "postgres" if db_saved else "jsonl_test_fallback"
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
    from trader.us.db.repos import load_us_order_events
    db_events = load_us_order_events(trade_date, session_run_id=session_run_id)
    if db_events is not None:
        return db_events
    fallback_allowed = bool(os.getenv("PYTEST_CURRENT_TEST") or os.getenv("US_OFFLINE") == "1" or os.getenv("OFFLINE") == "1")
    if not fallback_allowed:
        raise OSError("durable_order_event_ledger_unavailable")
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
    aborted = {
        str(event.get("submit_attempt_id") or "").strip()
        for event in events
        if event.get("event_type") == "BROKER_SUBMIT_ABORTED_BEFORE_BOUNDARY"
    }
    for event in events:
        attempt = str(event.get("submit_attempt_id") or "").strip()
        if not attempt:
            continue
        event_type = str(event.get("event_type") or "")
        if event_type == "BROKER_SUBMIT_STARTED" and attempt not in aborted:
            submits.setdefault(attempt, event)
        lifecycle.setdefault(attempt, set()).add(event_type)
        if attempt in aborted:
            continue
        if event_type in {"ORDER_PARTIALLY_FILLED", "ORDER_FILLED"}:
            event_key = (attempt, event.get("broker_order_no"), event_type, event.get("raw_response_hash"))
            fill_execution_keys.add(event_key)
            if event_type == "ORDER_PARTIALLY_FILLED": partial_observation_keys.add(event_key)
    ack = {a for a, types in lifecycle.items() if a not in aborted and types & {"BROKER_ACK_RECEIVED", "BROKER_ACK_RECOVERED"}}
    rejected = {a for a, types in lifecycle.items() if a not in aborted and "ORDER_REJECTED" in types}
    filled = {a for a, types in lifecycle.items() if a not in aborted and "ORDER_FILLED" in types}
    unresolved_submit_count = sum(
        1
        for attempt, types in lifecycle.items()
        if attempt in submits
        and attempt not in aborted
        and (
            not (types & {"BROKER_ACK_RECEIVED", "BROKER_ACK_RECOVERED", "ORDER_REJECTED"})
            or (
                bool(types & {"BROKER_ACK_RECEIVED", "BROKER_ACK_RECOVERED"})
                and not (types & {"DB_ACK_PERSISTED", "JOURNAL_REPLAY_DB_ACK_RESTORED"})
            )
        )
    )
    buy = sum(str(e.get("side") or "").upper() == "BUY" for e in submits.values())
    sell = sum(str(e.get("side") or "").upper() == "SELL" for e in submits.values())
    return {"scope": "trade_day", "orders_sent_total": len(submits), "buy_orders_count": buy,
            "sell_orders_count": sell, "orders_ack_total": len(ack), "orders_reject_total": len(rejected),
            "orders_fill_confirmed": len(filled), "fill_execution_row_count": len(fill_execution_keys),
            "partial_fill_observation_count": len(partial_observation_keys),
            "unresolved_submit_attempt_count": unresolved_submit_count,
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
    if submitted.tzinfo is None:
        submitted = submitted.replace(tzinfo=timezone.utc)
    from zoneinfo import ZoneInfo
    wanted_trade_date = str(trade_date).replace("-", "")
    eligible = []
    for row in broker_rows or []:
        if str(row.get("symbol") or row.get("pdno") or "").upper() != symbol.upper(): continue
        if str(row.get("side") or "").upper() != side.upper(): continue
        if int(row.get("requested_qty") or row.get("qty") or 0) != int(requested_qty): continue
        try: row_exchange = normalize_us_exchange(str(row.get("exchange") or row.get("raw_exchange") or exchange))
        except Exception: row_exchange = str(row.get("exchange") or row.get("raw_exchange") or "").upper()
        if wanted_exchange and row_exchange and row_exchange != wanted_exchange: continue
        row_price = float(row.get("limit_price") or row.get("order_price") or 0)
        if limit_price > 0 and row_price > 0 and abs(row_price - limit_price) > price_tolerance: continue
        time_raw = row.get("submitted_at_utc") or row.get("order_timestamp")
        if not time_raw: continue
        try:
            observed = datetime.fromisoformat(str(time_raw).replace("Z", "+00:00"))
            if observed.tzinfo is None: observed = observed.replace(tzinfo=timezone.utc)
            row_trade_date = str(
                row.get("us_trade_date")
                or observed.astimezone(ZoneInfo("America/New_York")).date().isoformat()
            ).replace("-", "")
            if row_trade_date != wanted_trade_date: continue
            if abs((observed - submitted).total_seconds()) > window: continue
        except Exception: continue
        eligible.append(row)
    groups: dict[str, list[dict]] = {}
    from trader.us.utils.order_no import normalize_us_order_no
    for row in eligible:
        key = normalize_us_order_no(row.get("original_order_no") or row.get("orgn_odno") or row.get("order_no"))
        if key: groups.setdefault(key, []).append(row)
    matches=[]
    priority={"ACK":1,"ACK_PENDING":1,"OPEN":2,"PARTIALLY_FILLED":3,"FILLED":4}
    for canonical, rows in groups.items():
        identities={(str(r.get("symbol") or "").upper(),str(r.get("side") or "").upper(),str(r.get("exchange") or "").upper(),int(r.get("requested_qty") or 0)) for r in rows}
        if len(identities)!=1:
            return {"status":"AMBIGUOUS_BROKER_EVIDENCE_CONFLICT","matches":rows}
        quantities={int(r.get("requested_qty") or 0) for r in rows}
        if len(quantities)!=1: return {"status":"AMBIGUOUS_BROKER_EVIDENCE_CONFLICT","matches":rows}
        best=max(rows,key=lambda r:(priority.get(str(r.get("status") or "").upper(),0),str(r.get("observed_at") or r.get("submitted_at_utc") or "")))
        merged={**best,"canonical_order_no":canonical,"order_no":best.get("raw_order_no") or best.get("order_no"),
                "filled_qty":max(int(r.get("filled_qty") or 0) for r in rows),
                "submitted_at_utc":min(str(r.get("submitted_at_utc")) for r in rows if r.get("submitted_at_utc"))}
        merged["remaining_qty"]=max(0,int(merged.get("requested_qty") or 0)-int(merged.get("filled_qty") or 0))
        matches.append(merged)
    if len(matches) == 1: return {"status": "MATCHED", "match": matches[0], "matches": matches}
    return {"status": "NO_MATCH" if not matches else "AMBIGUOUS_MULTIPLE_BROKER_CANDIDATES", "matches": matches}


def replay_order_journal(trade_date: str, session_run_id: str | None = None,
                         tick_id: str | None = None, provider: Any | None = None,
                         include_active_claims: bool = False,
                         active_claims_only: bool = False) -> dict:
    """Restore DB evidence and independently classify broker truth; never resubmit."""
    events = load_order_events(trade_date, session_run_id=session_run_id)
    if tick_id:
        events = [e for e in events if e.get("tick_id") == tick_id]
    active_claim_errors = []
    active_attempt_keys: set[tuple[str, str]] = set()
    if include_active_claims:
        try:
            from trader.us.db.repos import load_active_execution_claim_attempts
            active_claims = load_active_execution_claim_attempts()
            for claim in active_claims:
                source_date = str(claim.get("trade_date") or "")
                key = str(claim.get("client_order_key") or "")
                attempt_id = str(claim.get("active_attempt_id") or "")
                if not source_date or not key or not attempt_id:
                    active_claim_errors.append({
                        "symbol": "", "side": "", "reason": "active_claim_identity_incomplete",
                    })
                    continue
                active_attempt_keys.add((key, attempt_id))
                if source_date == trade_date and not active_claims_only:
                    continue
                source_events = load_order_events(source_date)
                attempt_events = [
                    event for event in source_events
                    if str(event.get("client_order_key") or "") == key
                    and str(event.get("submit_attempt_id") or "") == attempt_id
                ]
                if not any(event.get("event_type") == "BROKER_SUBMIT_STARTED" for event in attempt_events):
                    active_claim_errors.append({
                        "symbol": "", "side": "", "reason": "active_claim_submit_journal_missing",
                    })
                    continue
                events.extend(attempt_events)
        except Exception as exc:
            active_claim_errors.append({
                "symbol": "", "side": "", "reason": f"active_claim_lookup_failed:{type(exc).__name__}",
            })
    if active_claims_only:
        events = [
            event for event in events
            if (
                str(event.get("client_order_key") or ""),
                str(event.get("submit_attempt_id") or ""),
            ) in active_attempt_keys
        ]
        unique_events = {}
        for event in events:
            event_id = str(event.get("event_id") or "")
            identity = event_id or repr(sorted(event.items()))
            unique_events[identity] = event
        events = list(unique_events.values())
    grouped: dict[str, list[dict]] = {}
    for event in events:
        key = str(event.get("client_order_key") or "").strip()
        if key: grouped.setdefault(key, []).append(event)
    counts = {"db_ack_restored_count": 0, "broker_full_fill_count": 0,
              "broker_partial_fill_count": 0, "broker_unfilled_ack_count": 0,
              "broker_rejected_count": 0, "broker_unknown_count": 0,
              "identity_mismatch_count": 0, "failed_count": 0}
    unresolved_symbol_sides: list[list[str]] = []
    for error in active_claim_errors:
        counts["failed_count"] += 1
        unresolved_symbol_sides.append([error["symbol"], error["side"]])
    if active_claims_only and not events:
        unresolved = counts["failed_count"]
        return {
            "status": "ERROR" if unresolved else "OK",
            **counts,
            "restored_ack_count": 0,
            "filled_count": 0,
            "broker_confirmed_count": 0,
            "unresolved_count": unresolved,
            "unresolved_symbol_sides": unresolved_symbol_sides,
            "events_scanned": 0,
            "balance_fetched": False,
        }
    from trader.us.db.repos import save_order_ack, mark_order_filled_by_reconcile, apply_broker_order_observation
    from trader.us.utils.order_no import normalize_us_order_no
    # Query authoritative snapshots once per timeout, not once per journal row.
    balance = None
    today_orders = None
    # The 2026-10-13 incident made >600 KIS CCNL requests in 35 ticks.
    # Historical active claims often share an original trade date; do not
    # fetch that broker order book repeatedly within one reconciliation.
    # This memo never survives a replay/tick, so genuine later broker fills
    # are rediscovered on the next fresh invocation.
    orders_by_trade_date: dict[str, list[dict] | None] = {}
    source_scope_by_trade_date: dict[str, dict] = {}
    fills_by_trade_date: dict[str, list[dict] | None] = {}

    def orders_for_trade_date(source_trade_date: str) -> list[dict] | None:
        if source_trade_date not in orders_by_trade_date:
            try:
                method = getattr(provider, "get_today_orders", None)
                rows = method(trade_date=source_trade_date) if callable(method) else None
                orders_by_trade_date[source_trade_date] = rows if isinstance(rows, list) else None
                provenance = getattr(provider, "get_order_lookup_provenance", None)
                scope = provenance(source_trade_date) if callable(provenance) else {}
                source_scope_by_trade_date[source_trade_date] = scope if isinstance(scope, dict) else {}
            except Exception:
                orders_by_trade_date[source_trade_date] = None
        return orders_by_trade_date[source_trade_date]

    def fills_for_trade_date(source_trade_date: str) -> list[dict] | None:
        if source_trade_date not in fills_by_trade_date:
            try:
                from trader.us.execution.fills import get_fills_today
                response = get_fills_today(provider=provider, trade_date=source_trade_date)
                fills_by_trade_date[source_trade_date] = (
                    response.get("fills") if response.get("status") == "OK" else None
                )
            except Exception:
                fills_by_trade_date[source_trade_date] = None
        return fills_by_trade_date[source_trade_date]

    all_fills = None
    if provider is not None:
        try: balance = provider.get_balance(force_refresh=True)
        except TypeError: balance = provider.get_balance()
        except Exception: balance = None
        today_orders = orders_for_trade_date(trade_date)
        all_fills = fills_for_trade_date(trade_date)
    positions_by_symbol = {}
    if isinstance(balance, dict):
        for pos in balance.get("positions") or []:
            positions_by_symbol[str(pos.get("symbol") or pos.get("pdno") or "").upper()] = pos

    for key, order_events in grouped.items():
        types = {e.get("event_type") for e in order_events}
        if "BROKER_SUBMIT_STARTED" not in types or "BROKER_SUBMIT_ABORTED_BEFORE_BOUNDARY" in types:
            continue
        # Replay-generated OPEN, PARTIAL_FILL, recovery and error events may
        # contain synthetic defaults for qty, generation and session. They
        # describe observation state, NOT the original submission identity.
        # Anchor every later KIS lookup/observation to this immutable intent.
        latest = next(e for e in order_events if e.get("event_type") == "BROKER_SUBMIT_STARTED")
        event_trade_date = str(latest.get("trade_date") or trade_date)
        event_orders = today_orders
        event_fills = all_fills
        if provider is not None and event_trade_date != trade_date:
            event_orders = orders_for_trade_date(event_trade_date)
            event_fills = fills_for_trade_date(event_trade_date)
        append_order_event("JOURNAL_REPLAY_STARTED", latest)
        ack = next((e for e in reversed(order_events) if e.get("event_type") in {"BROKER_ACK_RECEIVED", "BROKER_ACK_RECOVERED"}), None)
        symbol, side, requested = str(latest.get("symbol") or "").upper(), str(latest.get("side") or "").upper(), int(latest.get("qty") or 0)
        if not ack or not str(ack.get("broker_order_no") or ""):
            match = match_ambiguous_submit_to_broker_order(
                trade_date=event_trade_date, symbol=symbol, side=side, requested_qty=requested,
                limit_price=float(latest.get("limit_price") or 0),
                submitted_at_utc=str(latest.get("submitted_at_utc") or latest.get("timestamp") or ""),
                exchange=str(latest.get("exchange") or ""), broker_rows=event_orders or [])
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
                "meta": recovered_meta}, trade_date=event_trade_date):
                counts["db_ack_restored_count"] += 1
                append_order_event("JOURNAL_REPLAY_DB_ACK_RESTORED", ack, broker_order_no=order_no, broker_status="JOURNAL_ACK_DB_RESTORED")
            broker_fill = None
            if provider is not None:
                # A fresh account-scoped order book was already fetched for
                # this source trade date in THIS invocation. Match the exact
                # broker order/symbol/side here instead of causing another
                # full KIS CCNL REST request per unresolved action. Reject
                # quarantined/quantity-regressing observations; do not cache
                # across ticks or generations.
                if isinstance(event_orders, list):
                    matches = [
                        row for row in event_orders
                        if _replay_order_row_matches(
                            row, order_no=order_no, symbol=symbol, side=side,
                            requested_qty=requested, source_trade_date=event_trade_date,
                            journal_event=latest,
                            lookup_context=source_scope_by_trade_date.get(event_trade_date),
                        )
                    ]
                    if matches:
                        def _fill_obs_time(row):
                            return str(
                                row.get("observed_at") or row.get("updated_at")
                                or row.get("order_timestamp") or row.get("order_time") or ""
                            )
                        best = max(
                            matches,
                            key=lambda row: (int(row.get("filled_qty") or 0), _fill_obs_time(row)),
                        )
                        conflict = any(
                            _fill_obs_time(row) > _fill_obs_time(best)
                            and int(row.get("filled_qty") or 0) < int(best.get("filled_qty") or 0)
                            for row in matches
                        )
                        broker_fill = dict(best)
                        broker_fill["filled_qty"] = int(best.get("filled_qty") or 0)
                        broker_fill["status"] = (
                            "EVIDENCE_QUANTITY_REGRESSION" if conflict
                            else str(best.get("status") or "OK")
                        )
                if broker_fill is None:
                    detail = getattr(provider, "get_fills_by_order_no", None)
                    if callable(detail):
                        try:
                            broker_fill = detail(order_no=order_no, symbol=symbol, trade_date=event_trade_date)
                        except TypeError as exc:
                            counts["failed_count"] += 1
                            unresolved_symbol_sides.append([symbol, side])
                            append_order_event("JOURNAL_REPLAY_FAILED", ack, broker_order_no=order_no, broker_status="PROVIDER_CONTRACT_ERROR", raw_response={"error": str(exc)})
                            continue
                # Direct per-order broker lookup is allowed only if no returned
                # explicit identity contradicts the durable submitted action.
                # Missing fields remain the KIS API call's scoped contract;
                # conflicting fields are NEVER silently accepted as a fallback.
                if (isinstance(broker_fill, dict)
                        and _replay_detail_identity_conflicts(
                            broker_fill, order_no=order_no, symbol=symbol,
                            side=side, requested_qty=requested,
                            source_trade_date=event_trade_date,
                            journal_event=latest,
                            lookup_context=source_scope_by_trade_date.get(event_trade_date),
                        )):
                    broker_fill = {"status": "EVIDENCE_IDENTITY_CONFLICT",
                                   "symbol": symbol, "side": side}
                if not broker_fill and isinstance(event_fills, list):
                    matched = [f for f in event_fills if normalize_us_order_no(f.get("order_no")) == normalize_us_order_no(order_no) and str(f.get("symbol") or symbol).upper() == symbol and str(f.get("side") or side).upper() == side and not _replay_detail_identity_conflicts(
                        f, order_no=order_no, symbol=symbol, side=side,
                        requested_qty=requested, source_trade_date=event_trade_date,
                        journal_event=latest,
                        lookup_context=source_scope_by_trade_date.get(event_trade_date),
                    )]
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
            if broker_fill_status == "EVIDENCE_IDENTITY_CONFLICT":
                counts["identity_mismatch_count"] += 1
                unresolved_symbol_sides.append([symbol, side])
                append_order_event("JOURNAL_REPLAY_FAILED", ack,
                                   broker_order_no=order_no,
                                   broker_status=broker_fill_status,
                                   raw_response=broker_fill)
                continue
            if broker_fill_status in {
                "EVIDENCE_QUANTITY_REGRESSION",
                "EVIDENCE_IDENTITY_CONFLICT",
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
                fill_price = float(broker_fill.get("avg_price") or broker_fill.get("avg_price_usd") or 0)
                result = apply_broker_order_observation(
                    trade_date=event_trade_date,
                    client_order_key=key,
                    raw_order_no=order_no,
                    canonical_order_no=normalize_us_order_no(order_no),
                    symbol=symbol,
                    side=side,
                    requested_qty=requested,
                    filled_qty=cumulative,
                    remaining_qty=(
                        int(broker_fill["remaining_qty"])
                        if broker_fill.get("remaining_qty") not in (None, "")
                        else max(0, requested - cumulative)
                    ),
                    broker_status=(
                        str(broker_fill.get("status") or "").upper().replace("CANCELED", "CANCELLED")
                        if str(broker_fill.get("status") or "").upper() in
                        {"CANCELLED", "CANCELED", "EXPIRED", "REJECTED"}
                        else "FILLED" if cumulative >= requested else "PARTIALLY_FILLED"
                    ),
                    evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
                    observed_at=broker_fill.get("observed_at"),
                    raw_row={**broker_fill, "requested_qty": requested, "avg_price": fill_price},
                )
                if result.get("status") == "EVIDENCE_QUANTITY_CONFLICT":
                    counts["identity_mismatch_count"] += 1; unresolved_symbol_sides.append([symbol,side])
                    append_order_event("JOURNAL_REPLAY_FAILED", ack, broker_order_no=order_no, broker_status="EVIDENCE_QUANTITY_CONFLICT", raw_response=result)
                    continue
                if result.get("status") != "OK": raise RuntimeError(result.get("status"))
                if result.get("observation_ignored"):
                    counts["failed_count"] += 1
                    unresolved_symbol_sides.append([symbol, side])
                    append_order_event(
                        "JOURNAL_REPLAY_FAILED",
                        ack,
                        broker_order_no=order_no,
                        broker_status=str(result["observation_ignored"]),
                        raw_response=result,
                    )
                    continue
                bucket="broker_full_fill_count" if cumulative>=requested else "broker_partial_fill_count"; counts[bucket]+=1
                append_order_event("JOURNAL_REPLAY_FILL_CONFIRMED",ack,broker_order_no=order_no,
                                   broker_status="BROKER_FILL_CONFIRMED" if cumulative>=requested else "BROKER_PARTIAL_FILL_CONFIRMED")
                append_order_event("ORDER_FILLED" if cumulative>=requested else "ORDER_PARTIALLY_FILLED", ack,
                    broker_order_no=order_no, broker_status="FILLED" if cumulative>=requested else "PARTIALLY_FILLED",
                    raw_response={"cumulative_filled_qty": cumulative, "fill_price": fill_price,
                                  "execution_timestamp": broker_fill.get("execution_timestamp") or broker_fill.get("observed_at"),
                                  "broker_execution_id": broker_fill.get("broker_execution_id"), "fees": broker_fill.get("fees")})
            else:
                broker_order = next((o for o in (event_orders or []) if _replay_order_row_matches(
                    o, order_no=order_no, symbol=symbol, side=side,
                    requested_qty=requested, source_trade_date=event_trade_date,
                    journal_event=latest,
                    lookup_context=source_scope_by_trade_date.get(event_trade_date),
                )), None)
                if broker_order:
                    broker_symbol=str(broker_order.get("symbol") or broker_order.get("pdno") or symbol).upper()
                    broker_side=str(broker_order.get("side") or side).upper()
                    if broker_symbol!=symbol or broker_side!=side:
                        counts["identity_mismatch_count"]+=1; unresolved_symbol_sides.append([symbol,side])
                    else:
                        observed_status = str(broker_order.get("status") or "ACK").upper()
                        result = apply_broker_order_observation(trade_date=event_trade_date, client_order_key=key,
                            raw_order_no=str(broker_order.get("raw_order_no") or broker_order.get("order_no") or order_no),
                            canonical_order_no=normalize_us_order_no(broker_order.get("order_no") or order_no),
                            symbol=symbol, side=side, requested_qty=requested,
                            filled_qty=broker_order.get("filled_qty"),
                            remaining_qty=int(broker_order.get("remaining_qty") or requested),
                            broker_status=observed_status, evidence_type="KIS_ORDER_STATUS_ACTUAL",
                            observed_at=broker_order.get("observed_at"), raw_row=broker_order)
                        if result.get("status") != "OK": raise RuntimeError(result.get("status"))
                        if result.get("requires_reconcile"):
                            counts["broker_unknown_count"] += 1
                            unresolved_symbol_sides.append([symbol, side])
                            append_order_event(
                                "JOURNAL_REPLAY_UNRESOLVED",
                                ack,
                                broker_order_no=order_no,
                                broker_status=str(result.get("filled_quantity_status") or observed_status),
                                raw_response=result,
                            )
                            continue
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
                                evidence_type="BALANCE_DELTA_SYNTHETIC",trade_date=event_trade_date,meta={"journal_replay":True})
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
