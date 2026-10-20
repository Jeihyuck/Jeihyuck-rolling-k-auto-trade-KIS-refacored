#!/usr/bin/env python3
"""Read-only KIS original-day evidence check for US active execution claims.

Run on the approved WSL practice operator host with its existing environment.
Never submits or cancels orders, writes DB/journal rows or releases claim fences.
Raw account keys, broker order numbers and journal payloads are never printed.

PYTHONPATH=. python scripts/us_kis_original_day_evidence.py \
  --from-date 2026-10-01 --to-date 2026-10-20
"""
from __future__ import annotations

import argparse
from datetime import date
import json
import math

from sqlalchemy import text

from trader.us.db.repos import _get_engine_or_none, _us_execution_claim_scope
from trader.us.execution.order_journal import (
    _replay_detail_identity_conflicts,
    _replay_order_row_matches,
    load_order_events,
)

# A single read-only, account/env/epoch-scoped snapshot.  Source-day KIS
# lookups happen separately with no write capability and are never deemed
# sufficient merely because a result set is empty.
CLAIMS_SQL = """
SELECT c.trade_date, c.strategy_owner, c.action_state, a.attempt_id,
       a.client_order_key, a.requested_qty,
       o.symbol, o.side, o.status AS local_status, o.qty_filled AS local_qty
FROM us_execution_claims c
JOIN us_execution_attempts a ON a.action_key = c.action_key
 AND a.attempt_id = c.active_attempt_id
LEFT JOIN us_orders o ON o.client_order_key = a.client_order_key
 AND o.trade_date = c.trade_date AND o.env = c.env
 AND o.trading_epoch_id = c.trading_epoch_id
WHERE c.env = :env AND c.account_id = :account_id
 AND c.trading_epoch_id = :trading_epoch_id AND c.market = :market
 AND c.active_attempt_id IS NOT NULL
 AND c.trade_date BETWEEN :start_date AND :end_date
ORDER BY c.trade_date, o.symbol, a.attempt_id
LIMIT 2000
"""


def _safe_qty(value):
    if isinstance(value, bool):
        return None
    try:
        n = float(value)
        return int(n) if math.isfinite(n) and n >= 0 and n.is_integer() else None
    except (ValueError, TypeError, OverflowError):
        return None


def _safe_price(value):
    try:
        n = float(value)
        return n if math.isfinite(n) and n > 0 else None
    except (ValueError, TypeError, OverflowError):
        return None


def verify_claim(row: dict, events: list[dict], broker_rows: list[dict] | None,
                 scope: dict) -> dict:
    """Never return a terminal result without full local and broker identity."""
    day = str(row.get("trade_date") or "")
    key = str(row.get("client_order_key") or "")
    attempt = str(row.get("attempt_id") or "")
    history = [
        event for event in events
        if str(event.get("client_order_key") or "") == key
        and str(event.get("submit_attempt_id") or "") == attempt
    ]
    submit = next(
        (e for e in reversed(history)
         if e.get("event_type") == "BROKER_SUBMIT_STARTED"), None,
    )
    ack = next(
        (e for e in reversed(history)
         if e.get("event_type") in {"BROKER_ACK_RECEIVED", "BROKER_ACK_RECOVERED"}
         and e.get("broker_order_no")), None,
    )
    symbol = str(row.get("symbol") or (submit or {}).get("symbol") or "").upper()
    side = str(row.get("side") or (submit or {}).get("side") or "").upper()
    qty = _safe_qty(row.get("requested_qty"))
    base = {
        "trade_date": day, "symbol": symbol, "side": side,
        "strategy_owner": str(row.get("strategy_owner") or ""),
        "requested_qty": qty, "local_status": str(row.get("local_status") or ""),
        "local_filled_qty": row.get("local_qty"),
        "proof_status": "UNRESOLVED", "actual_filled_qty": None,
        "actual_avg_price": None, "terminal_proven": False,
        "fence_release_authorized": False, "database_mutated": False,
        "reason": "BROKER_PROOF_MISSING",
    }
    if (not submit or not ack or not key or not attempt or not symbol
            or side not in {"BUY", "SELL"} or qty is None or qty <= 0):
        return {**base, "reason": "MISSING_ORIGINAL_ATTEMPT_IDENTITY"}
    # Never confirm a client action by looking only at its newest attempt.
    # A prior ACK/newer retry with another KIS order number is exactly the
    # case where a single us_orders row cannot safely resolve exposure.
    from trader.us.utils.order_no import normalize_us_order_no
    all_key_events = [
        event for event in events
        if str(event.get("client_order_key") or "") == key
    ]
    ack_numbers = {
        normalize_us_order_no(event.get("broker_order_no"))
        for event in all_key_events
        if event.get("event_type") in {"BROKER_ACK_RECEIVED", "BROKER_ACK_RECOVERED"}
        and event.get("broker_order_no")
    }
    live_attempts = {
        str(event.get("submit_attempt_id") or "")
        for event in all_key_events
        if event.get("event_type") == "BROKER_SUBMIT_STARTED"
    }
    if len(ack_numbers) > 1 or len(live_attempts) > 1:
        return {**base, "reason": "MULTI_ATTEMPT_OR_BROKER_ORDER_COLLISION"}
    if symbol != str(submit.get("symbol") or "").upper() or (
        side != str(submit.get("side") or "").upper()
    ) or qty != _safe_qty(submit.get("qty")):
        return {**base, "reason": "LOCAL_SUBMIT_IDENTITY_CONFLICT"}
    if not (scope.get("verified") is True
            and scope.get("source") == "kis_authenticated_order_book"
            and str(scope.get("query_trade_date") or "").replace("-", "")
                == day.replace("-", "")
            and str(scope.get("market") or "").upper() == "US"
            and bool(scope.get("account_id"))):
        return {**base, "reason": "BROKER_ACCOUNT_OR_DATE_SCOPE_NOT_VERIFIED"}
    if not isinstance(broker_rows, list):
        return {**base, "reason": "BROKER_HISTORY_FETCH_FAILED"}
    order_no = str(ack.get("broker_order_no") or "")
    matches = [
        order for order in broker_rows
        if _replay_order_row_matches(
            order, order_no=order_no, symbol=symbol, side=side,
            requested_qty=qty, source_trade_date=day,
            journal_event=submit, lookup_context=scope,
        ) and not _replay_detail_identity_conflicts(
            order, order_no=order_no, symbol=symbol, side=side,
            requested_qty=qty, source_trade_date=day,
            journal_event=submit, lookup_context=scope,
        )
    ]
    if not matches:
        return {**base, "reason": "NO_EXACT_ORDER_IN_KIS_HISTORY"}
    evidence = {
        (
            str(o.get("status") or "").upper().replace("CANCELED", "CANCELLED"),
            _safe_qty(o.get("filled_qty")),
            _safe_price(o.get("avg_price") or o.get("avg_price_usd")),
        )
        for o in matches
    }
    # Two differing cumulative observations could be a stale page or a
    # crossed execution update. A read-only audit must not choose the
    # favourable one or silently certify a terminal state.
    if len(evidence) != 1:
        return {**base, "reason": "CONFLICTING_BROKER_OBSERVATIONS"}
    status, filled, avg_price = evidence.pop()
    if filled is None or filled > qty:
        return {**base, "reason": "MISSING_OR_INVALID_CUMULATIVE_QTY"}
    if filled > 0 and avg_price is None:
        return {**base, "reason": "MISSING_ACTUAL_FILL_PRICE"}
    result = {
        **base, "actual_filled_qty": filled,
        "actual_avg_price": avg_price if filled else None,
    }
    if filled == qty and status in {"FILLED", "CLOSED", "EXECUTED"}:
        return {**result, "proof_status": "VERIFIED_FULL_FILL",
                "terminal_proven": True, "reason": "KIS_EXACT_ORDER_CUMULATIVE"}
    if status in {"CANCELLED", "REJECTED"}:
        return {
            **result,
            "proof_status": (
                "VERIFIED_PARTIAL_TERMINAL" if filled > 0
                else "VERIFIED_ZERO_FILL_TERMINAL"
            ),
            "terminal_proven": True,
            "remaining_target_qty": qty - filled,
            "reason": "KIS_EXPLICIT_TERMINAL_ORDER",
        }
    if status in {"ACK", "OPEN", "PARTIALLY_FILLED", "PARTIAL"}:
        return {**result, "proof_status": "BROKER_ORDER_STILL_ACTIVE",
                "reason": "KEEP_PENDING_ATTEMPT_FENCED"}
    return {**result, "reason": "UNRECOGNIZED_OR_INCOMPLETE_BROKER_STATE"}


def collect_evidence(rows, *, provider, event_loader=load_order_events):
    """Read-only day-batched KIS queries. Failure of one day fails closed."""
    source_cache = {}
    reports = []
    for row in rows:
        day = str(row.get("trade_date") or "")
        if day not in source_cache:
            journal_events = event_loader(day)
            try:
                broker_rows = provider.get_today_orders(trade_date=day)
                scope = provider.get_order_lookup_provenance(day)
            except Exception:
                broker_rows, scope = None, {}
            source_cache[day] = (journal_events, broker_rows, scope)
        events, broker_rows, scope = source_cache[day]
        reports.append(verify_claim(dict(row), events, broker_rows, scope))
    return reports


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    parser.add_argument("--require-all-terminal", action="store_true")
    args = parser.parse_args(argv)
    start, end = date.fromisoformat(args.from_date), date.fromisoformat(args.to_date)
    if start > end:
        raise SystemExit("from-date must not exceed to-date")
    scope = _us_execution_claim_scope()
    if (scope.get("env") != "practice"
            or any(not scope.get(k) for k in
                   ("env", "account_id", "market", "trading_epoch_id"))
            or str(scope.get("market")).upper() != "US"):
        raise SystemExit("SCOPE_NOT_VERIFIED: practice US claim audit only")
    engine = _get_engine_or_none()
    if engine is None:
        raise SystemExit("DATABASE_UNAVAILABLE: no fallback to empty result")
    params = {**scope, "start_date": start, "end_date": end}
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text("SET TRANSACTION READ ONLY"))
            rows = conn.execute(text(CLAIMS_SQL), params).mappings().all()
    from trader.us.data_provider import USDataProvider
    provider = USDataProvider(env="practice", offline=False)
    if provider._offline:
        raise SystemExit("KIS_HTTP_BLOCKED: cannot certify original-date proof")
    reports = collect_evidence(rows, provider=provider)
    terminal = sum(report["terminal_proven"] for report in reports)
    summary = {
        "mode": "LIVE_KIS_GET_ONLY", "read_only": True,
        "claim_count": len(reports), "terminal_proven_count": terminal,
        "unresolved_or_open_count": len(reports) - terminal,
        "all_terminal_proven": bool(reports) and terminal == len(reports),
        "day_count": len(set(str(row["trade_date"]) for row in rows)),
        "database_mutated": False, "claims_released": 0,
    }
    print(json.dumps({"summary": summary, "claims": reports},
                     ensure_ascii=False, sort_keys=True, default=str))
    if args.require_all_terminal and not summary["all_terminal_proven"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
