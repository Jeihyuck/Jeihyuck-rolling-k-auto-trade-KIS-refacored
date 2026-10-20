#!/usr/bin/env python3
"""Independent, read-only US KIS source-window diagnostic.

The production KIS date bridge excludes many received rows. Investigate whether
an old locally ACKed order is in the authenticated raw 2-day KST response but
was excluded by the NY-session filter. NEVER certify a fill, release a fence,
write DB/order journal, or show broker order/account numbers.
"""
from __future__ import annotations
from datetime import date
import argparse
import json
from sqlalchemy import text

from trader.us.db.repos import _get_engine_or_none, _us_execution_claim_scope
from trader.us.execution.kis_us_client import KisUSClient
from trader.us.data_provider import USDataProvider
from trader.us.execution.order_journal import load_order_events
from trader.us.utils.order_no import normalize_us_order_no
from scripts.us_kis_original_day_evidence import CLAIMS_SQL


class RawWindowDiagnosticClient(KisUSClient):
    """Diagnostic-only full KST window; never feeds the trade executor."""
    @classmethod
    def _filter_broker_rows_for_us_trade_date(cls, rows, trade_date):
        # Still GET-only and fully normalized. This bypass is EXCLUSIVELY to
        # find excluded order identity; returned rows are never fill proof.
        return list(rows)


def explain_source_window(row, journal, raw_rows, strict_rows, *, verified_scope):
    key = str(row.get("client_order_key") or "")
    attempt = str(row.get("attempt_id") or "")
    history = [
        e for e in journal if str(e.get("client_order_key") or "") == key
        and str(e.get("submit_attempt_id") or "") == attempt
    ]
    ack = next(
        (e for e in reversed(history)
         if e.get("event_type") in {"BROKER_ACK_RECEIVED","BROKER_ACK_RECOVERED"}
         and e.get("broker_order_no")), None,
    )
    symbol = str(row.get("symbol") or "").upper()
    side = str(row.get("side") or "").upper()
    day = str(row.get("trade_date") or "")
    base = {
        "day": day, "symbol": symbol, "side": side,
        "scope_verified": bool(verified_scope),
        "raw_window_rows": len(raw_rows),
        "strict_us_day_rows": len(strict_rows),
        "source_window_broker_number_found": False,
        "source_window_symbol_side_identity": False,
        "source_window_qty_match": False,
        "excluded_by_session_filter": False,
        "source_timestamp_us_date_matches": False,
        "raw_window_broker_status": None,
        "raw_window_broker_cumulative_qty": None,
        "actual_execution_proven": False,
        "fence_release_authorized": False,
        "database_mutated": False,
    }
    if not ack or not verified_scope:
        return {**base,"reason": "MISSING_ACK_OR_AUTHENTICATED_KIS_SCOPE"}
    num = normalize_us_order_no(ack["broker_order_no"])
    candidates = [
        r for r in raw_rows
        if normalize_us_order_no(r.get("order_no") or "") == num
    ]
    combined = [
        r for r in candidates
        if str(r.get("symbol") or "").upper() == symbol
        and str(r.get("side") or "").upper() == side
    ]
    from trader.us.execution.order_journal import _replay_order_row_matches
    # Not a fill proof: just the presence of an original ACK identity in
    # returned KIS rows, even when excluded as another ET session.
    strict_order_keys = {
        (normalize_us_order_no(r.get("order_no")), str(r.get("symbol") or "").upper(),
         str(r.get("side") or "").upper())
        for r in strict_rows
    }
    was_excluded = any(
        (normalize_us_order_no(r.get("order_no")), symbol, side)
        not in strict_order_keys for r in combined
    )
    date_matches = any(str(r.get("us_trade_date") or "") == day for r in combined)
    status = sorted({str(r.get("status") or "") for r in combined})
    cum_qty = sorted({str(r.get("filled_qty")) for r in combined})
    same_qty = any(str(r.get("requested_qty")) == str(row.get("requested_qty"))
                   for r in combined)
    return {
        **base,
        "source_window_broker_number_found": bool(candidates),
        "source_window_symbol_side_identity": bool(combined),
        "source_window_qty_match": same_qty,
        "excluded_by_session_filter": was_excluded,
        "source_timestamp_us_date_matches": date_matches,
        "raw_window_broker_status": status[:4],
        "raw_window_broker_cumulative_qty": cum_qty[:4],
        "reason": (
            "FOUND_EXCLUDED_BY_US_SESSION_BRIDGE" if was_excluded
            else "FOUND_ORIGINAL_ORDER_IN_SOURCE_WINDOW" if combined
            else "BROKER_NUMBER_MATCH_BUT_SYMBOL_SIDE_MISMATCH" if candidates
            else "NO_ORIGINAL_ACK_ORDER_IN_SOURCE_WINDOW"
        ),
    }


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-date", required=True)
    ap.add_argument("--to-date", required=True)
    args = ap.parse_args(argv)
    start,end = date.fromisoformat(args.from_date),date.fromisoformat(args.to_date)
    if end < start:
        raise SystemExit("INVALID_DATE_RANGE")
    scope=_us_execution_claim_scope()
    if scope.get("env")!="practice" or scope.get("market")!="US" or not all(
        scope.get(k) for k in ("env","account_id","trading_epoch_id")):
        raise SystemExit("INVALID_ACCOUNT_SCOPE")
    engine=_get_engine_or_none()
    if engine is None:
        raise SystemExit("DATABASE_UNAVAILABLE")
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text("SET TRANSACTION READ ONLY"))
            claims=conn.execute(text(CLAIMS_SQL),
                {**scope,"start_date":start,"end_date":end}).mappings().all()
    if not claims:
        raise SystemExit("NO_ACTIVE_CLAIMS_CANNOT_CERTIFY")
    strict=USDataProvider(env="practice",offline=False)
    diag=USDataProvider(env="practice",offline=False)
    if strict._offline or diag._offline:
        raise SystemExit("KIS_HTTP_BLOCKED")
    diag._client=RawWindowDiagnosticClient(env="practice",offline=False)
    snapshots={}
    result=[]
    for claim in claims:
        day=str(claim["trade_date"])
        if day not in snapshots:
            # Source book and nonfiltered diagnostics remain separate.
            rows=strict.get_today_orders(day)
            raw=diag.get_today_orders(day)
            evidence_scope=strict.get_order_lookup_provenance(day)
            diag_scope=diag.get_order_lookup_provenance(day)
            verified=bool(
                evidence_scope.get("verified") is True
                and diag_scope.get("verified") is True
                and evidence_scope.get("account_id")==diag_scope.get("account_id")
                and evidence_scope.get("account_id")==scope.get("account_id")
                and evidence_scope.get("query_trade_date")==day
                and diag_scope.get("query_trade_date")==day)
            snapshots[day]=(rows,raw,verified,load_order_events(day))
        rows,raw,verified,events=snapshots[day]
        result.append(explain_source_window(dict(claim),events,raw,rows,
                                            verified_scope=verified))
    print(json.dumps({"mode":"DIAGNOSTIC_ONLY","database_mutated":False,
        "claims_released":0,
        "claim_count":len(result),
        "excluded_with_same_original_order":sum(x["excluded_by_session_filter"] for x in result),
        "source_window_order_number_found":sum(x["source_window_broker_number_found"] for x in result),
        "results":result},ensure_ascii=False,sort_keys=True))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
