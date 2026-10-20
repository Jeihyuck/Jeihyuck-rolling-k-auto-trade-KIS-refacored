#!/usr/bin/env python3
"""One-time, evidence-gated KIS practice SELL settlement repair.

GET-only by default. --apply authorizes DB order/fill/claim reconciliation for
seven exactly scoped verified US_STANDARD full SELL fills, and never invokes
any order submit/cancel API. --post-verify audits DB after a process restart.
All runtime output is redacted of client_order_key, account and KIS order number.
"""
from __future__ import annotations
import argparse
from datetime import date
import json
import math
import os
from sqlalchemy import text
from trader.us.db.repos import _get_engine_or_none, _us_execution_claim_scope
from trader.us.data_provider import USDataProvider
from trader.us.execution.order_journal import (
    load_order_events, _replay_order_row_matches, _replay_detail_identity_conflicts,
)
from trader.us.utils.order_no import normalize_us_order_no
from scripts.us_kis_original_day_evidence import CLAIMS_SQL, verify_claim

# Exact historically observed source-day set: never broaden to unrelated actions.
EXPECTED = {
    ("2026-10-06", "AMD"): (1, False),
    ("2026-10-06", "MRVL"): (2, False),
    ("2026-10-12", "MSFT"): (1, False),
    ("2026-10-13", "MCK"): (1, True),
    ("2026-10-14", "ABBV"): (2, True),
    ("2026-10-14", "COST"): (1, True),
    ("2026-10-14", "GILD"): (4, True),
    ("2026-10-14", "NET"): (1, True),
    ("2026-10-14", "PG"): (4, True),
    ("2026-10-15", "CL"): (6, False),
    ("2026-10-15", "DHR"): (2, True),
    ("2026-10-15", "KO"): (7, False),
    ("2026-10-15", "KR"): (10, False),
}
EXPECTED_FULL = sum(1 for qty, filled in EXPECTED.values() if filled)
START, END = date(2026,10,1), date(2026,10,20)


def _verified_environment():
    for k, want in (("KIS_ENV", "practice"), ("TRADING_REGION","US"),
                    ("DRY_RUN", "1"), ("US_KIS_ORDER_ALLOWED", "0"),
                    ("US_ORDER_ARMED", "0"), ("DISABLE_REAL_TRADING", "1")):
        if os.getenv(k) != want:
            raise RuntimeError(f"UNSAFE_ENV_{k}")
    if os.getenv("KIS_REST_URL") != "https://openapivts.koreainvestment.com:29443":
        raise RuntimeError("NON_PRACTICE_ENDPOINT")
    scope = _us_execution_claim_scope()
    if (scope.get("env") != "practice" or scope.get("market") != "US"
        or not scope.get("account_id") or not scope.get("trading_epoch_id")):
        raise RuntimeError("UNVERIFIED_DB_SCOPE")
    engine = _get_engine_or_none()
    if engine is None:
        raise RuntimeError("DB_MISSING")
    return scope,engine


def _active_claims(engine,scope):
    sql = CLAIMS_SQL.replace("SELECT c.trade_date,", "SELECT c.action_key, c.trade_date,")
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text("SET TRANSACTION READ ONLY"))
            return [dict(r) for r in conn.execute(text(sql),
                {**scope, "start_date": START, "end_date": END}).mappings().all()]


def _claim_metas(engine, scope, rows):
    out={}
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text("SET TRANSACTION READ ONLY"))
            for row in rows:
                metadata=conn.execute(text("""
                    SELECT o.meta, o.order_no, o.status, o.qty_filled
                    FROM us_orders o
                    WHERE o.env=:env AND o.trading_epoch_id=:trading_epoch_id
                      AND o.trade_date=:trade_date
                      AND o.client_order_key=:client_order_key
                """),{**scope,"trade_date":row["trade_date"],
                     "client_order_key":row["client_order_key"]}).mappings().all()
                if len(metadata)!=1:
                    raise RuntimeError("LOCAL_ORDER_IDENTITY_NOT_UNIQUE")
                out[str(row["action_key"])]=dict(metadata[0])
    return out


def preflight(provider, engine, scope):
    rows=_active_claims(engine,scope)
    by_key={(str(r["trade_date"]),str(r.get("symbol") or "").upper()):r for r in rows}
    if len(rows)!=13 or set(by_key)!=set(EXPECTED):
        raise RuntimeError("HISTORICAL_ACTIVE_CLAIM_SET_CHANGED")
    stored=_claim_metas(engine,scope,rows)
    per_day={}
    fully_verified=[]
    summary=[]
    for (day,symbol), row in sorted(by_key.items()):
        qty, expected_fill=EXPECTED[(day,symbol)]
        if (row.get("strategy_owner")!="US_STANDARD" or row.get("side")!="SELL"
            or int(row.get("requested_qty") or -1)!=qty):
            raise RuntimeError(f"OWNER_OR_ACTION_MISMATCH_{symbol}")
        data=stored[str(row["action_key"])]
        meta=data.get("meta") or {}
        if isinstance(meta,str): meta=json.loads(meta)
        if (str(meta.get("execution_action_key") or "")!=str(row["action_key"])
            or str(meta.get("submit_attempt_id") or "")!=str(row["attempt_id"])
            or str(meta.get("strategy_owner") or "")!="US_STANDARD"
            or normalize_us_order_no(data.get("order_no"))==""):
            raise RuntimeError(f"LOCAL_CLAIM_META_CONFLICT_{symbol}")
        if day not in per_day:
            broker=provider.get_today_orders(day)
            provenance=provider.get_order_lookup_provenance(day)
            if not (
                provenance.get("verified") is True
                and provenance.get("source")=="kis_authenticated_order_book"
                and provenance.get("market")=="US"
                and provenance.get("env")=="practice"
                and str(provenance.get("account_id"))==str(scope["account_id"])
                and str(provenance.get("query_trade_date")).replace("-","")==day.replace("-","")
            ):
                raise RuntimeError(f"KIS_ACCOUNT_SCOPE_UNVERIFIED_{day}")
            per_day[day]=(broker,provenance,load_order_events(day))
        broker,provenance,events=per_day[day]
        proof=verify_claim(row,events,broker,provenance)
        if expected_fill and (
            proof["proof_status"]!="VERIFIED_FULL_FILL"
            or proof["actual_filled_qty"]!=qty
            or not (math.isfinite(float(proof.get("actual_avg_price") or 0))
                    and float(proof["actual_avg_price"])>0)
        ):
            raise RuntimeError(f"FULL_FILL_NOT_REPRODUCED_{symbol}_{proof['reason']}")
        if not expected_fill and (
            proof["proof_status"]!="BROKER_ORDER_STILL_ACTIVE"
            or proof.get("actual_filled_qty")!=0
        ):
            raise RuntimeError(f"UNFILLED_ACTION_STATE_CHANGED_{symbol}_{proof['reason']}")
        report={"date":day,"symbol":symbol,"qty":qty,
                "proof":proof["proof_status"],"price":proof.get("actual_avg_price")}
        summary.append(report)
        if not expected_fill:
            continue
        key=str(row["client_order_key"])
        attempt=str(row["attempt_id"])
        submit=next((e for e in reversed(events)
            if e.get("client_order_key")==key and e.get("submit_attempt_id")==attempt
            and e.get("event_type")=="BROKER_SUBMIT_STARTED"),None)
        ack=next((e for e in reversed(events)
            if e.get("client_order_key")==key and e.get("submit_attempt_id")==attempt
            and e.get("event_type") in {"BROKER_ACK_RECEIVED","BROKER_ACK_RECOVERED"}
            and e.get("broker_order_no")),None)
        if not submit or not ack:
            raise RuntimeError(f"SUBMIT_ACK_MISSING_{symbol}")
        order_no=str(ack["broker_order_no"])
        matched=[r for r in broker if _replay_order_row_matches(
            r,order_no=order_no,symbol=symbol,side="SELL",requested_qty=qty,
            source_trade_date=day,journal_event=submit,lookup_context=provenance)
            and not _replay_detail_identity_conflicts(
            r,order_no=order_no,symbol=symbol,side="SELL",requested_qty=qty,
            source_trade_date=day,journal_event=submit,lookup_context=provenance)]
        if len(matched)!=1:
            raise RuntimeError(f"BROKER_EXACT_ROW_NOT_UNIQUE_{symbol}")
        source=matched[0]
        if (str(source.get("status") or "")!="FILLED"
            or int(source.get("filled_qty") or -1)!=qty
            or float(source.get("avg_price") or 0)!=float(proof["actual_avg_price"])
            or not source.get("filled_qty_present",True)):
            raise RuntimeError(f"FILL_SOURCE_CONTRADICTION_{symbol}")
        fully_verified.append((row,order_no,source))
    if len(fully_verified)!=EXPECTED_FULL:
        raise RuntimeError("NOT_EXACTLY_SEVEN_VERIFIED_FILLS")
    return summary,fully_verified


def verify_after(engine,scope,expected_terminal):
    # Used both after same-process mutation and after a fresh Python process.
    sql=text("""
        SELECT c.trade_date, c.strategy_owner,c.action_state,c.active_attempt_id,
               a.authoritative,a.attempt_state,
               o.symbol,o.side,o.status,o.qty_filled,o.avg_price_usd,
               COUNT(f.id) FILTER (
                 WHERE f.meta->>'fill_evidence_type'='KIS_ORDER_CUMULATIVE_ACTUAL'
                 AND COALESCE((f.meta->>'is_synthetic')::boolean,false)=false
                 AND COALESCE((f.meta->>'accounting_active')::boolean,true)=true
               ) AS actual_fills
        FROM us_execution_claims c JOIN us_execution_attempts a ON a.action_key=c.action_key
        JOIN us_orders o ON o.env=c.env AND o.trading_epoch_id=c.trading_epoch_id
          AND o.trade_date=c.trade_date AND o.client_order_key=a.client_order_key
        LEFT JOIN us_fills f ON f.client_order_key=o.client_order_key
          AND f.trade_date=o.trade_date AND f.trading_epoch_id=o.trading_epoch_id
        WHERE c.env=:env AND c.account_id=:account_id AND c.market=:market
          AND c.trading_epoch_id=:trading_epoch_id AND c.trade_date BETWEEN :start AND :end
        GROUP BY c.action_key,c.trade_date,c.strategy_owner,c.action_state,
          c.active_attempt_id,a.authoritative,a.attempt_state,o.symbol,o.side,
          o.status,o.qty_filled,o.avg_price_usd
    """)
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text("SET TRANSACTION READ ONLY"))
            records=[dict(x) for x in conn.execute(sql,{
                **scope,"start":START,"end":END}).mappings().all()]
    # Multiple historic attempts per action are possible; only the attempted
    # current original client keys may be compared. The accepted input set is
    # immutable by date/symbol and all other rows fail closed.
    bysymbol={}
    for r in records:
        key=(str(r["trade_date"]),str(r.get("symbol") or "").upper())
        if key in EXPECTED and key not in bysymbol:
            bysymbol[key]=r
    if len(bysymbol)!=13:
        raise RuntimeError("POST_REPAIR_ACTION_SET_INCOMPLETE")
    result=[]
    for key,(qty,should_fill) in sorted(EXPECTED.items()):
        row=bysymbol[key]
        if should_fill and expected_terminal:
            if (row["action_state"]!="SATISFIED"
                or row.get("active_attempt_id") is not None
                or not row.get("authoritative")
                or int(row.get("qty_filled") or -1)!=qty
                or row.get("status")!="FILLED"
                or int(row.get("actual_fills") or 0)!=1):
                raise RuntimeError(f"POST_REPAIR_NOT_DURABLE_{key[1]}")
        if not should_fill:
            if row.get("active_attempt_id") is None or int(row.get("qty_filled") or 0)!=0:
                raise RuntimeError(f"UNFILLED_FENCE_CHANGED_{key[1]}")
        result.append({"date":key[0],"symbol":key[1],"action_state":row["action_state"],
                       "actual_fills":int(row.get("actual_fills") or 0)})
    return result


def main(argv=None):
    ap=argparse.ArgumentParser()
    ap.add_argument("--apply",action="store_true")
    ap.add_argument("--post-verify",action="store_true")
    a=ap.parse_args(argv)
    scope,engine=_verified_environment()
    if a.post_verify:
        report=verify_after(engine,scope,True)
        print(json.dumps({"mode":"POST_RESTART_READ_ONLY","verified_actions":report}))
        return 0
    provider=USDataProvider(env="practice",offline=False)
    if provider._offline:raise RuntimeError("KIS_HTTP_UNAVAILABLE")
    proof,verified=preflight(provider,engine,scope)
    print(json.dumps({"mode":"PRE_APPLY_KIS_EVIDENCE","verified_count":len(verified),
                      "actions":proof},sort_keys=True),flush=True)
    if not a.apply:
        print(json.dumps({"mode":"AUDIT_ONLY","database_mutated":False}))
        return 0
    from trader.us.db.repos import apply_broker_order_observation
    for row,order_no,broker in verified:
        day=str(row["trade_date"])
        qty=int(row["requested_qty"])
        result=apply_broker_order_observation(
            trade_date=day,client_order_key=str(row["client_order_key"]),
            raw_order_no=order_no,canonical_order_no=normalize_us_order_no(order_no),
            symbol=str(row["symbol"]).upper(),side="SELL",
            requested_qty=qty,filled_qty=qty,remaining_qty=0,
            broker_status="FILLED",evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
            observed_at=broker.get("submitted_at_utc"),
            raw_row=broker,
        )
        if (not isinstance(result,dict) or result.get("status")!="OK"
            or not result.get("authoritative")
            or result.get("requires_reconcile") or result.get("observation_ignored")):
            # Partial prior rows remain fenced on failure; do not write any
            # compensating fake fills. Idempotency is retried after requery.
            raise RuntimeError("RECONCILE_NOT_AUTHORITATIVE_"+str(row["symbol"]))
        print(json.dumps({"mode":"APPLIED_VERIFIED_BROKER_FILL",
            "date":day,"symbol":row["symbol"],"qty":qty,
            "avg_price":broker["avg_price"]}),flush=True)
    report=verify_after(engine,scope,True)
    print(json.dumps({"mode":"APPLY_DONE","actual_fills_verified":EXPECTED_FULL,
        "statuses":report}),flush=True)
    return 0


if __name__=="__main__":
    raise SystemExit(main())
