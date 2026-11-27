"""Strict read-only performance evidence. Never authorizes trading or merge."""
from __future__ import annotations
from collections import Counter
import json
import math
import re
from statistics import mean

_MARKERS={"ATTEMPT":"[US400_RUNTIME][ATTEMPT] payload=",
          "RESULT":"[US400_RUNTIME][RESULT] payload=",
          "FAILURE":"[US400_RUNTIME][FAILURE] payload="}
_WORKLOAD = ("positions_evaluated", "raw_watchlist_candidates", "intent_generation_attempted",
             "orders_routed", "fills_api_count")
_NORMAL={"OK","OK_ORDERS_SENT","OK_ENTRY_ORDERS_SENT","OK_EXIT_ORDERS_SENT",
         "OK_EXIT_POSITION_CLOSED","OK_RECONCILED","NO_ENTRY_INTENTS","OK_NO_TRADE"}


def finite(value):
    return (isinstance(value,(int,float)) and not isinstance(value,bool)
            and math.isfinite(value) and value>=0)


def result_evidence(scope,result,*,wall_sec):
    from trader.us.runner.status_contract import classify_tick_status
    # Null/unknown stays unknown. Do not copy order payloads, account data,
    # credentials or broker responses into the evidence stream.
    out=dict(scope)
    out.update(wall_sec=wall_sec if finite(wall_sec) else None,
               status=str(result.get("status") or "UNKNOWN"),
               classification=classify_tick_status(result),
               runtime_integrity_status=str(result.get("runtime_integrity_status") or "UNKNOWN"),
               sell_liveness_status=str(result.get("sell_liveness_status") or "UNKNOWN"),
               entry_liveness_status=str(result.get("entry_liveness_status") or "UNKNOWN"),
               reconcile_liveness_status=str(result.get("reconcile_liveness_status") or "UNKNOWN"),
               tick_total_ms=result.get("tick_total_ms") if finite(result.get("tick_total_ms")) else None,
               entry_can_proceed=result.get("entry_can_proceed") if isinstance(result.get("entry_can_proceed"),bool) else None,
               exit_can_proceed=result.get("exit_can_proceed") if isinstance(result.get("exit_can_proceed"),bool) else None)
    for key in _WORKLOAD:
        v=result.get(key)
        out[key]=v if isinstance(v,int) and not isinstance(v,bool) and v>=0 else None
    if scope.get("tick_kind")=="protective_wakeup":
        out["protective_wakeup"]=result.get("protective_wakeup") is True
        out["protective_wakeup_kind"]=result.get("protective_wakeup_kind","stop")
        out["reason"]=str(result.get("reason") or "UNKNOWN")
        out["exit_route_reconcile_required"]=bool(result.get("exit_route_reconcile_required"))
        out["manual_reconcile_required"]=bool(result.get("manual_reconcile_required"))
        for key in ("entry_intents","exit_intents","buy_orders","orders_sent","orders_ack"):
            v=result.get(key)
            out[key]=v if type(v) is int and v>=0 else None
    return out


def summarize_runtime(lines,*,expected_sha):
    records={k:{} for k in _MARKERS}
    account_digests = set()
    malformed=conflicts=0
    for line in lines:
        for kind,marker in _MARKERS.items():
            if marker not in line:
                continue
            try:
                row=json.loads(line.split(marker,1)[1])
                key=tuple(row[k] for k in ("trade_date","session","run_id","tick_id"))
                if not all(isinstance(x,str) and x.strip() for x in key):
                    raise ValueError("missing_identity")
                if row.get("code_sha")!=expected_sha or row.get("native_practice") is not True:
                    raise ValueError("unverified_scope")
                if row.get("tick_kind","normal") not in {"normal","protective_wakeup"}:
                    raise ValueError("unverified_tick_kind")
                digest = row.get("account_digest")
                if not isinstance(digest, str) or not re.fullmatch("[0-9a-f]{64}", digest):
                    raise ValueError("unverified_account")
                account_digests.add(digest)
                prior=records[kind].get(key)
                if prior is not None and prior!=row:
                    conflicts+=1
                else:
                    records[kind][key]=row
            except (ValueError,TypeError,KeyError,AttributeError):
                malformed+=1
            break
    started=records["ATTEMPT"]
    ended=records["RESULT"]
    failed=records["FAILURE"]
    # A result with no start, or an exception following a result, is not a
    # clean complete sample. Duplicate identical log copies never pad counts.
    missing=set(started)-set(ended)-set(failed)
    orphan=(set(ended)|set(failed))-set(started)
    ambiguous=set(ended)&set(failed)
    for key in set(started)&(set(ended)|set(failed)):
        for rows in (ended,failed):
            if key in rows and any(started[key].get(field,default)!=rows[key].get(field,default)
                                  for field,default in (("tick_kind","normal"),("account_digest",None))):
                conflicts+=1
    sessions={}
    for session in ("am","afternoon"):
        all_keys={k for k in started if k[1]==session}
        wake_keys={k for k in all_keys if started[k].get("tick_kind","normal")=="protective_wakeup"}
        keys=all_keys-wake_keys
        wake_ok=wake_uncertain=0
        for key in wake_keys & set(ended)-set(failed):
            row=ended[key]
            common=(row.get("protective_wakeup") is True and finite(row.get("wall_sec"))
                    and row.get("entry_can_proceed") is False and row.get("exit_can_proceed") is True
                    and row.get("entry_intents")==row.get("buy_orders")==0
                    and row.get("classification")=="success"
                    and not row.get("exit_route_reconcile_required")
                    and not row.get("manual_reconcile_required"))
            if common and (
                (row.get("status")=="OK_NO_TRADE" and row.get("orders_sent")==row.get("orders_ack")==row.get("exit_intents")==0
                 and row.get("reason") in {"protective_wakeup_no_fresh_signal","protective_wakeup_holding_changed"})
                or (row.get("status")=="OK_EXIT_ORDERS_SENT" and row.get("orders_sent")==row.get("orders_ack")==row.get("exit_intents")==1
                    and row.get("reason")=="protective_wakeup_routed")
                or (row.get("protective_wakeup_kind")=="profit"
                    and row.get("status")=="OK_NO_TRADE"
                    and row.get("orders_sent")==row.get("orders_ack")==row.get("exit_intents")==0
                    and row.get("reason")=="protective_profit_wakeup_no_trade")
                or (row.get("protective_wakeup_kind")=="profit"
                    and row.get("status")=="OK_EXIT_ORDERS_SENT"
                    and type(row.get("orders_ack")) is int and row["orders_ack"]>0
                    and row.get("orders_sent")==row["orders_ack"]==row.get("exit_intents")
                    and row.get("reason")=="protective_profit_wakeup_routed")):
                wake_ok+=1
            else:
                wake_uncertain+=1
        values=[]
        workload=Counter()
        policy=degraded=unknown=0
        for key in sorted(keys & set(ended)-set(failed)):
            row=ended[key]
            if (not finite(row.get("wall_sec")) or not finite(row.get("tick_total_ms"))
                    or any(not isinstance(row.get(f),int) or isinstance(row.get(f),bool)
                           or row[f]<0 for f in _WORKLOAD)
                    or row["wall_sec"]+.05<row["tick_total_ms"]/1000):
                unknown+=1
                continue
            if (row.get("classification")=="success" and row.get("runtime_integrity_status")=="POLICY_ENTRY_BLOCKED"
                    and row.get("sell_liveness_status")=="OK" and row.get("entry_liveness_status")=="OK"
                    and row.get("reconcile_liveness_status") in {"OK","PENDING_SETTLEMENT"}):
                policy+=1
                continue
            if (row.get("status") not in _NORMAL or row.get("classification")!="success"
                    or row.get("runtime_integrity_status")!="OK"
                    or row.get("sell_liveness_status")!="OK" or row.get("entry_liveness_status")!="OK"
                    or row.get("reconcile_liveness_status") not in {"OK","PENDING_SETTLEMENT"}
                    or row.get("entry_can_proceed") is not True or row.get("exit_can_proceed") is not True):
                degraded+=1
                continue
            values.append(row["wall_sec"])
            workload[tuple(row[f] for f in _WORKLOAD)]+=1
        values.sort()
        def percentile(p):
            return round(values[math.ceil(p*len(values))-1],3) if values else None
        sessions[session]={"attempted":len(keys),"total_attempted":len(all_keys),
                          "protective_wakeup_attempted":len(wake_keys),"protective_wakeup_clean":wake_ok,
                          "protective_wakeup_uncertain":wake_uncertain,
                          "protective_wakeup_failed":len(wake_keys & set(failed)),
                          "protective_wakeup_missing":len(wake_keys & missing),
                          "normal":len(values),"policy_blocked":policy,
                          "degraded":degraded,"unknown":unknown,"failed":len(keys & set(failed)),
                          "missing_result":len(keys & missing),"mean_sec":mean(values) if values else None,
                          "p50_sec":percentile(.5),"p95_sec":percentile(.95),"max_sec":max(values) if values else None,
                          "workload":{"/".join(map(str,k)):v for k,v in sorted(workload.items())}}
    blockers=[]
    if not re.fullmatch("[0-9a-f]{40}",str(expected_sha)): blockers.append("EXPECTED_SHA_INVALID")
    if malformed or conflicts or orphan or ambiguous: blockers.append("LOG_SCOPE_OR_IDENTITY_CONFLICT")
    if any(k[1] not in {"am","afternoon"} for k in started): blockers.append("UNSUPPORTED_SESSION")
    if len(account_digests) != 1:
        blockers.append("ACCOUNT_SCOPE_UNVERIFIED")
    if missing: blockers.append("INCOMPLETE_ATTEMPT_POPULATION")
    if any(r["degraded"] or r["unknown"] or r["failed"] for r in sessions.values()):
        blockers.append("DEGRADED_OR_FAILED_ATTEMPTS")
    if any(r["protective_wakeup_uncertain"] or r["protective_wakeup_failed"] or r["protective_wakeup_missing"] for r in sessions.values()):
        blockers.append("PROTECTIVE_WAKEUP_UNVERIFIED")
    return {"account_digest": next(iter(account_digests)) if len(account_digests) == 1 else None,
            "expected_sha":expected_sha,"sessions":sessions,"malformed":malformed,
            "conflicts":conflicts,"orphan":len(orphan),"ambiguous":len(ambiguous),"blockers":blockers}


def compare_runtime(baseline,candidate,*,min_samples=20):
    blockers=list(baseline["blockers"])+list(candidate["blockers"])
    if baseline["expected_sha"]==candidate["expected_sha"]:
        blockers.append("BASELINE_CANDIDATE_SHA_IDENTICAL")
    if baseline["account_digest"] != candidate["account_digest"]:
        blockers.append("BASELINE_CANDIDATE_ACCOUNT_MISMATCH")
    for session in ("am","afternoon"):
        b,c=baseline["sessions"][session],candidate["sessions"][session]
        if b["normal"]<min_samples or c["normal"]<min_samples:
            blockers.append(session+":INSUFFICIENT_NORMAL_SAMPLES")
        # Strict exact workload strata and counts. A fast run with fewer
        # candidates or a missing protective lane is not a speedup.
        if b["workload"]!=c["workload"]:
            blockers.append(session+":NONCOMPARABLE_WORKLOAD")
        if c["mean_sec"] is None or c["mean_sec"]>60:
            blockers.append(session+":MEAN_TICK_OVER_60_OR_UNKNOWN")
    return {"status":"PASS_LOG_TIMING_ONLY" if not blockers else "NOT_PROVEN",
            "blockers":sorted(set(blockers)),"baseline":baseline,"candidate":candidate,
            "native_kis_ack_fill_verified":False,"operational_go":False,
            "order_authorization":False,"integrated_implementation_complete":False}
