import json
import pytest
from trader.us.runner.runtime_acceptance_evidence import summarize_runtime,compare_runtime,result_evidence


def rows(sha,seconds=50,positions=12,account="d"*64,orders=1,fills=2):
    for session in ("am","afternoon"):
        for i in range(20):
            scope=dict(trade_date="2026-11-25",session=session,run_id="run-"+session,
                       tick_id=str(i),code_sha=sha,native_practice=True,account_digest=account)
            data=dict(status="OK",runtime_integrity_status="OK",sell_liveness_status="OK",
                      entry_liveness_status="OK",reconcile_liveness_status="OK",
                      tick_total_ms=seconds*1000,entry_can_proceed=True,exit_can_proceed=True,
                      positions_evaluated=positions,orders_routed=orders,fills_api_count=fills,raw_watchlist_candidates=30,intent_generation_attempted=12)
            yield "[US400_RUNTIME][ATTEMPT] payload="+json.dumps(scope)
            yield "[US400_RUNTIME][RESULT] payload="+json.dumps(result_evidence(scope,data,wall_sec=seconds))


@pytest.mark.parametrize("problem",["none","slow","less_work","missing","duplicate_conflict","timeout","wrong_sha","degraded"])
def test_gate_never_counts_fast_incomplete_or_noncomparable_ticks(problem):
    before=list(rows("a"*40,seconds=150))
    after=list(rows("b"*40,seconds=61 if problem=="slow" else 50,
                    positions=2 if problem=="less_work" else 12))
    if problem=="missing": after.pop()
    if problem=="duplicate_conflict":
        row=json.loads(after[1].split("payload=",1)[1]); row["wall_sec"]=99
        after.append("[US400_RUNTIME][RESULT] payload="+json.dumps(row))
    if problem=="timeout":
        row=json.loads(after.pop().split("payload=",1)[1])
        after.append("[US400_RUNTIME][FAILURE] payload="+json.dumps(row))
    if problem=="wrong_sha": after[0]=after[0].replace("b"*40,"c"*40)
    if problem=="degraded":
        row=json.loads(after[1].split("payload=",1)[1]); row["entry_liveness_status"]="LIVENESS_DEGRADED"
        after[1]="[US400_RUNTIME][RESULT] payload="+json.dumps(row)
    report=compare_runtime(summarize_runtime(before,expected_sha="a"*40),
                           summarize_runtime(after+after,expected_sha="b"*40))
    assert (report["status"]=="PASS_LOG_TIMING_ONLY")== (problem=="none")
    assert report["operational_go"] is False and report["order_authorization"] is False
    assert report["candidate"]["sessions"]["am"]["attempted"]<=20

@pytest.mark.parametrize(("overrides", "blocker"), [
    ({"orders": 0}, "am:NONCOMPARABLE_WORKLOAD"),
    ({"fills": 0}, "afternoon:NONCOMPARABLE_WORKLOAD"),
    ({"seconds": 60.0004}, "am:MEAN_TICK_OVER_60_OR_UNKNOWN"),
    ({"account": "e" * 64}, "BASELINE_CANDIDATE_ACCOUNT_MISMATCH"),
    ({"account": None}, "ACCOUNT_SCOPE_UNVERIFIED"),
])
def test_comparison_requires_matching_account_and_order_workload(overrides, blocker):
    baseline = summarize_runtime(rows("a" * 40), expected_sha="a" * 40)
    candidate = summarize_runtime(rows("b" * 40, **overrides), expected_sha="b" * 40)
    report = compare_runtime(baseline, candidate)
    assert report["status"] == "NOT_PROVEN"
    assert blocker in report["blockers"]
