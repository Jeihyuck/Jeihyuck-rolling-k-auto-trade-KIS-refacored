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


@pytest.mark.parametrize("case", [
    "clean", "ack", "unknown", "missing", "failure", "kind_conflict", "buy", "wrong_account",
    "missing_exit_proof", "missing_manual_proof", "manual_debt",
])
def test_protective_wakeup_is_separate_and_cannot_pad_or_hide_normal_work(case):
    before=list(rows("a"*40))
    after=list(rows("b"*40))
    scope=dict(trade_date="2026-11-25",session="am",run_id="run-am",
               tick_id="protective-1",code_sha="b"*40,native_practice=True,
               account_digest="d"*64,tick_kind="protective_wakeup")
    after.append("[US400_RUNTIME][ATTEMPT] payload="+json.dumps(scope))
    data=dict(status="OK_EXIT_ORDERS_SENT" if case=="ack" else "OK_NO_TRADE",
              reason="protective_wakeup_routed" if case=="ack" else "protective_wakeup_no_fresh_signal",
              protective_wakeup=True,entry_can_proceed=False,exit_can_proceed=True,
              entry_intents=0,buy_orders=1 if case=="buy" else 0,
              exit_intents=1 if case=="ack" else 0,orders_sent=1 if case=="ack" else 0,
              orders_ack=1 if case=="ack" else 0,exit_route_reconcile_required=case=="unknown",
              manual_reconcile_required=int(case=="manual_debt"))
    if case=="missing_exit_proof": data.pop("exit_route_reconcile_required")
    if case=="missing_manual_proof": data.pop("manual_reconcile_required")
    if case=="kind_conflict": scope=dict(scope,tick_kind="normal")
    if case=="wrong_account": scope=dict(scope,account_digest="e"*64)
    if case=="failure": after.append("[US400_RUNTIME][FAILURE] payload="+json.dumps(scope))
    elif case!="missing": after.append("[US400_RUNTIME][RESULT] payload="+json.dumps(result_evidence(scope,data,wall_sec=2)))
    report=compare_runtime(summarize_runtime(before,expected_sha="a"*40),
                           summarize_runtime(after,expected_sha="b"*40))
    assert (report["status"]=="PASS_LOG_TIMING_ONLY")== (case in {"clean","ack"})
    am=report["candidate"]["sessions"]["am"]
    assert am["attempted"]==am["normal"]==20
    assert am["total_attempted"]==21 and am["protective_wakeup_attempted"]==1
    assert report["operational_go"] is False


@pytest.mark.parametrize(("seconds", "optimized"), [(30, True), (30.001, False), (60, False)])
def test_30_second_target_requires_full_comparable_normal_work(seconds, optimized):
    baseline = summarize_runtime(rows("a"*40, seconds=150), expected_sha="a"*40)
    candidate = summarize_runtime(rows("b"*40, seconds=seconds), expected_sha="b"*40)
    report = compare_runtime(baseline, candidate)
    assert report["status"] == "PASS_LOG_TIMING_ONLY"
    assert (report["optimization_status"] == "PASS_LOG_TIMING_ONLY") is optimized
    assert ("am:MEAN_TICK_OVER_30_OR_UNKNOWN" in report["optimization_blockers"]) is not optimized

    mismatched = summarize_runtime(rows("b"*40, seconds=seconds, positions=1), expected_sha="b"*40)
    report = compare_runtime(baseline, mismatched)
    assert report["optimization_status"] == "NOT_PROVEN"
    assert "am:NONCOMPARABLE_WORKLOAD" in report["optimization_blockers"]


@pytest.mark.parametrize("case", ["same", "missing", "different_workload"])
def test_policy_blocked_population_cannot_disappear_from_timing_comparison(case):
    def with_policy(sha, positions):
        lines = list(rows(sha))
        for session in ("am", "afternoon"):
            scope = dict(trade_date="2026-11-25", session=session, run_id="run-"+session,
                         tick_id="policy-1", code_sha=sha, native_practice=True,
                         account_digest="d"*64)
            data = dict(status="OK", runtime_integrity_status="POLICY_ENTRY_BLOCKED",
                        sell_liveness_status="OK", entry_liveness_status="OK",
                        reconcile_liveness_status="OK", tick_total_ms=50000,
                        entry_can_proceed=False, exit_can_proceed=True,
                        positions_evaluated=positions, raw_watchlist_candidates=30,
                        intent_generation_attempted=0, orders_routed=1, fills_api_count=2)
            lines.append("[US400_RUNTIME][ATTEMPT] payload="+json.dumps(scope))
            lines.append("[US400_RUNTIME][RESULT] payload="+json.dumps(result_evidence(scope,data,wall_sec=50)))
        return lines
    baseline=summarize_runtime(with_policy("a"*40,12),expected_sha="a"*40)
    candidate=summarize_runtime(list(rows("b"*40)) if case=="missing" else
                                with_policy("b"*40,2 if case=="different_workload" else 12),
                                expected_sha="b"*40)
    report=compare_runtime(baseline,candidate)
    assert baseline["sessions"]["am"]["policy_blocked"]==1
    assert (report["status"]=="PASS_LOG_TIMING_ONLY")== (case=="same")
    assert ("am:NONCOMPARABLE_POLICY_WORKLOAD" in report["blockers"]) == (case!="same")
    assert report["operational_go"] is False


@pytest.mark.parametrize("overrides", [
    {"exit_can_proceed": False},
    {"exit_can_proceed": None},
    {"entry_can_proceed": True},
    {"entry_can_proceed": None},
    {"status": "DEGRADED_NO_TRADE"},
    {"status": "DEGRADED_FILLS_UNAVAILABLE"},
])
def test_policy_entry_block_never_masks_degraded_or_unverified_exit(overrides):
    def population(sha):
        lines = list(rows(sha))
        scope = dict(trade_date="2026-11-25", session="am", run_id="run-am",
                     tick_id="policy-1", code_sha=sha, native_practice=True,
                     account_digest="d"*64)
        data = dict(status="OK", runtime_integrity_status="POLICY_ENTRY_BLOCKED",
                    sell_liveness_status="OK", entry_liveness_status="OK",
                    reconcile_liveness_status="OK", tick_total_ms=50000,
                    entry_can_proceed=False, exit_can_proceed=True,
                    positions_evaluated=12, raw_watchlist_candidates=30,
                    intent_generation_attempted=0, orders_routed=1, fills_api_count=2)
        data.update(overrides)
        lines.append("[US400_RUNTIME][ATTEMPT] payload="+json.dumps(scope))
        lines.append("[US400_RUNTIME][RESULT] payload="+json.dumps(
            result_evidence(scope, data, wall_sec=50)))
        return summarize_runtime(lines, expected_sha=sha)

    baseline = population("a"*40)
    candidate = population("b"*40)
    report = compare_runtime(baseline, candidate)
    assert report["status"] == "NOT_PROVEN"
    assert "DEGRADED_OR_FAILED_ATTEMPTS" in report["blockers"]
    assert candidate["sessions"]["am"]["policy_blocked"] == 0
    assert candidate["sessions"]["am"]["degraded"] == 1
