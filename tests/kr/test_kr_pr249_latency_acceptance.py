"""Proof gate tests only: passing synthetic fixtures do NOT claim live p95."""
import pytest
from scripts.kr.pr249_latency_acceptance import evaluate_latency


def _records(after_tick, after_sell, *, before_tick=330, before_sell=110):
    rows = []
    for revision, tick, sell, sha in (
        ("before", before_tick, before_sell, "before-verified"),
        ("after", after_tick, after_sell, "after-verified"),
    ):
        for i in range(20):
            rows.append({
                "revision": revision, "workload_id": "30_final5_open",
                "tick_id": str(i), "git_sha": sha, "kind": "measured",
                "tick_elapsed_sec": tick + i / 1000,
                "first_protective_sell_submit_sec": sell + i / 1000,
                "result_status": "OK",
            })
    return rows


def test_measured_matching_workload_must_meet_both_p95_targets():
    outcome = evaluate_latency(_records(165, 48))
    assert outcome["result"] == "PASS"
    row = outcome["scenarios"][0]
    assert row["tick_limit_pass"] and row["sell_limit_pass"]


@pytest.mark.parametrize("after_tick,after_sell", [(185, 49), (165, 62), (340, 120)])
def test_unsupported_performance_fails_even_if_ci_succeeds(after_tick, after_sell):
    assert evaluate_latency(_records(after_tick, after_sell))["result"] == "FAIL"


@pytest.mark.parametrize("failure", ["PR249_LATENCY_INSUFFICIENT_TICKS",
                                      "PR249_LATENCY_INSUFFICIENT_SELL_PROOFS",
                                      "PR249_LATENCY_UNPROVEN_SAMPLE",
                                      "PR249_LATENCY_NO_SAMPLES"])
def test_missing_evidence_cannot_claim_speedup(failure):
    rows = _records(165, 48)
    if "INSUFFICIENT_TICKS" in failure:
        rows = rows[:1] + rows[20:]
    elif "INSUFFICIENT_SELL" in failure:
        for r in rows:
            r.pop("first_protective_sell_submit_sec")
    elif "UNPROVEN" in failure:
        rows[0]["kind"] = "synthetic"
    else:
        rows = []
    with pytest.raises(ValueError, match=failure):
        evaluate_latency(rows)


def test_fast_failed_tick_never_counts_as_performance_pass():
    rows = _records(165, 48)
    rows[-1]["result_status"] = "RECOVERABLE_DB_TIMEOUT"
    with pytest.raises(ValueError, match="PR249_LATENCY_FAILED_TICK_NOT_ACCEPTABLE"):
        evaluate_latency(rows)


def test_missing_result_status_never_counts_as_performance_pass():
    rows = _records(165, 48)
    rows[-1].pop("result_status")
    with pytest.raises(ValueError, match="PR249_LATENCY_FAILED_TICK_NOT_ACCEPTABLE"):
        evaluate_latency(rows)



def test_exact_pr_head_sha_must_match_measured_after_records():
    rows = _records(165, 48)
    for row in rows:
        if row["revision"] == "after":
            row["git_sha"] = "a" * 40
    assert evaluate_latency(rows, expected_after_sha="a" * 40)["result"] == "PASS"
    with pytest.raises(ValueError, match="PR249_LATENCY_AFTER_SHA_NOT_PR_HEAD"):
        evaluate_latency(rows, expected_after_sha="b" * 40)


def test_short_or_unverified_pr_head_is_not_an_acceptable_release_argument():
    with pytest.raises(ValueError, match="PR249_LATENCY_INVALID_EXPECTED_PR_HEAD"):
        evaluate_latency(_records(165, 48), expected_after_sha="abc123")
