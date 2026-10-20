"""PR243 20-sample 60s gate: no duplicates, no fabricated live proof."""
from pathlib import Path
from scripts.us_latency_compare import summarize,compare


def _write_samples(path: Path, *, n: int, delay: float, duplicate: bool = False):
    records=[]
    for i in range(n):
        records.append(
            "2026-10-20 23:00:00,000 INFO [US_EXIT][SIGNAL_TO_BROKER_BOUNDARY]"
            f" symbol=SIM{i:02d} attempt=attempt-{i}"
            " signal_id=signal-"
            f"{i} source=selected_intent_exact delay_ms={delay}\n"
        )
    if duplicate:
        records.extend(records[:15])
    path.write_text("".join(records), encoding="utf-8")


def test_20_unique_attempts_can_pass_log_only_gate(tmp_path):
    baseline=tmp_path/"baseline.log"
    candidate=tmp_path/"candidate.log"
    _write_samples(baseline,n=20,delay=80000)
    _write_samples(candidate,n=20,delay=40000)
    result=compare(summarize(baseline),summarize(candidate))[
        "exact_selected_sell_to_http_sec"
    ]
    assert result["comparison_valid"] is True
    assert result["baseline_p95_sec"] == 80.0
    assert result["candidate_p95_sec"] == 40.0


def test_repeated_log_copies_cannot_fake_twenty_samples(tmp_path):
    baseline=tmp_path/"baseline.log"
    candidate=tmp_path/"candidate.log"
    _write_samples(baseline,n=20,delay=80000)
    _write_samples(candidate,n=5,delay=40000,duplicate=True)
    cand=summarize(candidate)
    assert cand["distributions_seconds"]["exact_selected_sell_to_http_sec"]["count"] == 5
    assert cand["counters"]["sell_http_duplicate_attempts"] == 5
    assert compare(summarize(baseline),cand)[
        "exact_selected_sell_to_http_sec"
    ]["comparison_valid"] is False


def test_conflicting_latency_for_same_attempt_is_quarantined(tmp_path):
    log=tmp_path/"bad.log"
    _write_samples(log,n=20,delay=40000)
    with log.open("a", encoding="utf-8") as f:
        f.write("[US_EXIT][SIGNAL_TO_BROKER_BOUNDARY] symbol=SIM00"
                " attempt=attempt-0 source=selected_intent_exact delay_ms=95000\n")
    evidence=summarize(log)
    assert evidence["counters"]["sell_http_conflicting_duplicate_attempts"]==1
    assert compare(evidence,evidence)[
        "exact_selected_sell_to_http_sec"
    ]["comparison_valid"] is False


def test_missing_attempt_identity_cannot_be_counted(tmp_path):
    log=tmp_path/"missing.log"
    log.write_text(
        "[US_EXIT][SIGNAL_TO_BROKER_BOUNDARY]"
        " source=selected_intent_exact delay_ms=1\n", encoding="utf-8"
    )
    evidence=summarize(log)
    assert evidence["counters"]["sell_http_missing_attempt_id"]==1
    assert evidence["distributions_seconds"]["exact_selected_sell_to_http_sec"]["count"]==0
