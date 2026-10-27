"""Stage4 READY gap must be proven from real WSL log; no simulated PASS."""
from scripts.us_stage4_latency_acceptance import (
    evaluate_stage4_ready_intervals,
)


def _samples(ms=15000., n=20):
    return [
        f"2026-10-28 INFO [US_STAGE4][READY_INTERVAL] interval_ms={ms} "
        "count=20 p50_ms=15000 p95_ms=15000 p99_ms=15000 max_ms=15000"
        for _ in range(n)
    ]


def test_ready_continuity_20_samples_below_p95_p99():
    result = evaluate_stage4_ready_intervals(_samples())
    assert result["status"] == "PASS"
    assert result["count"] == 20
    assert result["p95_ms"] == 15000
    assert result["broker_ack_fill_proven"] is False


def test_39_second_single_gap_blocks_full_acceptance():
    result = evaluate_stage4_ready_intervals(
        _samples(n=19) + _samples(ms=39000., n=1)
    )
    assert result["status"] == "NOT_PROVEN"
    assert "STAGE4_WRITER_STALE_RISK" in result["blockers"]
    assert result["p99_ms"] == 39000


def test_failed_reconcile_or_insufficient_samples_cannot_be_green():
    assert evaluate_stage4_ready_intervals([])["status"] == "NOT_PROVEN"
    result = evaluate_stage4_ready_intervals(
        _samples() + ["ERROR [US_STAGE4][RECONCILE_FAILURE] type=TimeoutError"]
    )
    assert result["status"] == "NOT_PROVEN"
    assert "STAGE4_RECONCILE_FAILURE_OBSERVED" in result["blockers"]


def test_stage4_parser_does_not_accept_unrelated_tick_or_fabricated_line():
    result = evaluate_stage4_ready_intervals(
        ["INFO [US_TICK][LATENCY] total_ms=100"] * 30
    )
    assert result["count"] == 0
    assert result["status"] == "NOT_PROVEN"
