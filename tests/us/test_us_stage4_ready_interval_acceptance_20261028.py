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


def test_stage4_only_cli_accepts_ready_without_protective_sell_samples(tmp_path, monkeypatch, capsys):
    """Stage4 evidence and SELL latency are independent acceptance domains."""
    import sys
    from scripts import us_stage4_latency_acceptance as cli
    log = tmp_path / "stage4-worker.log"
    log.write_text("\n".join(_samples()) + "\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["stage4_acceptance", "--stage4-only", str(log)])
    assert cli.main() == 0
    import json
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "PASS"
    assert output["stage4_writer_acceptance_only"] is True
    assert output["signal_to_http_gate"] == "NOT_PROVEN"
    assert output["stage4_writer_ready_continuity"]["count"] == 20
    monkeypatch.setattr(sys, "argv", ["stage4_acceptance", str(log)])
    assert cli.main() == 2
    assert json.loads(capsys.readouterr().out)["status"] == "NOT_PROVEN"


def test_real_stage4_startup_timeout_and_block_all_orders_override_green_samples():
    events = (
        "[US_STAGE4][STARTUP_TIMEOUT] last_code=US_STAGE4_WRITER_STALE",
        "[US_STAGE4][TICK_HEALTH_RETRY_EXHAUSTED] last_code=US_STAGE4_WRITER_STALE",
        "[US_STAGE4][BLOCK_ALL_ORDERS] reason=RuntimeError",
    )
    for event in events:
        proof = evaluate_stage4_ready_intervals(_samples() + [event])
        assert proof["status"] == "NOT_PROVEN"
        assert proof["blockers"], event
    summary = evaluate_stage4_ready_intervals(_samples() + list(events))
    assert summary["startup_timeout_count"] == 1
    assert summary["tick_health_retry_exhausted_count"] == 1
    assert summary["block_all_orders_count"] == 1


def test_stage4_only_cli_rejects_log_with_single_stale_gap(tmp_path, monkeypatch):
    import sys
    from scripts import us_stage4_latency_acceptance as cli
    log = tmp_path / "stage4-worker-stale.log"
    log.write_text("\n".join(_samples(n=19) + _samples(ms=31000., n=1)))
    monkeypatch.setattr(sys, "argv", ["stage4_acceptance", "--stage4-only", str(log)])
    assert cli.main() == 2
