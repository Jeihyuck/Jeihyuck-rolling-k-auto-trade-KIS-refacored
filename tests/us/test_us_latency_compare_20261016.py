"""Provenance-aware comparison: legacy signal clocks never become exact."""
from scripts.us_latency_compare import compare, summarize


def test_legacy_aggregate_sell_delay_excluded_from_exact_metric(tmp_path):
    log = tmp_path / "tick.log"
    log.write_text(
        "[US_TICK][PHASE_BUDGET] total_ms=40000 exit_engine_ms=12000 "
        "order_reconcile_ms=20000 market_state_ms=1000\n"
        "[US_EXIT][SIGNAL_TO_BROKER_BOUNDARY] symbol=AAA "
        "attempt=one delay_ms=90000\n"
        "[US_EXIT][SIGNAL_TO_BROKER_BOUNDARY] symbol=BBB "
        "attempt=two source=selected_intent_exact delay_ms=4000\n"
        "[US_EXIT][SNAPSHOT_OUTCOME] symbol=CCC outcome=SKIP "
        "reason=quote_stale total_ms=3000\n",
        encoding="utf-8",
    )
    data = summarize(log)
    assert data["distributions_seconds"]["tick_total_sec"]["p95"] == 40.0
    assert data["distributions_seconds"]["exact_selected_sell_to_http_sec"]["p95"] == 4.0
    assert data["counters"]["sell_http_legacy_aggregate"] == 1
    assert data["counters"]["snapshot_failure"] == 1
    assert data["distributions_seconds"]["tick_total_sec"]["underpowered_p95"]


def test_no_delta_claim_when_candidate_or_baseline_has_too_few_ticks(tmp_path):
    old = tmp_path / "old.log"
    new = tmp_path / "new.log"
    old.write_text("[US_TICK][PHASE_BUDGET] total_ms=150000\n", encoding="utf-8")
    new.write_text("[US_TICK][PHASE_BUDGET] total_ms=80000\n", encoding="utf-8")
    result = compare(summarize(old), summarize(new))
    assert result["tick_total_sec"]["comparison_valid"] is False
    assert result["tick_total_sec"]["p95_delta_sec"] is None
