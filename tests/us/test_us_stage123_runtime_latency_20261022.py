"""Offline proof that runtime acceptance cannot be satisfied by fake data."""
from scripts.us.check_stage123_runtime_latency import evaluate_logs


def _runtime_log(sells: int = 20, ticks: int = 20, *,
                 sell_delay_ms: int = 1200, tick_delay_ms: int = 45000):
    lines = []
    for i in range(sells):
        lines.append(
            f"2026-10-22 23:00:{i:02d},001 INFO "
            "[US_EXIT][SIGNAL_TO_BROKER_BOUNDARY] symbol=AAA "
            f"attempt=attempt-{i} signal_id=exact-{i} "
            f"source=selected_intent_exact delay_ms={sell_delay_ms:.2f}"
        )
    for i in range(ticks):
        lines.append(
            f"2026-10-22 23:01:{i:02d},001 INFO "
            f"[US_TICK][LATENCY] tick={i} total_ms={tick_delay_ms}"
        )
    return "\n".join(lines)


def test_true_independent_samples_and_thresholds_pass_as_log_timing_only():
    result = evaluate_logs(_runtime_log())
    assert result["status"] == "PASS_LOG_TIMINGS_ONLY"
    assert result["native_KIS_proven"] is False
    assert result["selected_sell_signal_to_http_p95_sec"] == 1.2
    assert result["tick_total_p95_sec"] == 45.0


def test_fewer_than_twenty_or_unobserved_ids_fail_closed():
    assert evaluate_logs(_runtime_log(sells=19))["status"] == "FAIL"
    assert evaluate_logs(_runtime_log(ticks=19))["status"] == "FAIL"
    assert evaluate_logs(
        _runtime_log().replace("signal_id=exact-3", "signal_id=UNOBSERVED")
    )["status"] == "FAIL"


def test_breach_of_either_p95_target_fails():
    assert evaluate_logs(_runtime_log(sell_delay_ms=11000))["status"] == "FAIL"
    assert evaluate_logs(_runtime_log(tick_delay_ms=63000))["status"] == "FAIL"


def test_repeated_attempt_lines_cannot_fabricate_twenty_samples():
    log = "\n".join([
        "2026-10-22 23:00:00,001 INFO "
        "[US_EXIT][SIGNAL_TO_BROKER_BOUNDARY] symbol=AAA "
        "attempt=a signal_id=one source=selected_intent_exact delay_ms=800"
        for _ in range(30)
    ])
    assert evaluate_logs(log)["unique_selected_sell_attempts"] == 1
