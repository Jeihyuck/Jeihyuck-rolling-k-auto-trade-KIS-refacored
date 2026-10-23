"""Offline strict Stage4 real-log acceptance contract; never fabricates samples."""
from __future__ import annotations

from scripts.us_stage4_latency_acceptance import evaluate_logs, nearest_rank_p95


def selected(index: int, ms: int = 9000, *, owner="US_STANDARD",
             exit_type="hard_stop_loss", signal_id=None, key=None) -> str:
    return ("INFO [US_EXIT][SIGNAL_TO_BROKER_BOUNDARY] "
            f"symbol=S{index} attempt=a{index} signal_id={signal_id or 'signal'+str(index)} "
            f"source=selected_intent_exact delay_ms={ms}.00 "
            f"owner={owner} exit_type={exit_type} order_key={key or 'key'+str(index)}")


def tick(index: int, ms: int = 59000) -> str:
    return (f"INFO [US_TICK][LATENCY] tick={index} total_ms={ms}.0 "
            "balance_ms=1 fills_ms=2 session=am run=run-one")


def test_both_proven_p95_below_threshold_with_twenty_distinct_orders_and_ticks():
    lines = [selected(i) for i in range(20)] + [tick(i) for i in range(20)]
    result = evaluate_logs(lines)
    assert result["status"] == "PASS"
    assert result["selected_protective_unique_order_count"] == 20
    assert result["comparable_unique_tick_count"] == 20
    assert result["native_boundary_is_not_kis_ack_or_fill"] is True


def test_repeated_same_order_key_and_nonprotective_owner_do_not_pad_sample():
    lines = ([selected(i) for i in range(19)] +
             [selected(0, key="key0")] +
             [selected(20, owner="TQQQ_INFINITE")] +
             [selected(21, exit_type="profit_protect")] +
             ["[US_ORDER][PRE_IO_FAILURE] reason=expired"] +
             [tick(i) for i in range(20)])
    result = evaluate_logs(lines)
    assert result["status"] == "NOT_PROVEN"
    assert result["selected_protective_unique_order_count"] == 19
    assert result["pre_http_failures_not_counted_as_http"] == 1


def test_signal_missing_identity_and_insufficient_ticks_fail_closed():
    lines = ([selected(i) for i in range(20)] +
             [selected(22, signal_id="UNOBSERVED")] + [tick(i) for i in range(19)])
    result = evaluate_logs(lines)
    assert result["status"] == "NOT_PROVEN"
    assert result["excluded_missing_selected_identity"] == 1
    assert result["whole_tick_gate"] == "NOT_PROVEN"


def test_conservative_nearest_rank_p95_and_threshold():
    assert nearest_rank_p95([]) is None
    assert nearest_rank_p95(list(range(1, 21))) == 19
    lines = ([selected(i, ms=9000 if i < 18 else 11000) for i in range(20)] +
             [tick(i, ms=59000 if i < 18 else 62000) for i in range(20)])
    result = evaluate_logs(lines)
    assert result["selected_protective_signal_to_http_p95_ms"] == 11000
    assert result["whole_tick_p95_ms"] == 62000
    assert result["status"] == "NOT_PROVEN"
