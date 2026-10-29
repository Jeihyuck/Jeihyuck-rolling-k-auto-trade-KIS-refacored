from trader.us.execution.latency_evidence import latency_summary

def test_missing_not_zero_and_stale_alert():
    result = latency_summary([
        {"submit_to_ack": 1.0, "stage4_ready_interval": 20.0},
        {"submit_to_ack": 3.0, "stage4_ready_interval": 35.0},
        {"submit_to_ack": None},
    ])
    assert result["phases"]["submit_to_ack"]["count"] == 2
    assert result["phases"]["submit_to_ack"]["p50_sec"] == 2.0
    assert result["phases"]["submit_to_ack"]["missing_count"] == 1
    assert result["phases"]["stage4_ready_interval"]["stale_risk_count"] == 1

def test_invalid_samples_are_not_silently_treated_as_zero():
    result = latency_summary([{"submit_to_ack": float("nan")},
                              {"submit_to_ack": -5},
                              {"submit_to_ack": "not-a-number"}])
    assert result["phases"]["submit_to_ack"]["count"] == 0
    assert result["invalid_count"] == 3

def test_measured_tick_phases_not_confused_with_ack_or_fill():
    from trader.us.execution.latency_evidence import observed_tick_sample
    sample = observed_tick_sample({
        "tick_total_ms": 5000, "order_route_ms": 1200,
        "fill_persist_ms": 250, "broker_http_by_attempt_ms": {"a": 340},
    })
    assert sample["tick_total"] == 5.0
    assert sample["order_route"] == 1.2
    assert sample["broker_http_by_attempt_sec"] == {"a": 0.34}
    assert "submit_to_ack" not in sample
    assert "ack_to_fill" not in sample
    summary = latency_summary([sample])
    assert summary["phases"]["order_route"]["p50_sec"] == 1.2
    assert summary["phases"]["broker_http"]["count"] == 1
    assert summary["phases"]["submit_to_ack"]["count"] == 0
