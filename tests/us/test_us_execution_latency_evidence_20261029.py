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
