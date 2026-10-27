from trader.kr.exit_latency_evidence import classify_exit_evaluation_latency


def test_exit_latency_bands_are_disjoint():
    assert classify_exit_evaluation_latency(0) == "OK"
    assert classify_exit_evaluation_latency(10) == "OK"
    assert classify_exit_evaluation_latency(10.1) == "SLOW"
    assert classify_exit_evaluation_latency(30) == "SLOW"
    assert classify_exit_evaluation_latency(30.1) == "CRITICAL"


def test_exit_latency_rejects_invalid_times():
    assert classify_exit_evaluation_latency(-1) == "UNKNOWN"
    assert classify_exit_evaluation_latency(float("nan")) == "UNKNOWN"
    assert classify_exit_evaluation_latency(None) == "UNKNOWN"
