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


def test_ordered_exit_history_wait_is_measured_without_lookahead_or_reordering():
    from trader.kr.exit_latency_evidence import timed_ordered_exit_history
    ticks = iter([1., 1.4, 2., 13.])
    def clock():
        return next(ticks)
    observed = list(timed_ordered_exit_history([("holding1", 1), ("holding2", 2)], clock=clock))
    assert [value[1][0] for value in observed] == ["holding1", "holding2"]
    assert abs(observed[0][0] - 0.4) < 1e-9
    assert observed[1][0] == 11.0


def test_empty_ordered_exit_history_does_not_create_dummy_holdings():
    from trader.kr.exit_latency_evidence import timed_ordered_exit_history
    assert list(timed_ordered_exit_history([])) == []
