"""Stage4 phase timing evidence is monotonic and never changes order policy."""
from trader.us.execution.stage4_phase_latency import Stage4PhaseTimer


def test_phase_metrics_use_monotonic_intervals_and_accounting():
    ticks = iter([10.0, 10.1, 10.3, 10.31, 10.5])
    timer = Stage4PhaseTimer(clock=lambda: next(ticks))
    assert abs(timer.mark("kis_fill_fetch") - 100.0) < 1e-7
    assert abs(timer.mark("db_fill_persist_or_memo") - 200.0) < 1e-7
    assert abs(timer.mark("ack_reconcile") - 10.0) < 1e-7
    result = timer.summary()
    assert abs(result["total_ms"] - 500.0) < 1e-7
    assert len(result["phases_ms"]) == 3


def test_monotonic_clock_regression_is_clamped():
    ticks = iter([11.0, 10.0, 10.0])
    timer = Stage4PhaseTimer(clock=lambda: next(ticks))
    assert timer.mark("kis_fill_fetch") == 0.0
    assert timer.summary()["total_ms"] == 0.0


def test_stage4_percentiles_are_nearest_rank_not_average():
    from trader.us.execution.stage4_phase_latency import latency_percentiles
    vals = [9000.] * 18 + [26000., 39000.]
    report = latency_percentiles(vals)
    assert report["count"] == 20
    assert report["p50_ms"] == 9000.
    assert report["p95_ms"] == 26000.
    assert report["p99_ms"] == 39000.
    assert report["max_ms"] == 39000.


def test_stage4_ready_window_bounded_and_bad_samples_rejected():
    from trader.us.execution.stage4_phase_latency import RollingStage4Latency
    window = RollingStage4Latency(size=3)
    for ms in (100., 200., 300., 400.):
        result = window.observe(ms)
    assert result["count"] == 3
    assert result["p50_ms"] == 300.
    assert result["p99_ms"] == 400.
    assert window.observe(float("nan")) == result
