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
