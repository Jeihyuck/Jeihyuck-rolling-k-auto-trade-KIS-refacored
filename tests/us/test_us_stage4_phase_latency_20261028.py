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


def test_stage4_deferred_stop_repairs_are_one_phase_one_candidate_per_cycle(monkeypatch):
    from trader.us.execution import authoritative_reconcile_worker as worker
    from trader.us import protective_stop_obligation as obligations
    from trader.us.execution import protective_stop_settlement_worker as settlement
    monkeypatch.setattr(worker, "_DEFERRED_PHASE_CURSOR", 0)
    monkeypatch.setattr(obligations, "load_pending_stop_obligations",
                        lambda **kw: [{"symbol": "KO"}])
    observed = []
    def tp(**kwargs):
        observed.append(("tp", kwargs.get("max_candidates")))
        return {"checked": 1}
    def settle(**kwargs):
        observed.append(("settlement", kwargs.get("limit")))
        return {"checked": 1, "settled": 0, "blocked": 1}
    monkeypatch.setattr(worker, "recover_stop_tp_conflicts_once", tp)
    monkeypatch.setattr(settlement, "settle_verified_stop_debts_once", settle)
    first = worker.run_deferred_recovery_once(provider=object(), trade_date="2026-10-27")
    assert first["phase"] == "tp"
    assert observed == [("tp", 1)]
    second = worker.run_deferred_recovery_once(provider=object(), trade_date="2026-10-27")
    assert second["phase"] == "settlement"
    assert observed == [("tp", 1), ("settlement", 1)]
    third = worker.run_deferred_recovery_once(provider=object(), trade_date="2026-10-27")
    assert third["phase"] == "tp"
    assert observed[-1] == ("tp", 1)


def test_stage4_deferred_no_debt_never_invokes_kis_recovery(monkeypatch):
    from trader.us.execution import authoritative_reconcile_worker as worker
    from trader.us import protective_stop_obligation as obligations
    monkeypatch.setattr(obligations, "load_pending_stop_obligations", lambda **kw: [])
    def forbid(**kwargs):
        raise AssertionError("No historical KIS inquiries without real debt")
    monkeypatch.setattr(worker, "recover_stop_tp_conflicts_once", forbid)
    assert worker.run_deferred_recovery_once(
        provider=object(), trade_date="2026-10-27"
    )["phase"] == "no_debt"
