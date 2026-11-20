"""Stage4 phase timing cannot misclassify stop/TP state I/O as claim replay.

All evidence is synthetic. No KIS GET/POST, broker credentials, or real DB.
"""
from __future__ import annotations

from trader.us.execution import authoritative_reconcile_worker as stage4


def test_stage4_current_fill_stage_and_ack_stage_have_independent_timing(monkeypatch):
    from trader.us.db import repos
    from trader.us.execution import order_journal, reconcile
    from trader.us.execution import reservation_reconcile_worker
    from trader.us.execution import stage4_phase_latency
    from trader.us.runner import trade_tick_runner

    steps = []
    def clock():
        steps.append(1)
        return len(steps) * .01

    real_timer = stage4_phase_latency.Stage4PhaseTimer
    monkeypatch.setattr(
        stage4_phase_latency, "Stage4PhaseTimer",
        lambda: real_timer(clock=clock),
    )
    monkeypatch.setattr(repos, "save_fills_with_result",
                        lambda fills, trade_date: {"status": "OK"})
    def replay(*args, **kwargs):
        assert kwargs["active_claims_only"] is True
        assert kwargs["active_claim_date_scope"] == "current"
        return {"status": "OK", "unresolved_count": 0}
    monkeypatch.setattr(order_journal, "replay_order_journal", replay)
    monkeypatch.setattr(
        reconcile, "reconcile_ack_orders_with_balance",
        lambda **kw: {"status": "OK", "pending_count": 0,
                      "unresolved_count": 0},
    )
    monkeypatch.setattr(
        reservation_reconcile_worker, "reconcile_proven_reservations_once",
        lambda **kw: {"unresolved": 0},
    )
    monkeypatch.setattr(
        trade_tick_runner, "_mark_trend_stages_from_records",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        trade_tick_runner, "_mark_soft_stop_stages_from_records",
        lambda *args, **kwargs: None,
    )
    result = stage4.run_reconcile_once(
        provider=object(), trade_date="2099-01-05",
        session_run_id="synthetic-stage4", writer_generation="synthetic",
        conn=object(), scope={},
        prefetched_fills={"status": "OK", "fills": []},
    )
    durations = result["latency"]["phases_ms"]
    assert list(durations) == [
        "kis_fill_fetch", "db_fill_persist_or_memo",
        "current_fill_stage_updates", "active_claim_replay",
        "ack_reconcile", "ack_stage_updates", "reservation_reconcile",
    ]
    assert all(value >= 0 for value in durations.values())
    assert result["reconcile_status"] == result["replay_status"] == "OK"
    assert result["filled_rows"] == 0
