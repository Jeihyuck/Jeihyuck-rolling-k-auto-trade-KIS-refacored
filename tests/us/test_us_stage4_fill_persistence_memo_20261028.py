"""US Stage4 2026-10-27 stale-writer incident: eliminate duplicate fill writes.

No KIS orders are submitted by these tests.
"""
from __future__ import annotations

from trader.us.execution.stage4_fill_memo import VerifiedFillPersistenceMemo, fills_digest


def _fill(*, qty=8, price=90.5, observed_at="2026-10-27T21:00:00Z"):
    return {
        "order_no": "123", "symbol": "SYY", "side": "BUY",
        "qty": qty, "price": price, "observed_at": observed_at,
        "meta": {"observed_at": observed_at, "cumulative_filled_qty": qty},
        "raw": {"ft_ccld_qty": str(qty), "ft_ccld_unpr3": str(price)},
    }


def test_identical_cumulative_broker_evidence_skips_redundant_db_save():
    now = [0.0]
    memo = VerifiedFillPersistenceMemo(clock=lambda: now[0], refresh_sec=120)
    initial = [_fill()]
    assert memo.should_persist(initial)
    memo.mark_persisted(initial, status="OK")
    now[0] = 10.0
    assert not memo.should_persist([_fill(observed_at="2026-10-27T21:01:00Z")])
    now[0] = 120.0
    assert memo.should_persist([_fill()])


def test_changed_broker_qty_price_and_raw_evidence_force_write():
    memo = VerifiedFillPersistenceMemo(clock=lambda: 2.0)
    memo.mark_persisted([_fill()], status="OK")
    assert memo.should_persist([_fill(qty=9)])
    assert memo.should_persist([_fill(price=91.0)])
    different = _fill()
    different["raw"]["ord_dt"] = "20261028"
    assert memo.should_persist([different])


def test_failed_save_never_memoizes_and_restart_requires_save():
    memo = VerifiedFillPersistenceMemo(clock=lambda: 0.0)
    memo.mark_persisted([_fill()], status="DB_ERROR")
    assert memo.should_persist([_fill()])
    memo.mark_persisted([_fill()], status="OK")
    assert not memo.should_persist([_fill()])
    memo.invalidate()
    assert memo.should_persist([_fill()])
    assert VerifiedFillPersistenceMemo(clock=lambda: 0.0).should_persist([_fill()])


def test_empty_broker_fills_not_memoized():
    memo = VerifiedFillPersistenceMemo(clock=lambda: 0.0)
    memo.mark_persisted([], status="OK")
    assert memo.should_persist([])


def test_mutable_save_status_not_a_new_broker_fill():
    before = _fill()
    after = _fill(observed_at="2026-10-27T21:01:00Z")
    after["save_status"] = "OK"
    assert fills_digest([before]) == fills_digest([after])


def test_nonfinite_fill_price_changes_digest_and_never_looks_like_valid_price():
    assert fills_digest([_fill(price=float("nan"))]) != fills_digest([_fill()])
    assert fills_digest([_fill(price=float("inf"))]) != fills_digest([_fill()])


def test_reconcile_cycle_still_reads_kis_and_replays_ack_on_memo_hit(monkeypatch):
    import trader.us.execution.authoritative_reconcile_worker as worker
    import trader.us.execution.fills as fills_mod
    import trader.us.db.repos as repos
    import trader.us.execution.order_journal as journal
    import trader.us.execution.reconcile as reconcile
    import trader.us.execution.reservation_reconcile_worker as reservations
    import trader.us.runner.trade_tick_runner as ticks

    events = []
    monkeypatch.setattr(worker, "_STAGE4_FILL_MEMOS", {})
    monkeypatch.setattr(fills_mod, "get_fills_today",
                        lambda **kw: (events.append("KIS"), {"status": "OK", "fills": [_fill()]})[1])
    monkeypatch.setattr(repos, "save_fills_with_result",
                        lambda *a, **k: (events.append("PERSIST"), {"status": "OK"})[1])
    monkeypatch.setattr(journal, "replay_order_journal",
                        lambda *a, **k: (events.append("REPLAY"), {"status": "OK"})[1])
    monkeypatch.setattr(reconcile, "reconcile_ack_orders_with_balance",
                        lambda **k: (events.append("ACK"), {"status": "OK"})[1])
    monkeypatch.setattr(reservations, "reconcile_proven_reservations_once",
                        lambda **k: {"unresolved": 0})
    monkeypatch.setattr(ticks, "_mark_trend_stages_from_records", lambda *a, **k: None)
    monkeypatch.setattr(ticks, "_mark_soft_stop_stages_from_records", lambda *a, **k: None)
    args = dict(provider=object(), trade_date="2026-10-27", session_run_id="test",
                writer_generation="test", conn=None, scope={})
    worker.run_reconcile_once(**args)
    worker.run_reconcile_once(**args)
    assert events.count("KIS") == 2
    assert events.count("PERSIST") == 1
    assert events.count("REPLAY") == 2
    assert events.count("ACK") == 2
