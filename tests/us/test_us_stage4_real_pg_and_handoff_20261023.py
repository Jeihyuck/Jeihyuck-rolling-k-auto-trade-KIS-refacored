"""Stage4 nonbroker writer handoff and account singleton PostgreSQL evidence."""
from __future__ import annotations

import os
import uuid
from datetime import datetime,timezone

import pytest

from trader.us.execution import authoritative_reconcile_worker as worker


def test_one_stage4_worker_owns_fills_replay_ack_reservations_and_stop_settlement(monkeypatch):
    from trader.us.execution import fills,order_journal,reconcile,reservation_reconcile_worker
    from trader.us.db import repos
    from trader.us.execution import protective_stop_settlement_worker as stop
    from trader.us import protective_stop_obligation as stop_ledger
    from trader.us.runner import trade_tick_runner as tick
    # The deferred recovery path runs only when an actual durable stop debt
    # exists. Keep this ownership test on that path (no production gate bypass).
    monkeypatch.setattr(stop_ledger, "load_pending_stop_obligations",
                        lambda **kwargs: [{"symbol": "AMD", "severity": 3}])

    sequence=[]
    def record(name,ret):
        def wrapped(*args, **kwargs):
            sequence.append(name)
            return ret
        return wrapped
    monkeypatch.setattr(fills,"get_fills_today",record("fills",{"status":"OK","fills":[]}))
    monkeypatch.setattr(repos,"save_fills_with_result",record("persist_fills",{"status":"OK"}))
    monkeypatch.setattr(order_journal,"replay_order_journal",
                        lambda *a,**kw:sequence.append("journal") or
                        {"status":"OK","unresolved_count":0})
    monkeypatch.setattr(reconcile,"reconcile_ack_orders_with_balance",
                        record("ack",{"status":"OK","pending_count":0,"unresolved_count":0}))
    monkeypatch.setattr(reservation_reconcile_worker,
                        "reconcile_proven_reservations_once",
                        record("reservation",{"unresolved":0}))
    monkeypatch.setattr(tick,"_mark_trend_stages_from_records",
                        lambda *a,**kw:sequence.append("trend"))
    monkeypatch.setattr(tick,"_mark_soft_stop_stages_from_records",
                        lambda *a,**kw:sequence.append("soft"))
    monkeypatch.setattr(worker,"recover_stop_tp_conflicts_once",
                        record("tp",{"checked":0}))
    monkeypatch.setattr(stop,"settle_verified_stop_debts_once",
                        record("stop",{"settled":0}))
    result=worker.run_reconcile_once(
        provider=object(),trade_date="2026-10-23",session_run_id="test",
        writer_generation=str(uuid.uuid4()),conn=object(),scope={},
    )
    assert sequence==["fills","persist_fills","journal","ack",
                      "trend","soft","reservation"]
    assert result["reconcile_status"]=="OK"
    deferred=worker.run_deferred_recovery_once(provider=object(),
                                                trade_date="2026-10-23")
    assert sequence[-2:]==["tp","stop"]
    assert deferred["settlement"]["settled"]==0


@pytest.mark.skipif(not os.getenv("PBCORE_TEST_POSTGRES_URL"),
                    reason="requires real PostgreSQL integration")
def test_stage4_postgres_account_single_writer_and_health(monkeypatch):
    from pathlib import Path
    from sqlalchemy import create_engine,text
    from trader.us.db import repos
    engine=create_engine(os.environ["PBCORE_TEST_POSTGRES_URL"],future=True)
    account=uuid.uuid4().hex*2
    epoch="test-"+uuid.uuid4().hex
    generation=str(uuid.uuid4())
    scope={"env":"practice","account_id":account,"trading_epoch_id":epoch}
    lock=worker.advisory_key(scope)
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(
                Path("migrations/0059_us_stage4_reconcile_health.sql").read_text()
            )
        with engine.connect() as first,engine.connect() as second:
            assert first.execute(text("SELECT pg_try_advisory_lock(:k)"),
                                 {"k":lock}).scalar_one() is True
            first.commit()
            assert second.execute(text("SELECT pg_try_advisory_lock(:k)"),
                                  {"k":lock}).scalar_one() is False
            second.commit()
            with engine.begin() as conn:
                worker.publish_health(
                    conn,scope=scope,generation=generation,state="READY",
                    result={"reconcile_status":"OK","replay_status":"OK",
                            "pending_count":0,"unresolved_count":0,"filled_rows":1},
                )
            monkeypatch.setattr(repos,"_us_execution_claim_scope",lambda:scope)
            monkeypatch.setattr(repos,"_get_engine_or_none",lambda:engine)
            monkeypatch.setenv("US_RECONCILE_WORKER_MODE","WORKER")
            monkeypatch.setenv("US_STAGE4_EXPECTED_GENERATION",generation)
            monkeypatch.setenv("US_STAGE4_WORKER_ALIVE","1")
            monkeypatch.setenv("US_STAGE4_WRITER_AUTHORIZED","1")
            monkeypatch.setenv("US_PROTECTIVE_STOP_LEDGER_MODE","ENFORCE")
            actual=worker.read_worker_health(max_age_seconds=60)
            assert actual["worker_state"]=="READY"
            assert actual["filled_rows"]==1
            monkeypatch.setenv("US_STAGE4_EXPECTED_GENERATION",str(uuid.uuid4()))
            with pytest.raises(RuntimeError,match="GENERATION_MISMATCH"):
                worker.read_worker_health(max_age_seconds=60)
            assert first.execute(text("SELECT pg_advisory_unlock(:k)"),
                                 {"k":lock}).scalar_one() is True
            first.commit()
            assert second.execute(text("SELECT pg_try_advisory_lock(:k)"),
                                  {"k":lock}).scalar_one() is True
            second.execute(text("SELECT pg_advisory_unlock(:k)"),
                           {"k":lock})
            second.commit()
    finally:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM us_stage4_reconcile_health WHERE trading_epoch_id=:e"),
                         {"e":epoch})
        engine.dispose()
