"""New stop-policy and obligation migrations against the actual CI PostgreSQL."""
from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pytest

pytestmark=pytest.mark.skipif(not os.getenv("PBCORE_TEST_POSTGRES_URL"),
                             reason="real PostgreSQL integration URL unavailable")


def test_real_pg_stop_obligation_idempotent_escalation_and_no_reopen(monkeypatch):
    from sqlalchemy import create_engine, text
    import trader.us.db.repos as repos
    from trader.us.protective_stop_obligation import record_confirmed_stop

    engine=create_engine(os.environ["PBCORE_TEST_POSTGRES_URL"],future=True)
    epoch="test-stop-"+uuid4().hex
    lifecycle="life-"+uuid4().hex
    try:
        with engine.begin() as conn:
            for filename in ("0057_us_stop_policy_revisions.sql",
                             "0058_us_protective_stop_obligations.sql"):
                conn.exec_driver_sql(Path("migrations",filename).read_text(encoding="utf-8"))
        monkeypatch.setattr(repos,"_get_engine_or_none",lambda:engine)
        monkeypatch.setattr(repos,"_active_us_epoch",lambda conn:epoch)
        monkeypatch.setenv("KIS_ENV","practice")
        monkeypatch.setenv("KIS_HTTP_BLOCK","0")
        monkeypatch.setenv("OFFLINE_MODE","0")
        monkeypatch.setenv("US_PROTECTIVE_STOP_LEDGER_MODE","ENFORCE")
        position={"symbol":"AMD","account_id":"practice-account",
                  "strategy_owner":"US_STANDARD","position_lifecycle_id":lifecycle}
        assert record_confirmed_stop(position,exit_type="soft_stop_loss",
                target_qty=2,pnl_pct=-.031,signal_price=96.9,
                policy={"soft_stop":.03,"hard_stop":.06})
        assert record_confirmed_stop(position,exit_type="hard_stop_loss",
                target_qty=4,pnl_pct=-.071,signal_price=92.9,
                policy={"soft_stop":.03,"hard_stop":.06})
        assert record_confirmed_stop(position,exit_type="soft_stop_loss",
                target_qty=1,pnl_pct=-.035,signal_price=96.5,
                policy={"soft_stop":.03,"hard_stop":.06})
        with engine.begin() as conn:
            row=conn.execute(text("""
                SELECT stop_family,severity,target_qty,obligation_state
                FROM us_protective_stop_obligations
                WHERE trading_epoch_id=:epoch AND lifecycle_id=:life
            """),{"epoch":epoch,"life":lifecycle}).mappings().one()
            assert row["severity"]==3
            assert row["stop_family"]=="hard_stop_loss"
            assert row["target_qty"]==4
            assert row["obligation_state"]=="PENDING"
            # A terminal claim cannot be re-opened by a delayed old signal.
            conn.execute(text("""
                UPDATE us_protective_stop_obligations
                SET obligation_state='SATISFIED',
                    terminal_proof='{"source":"TEST_WITHOUT_KIS_ORDER"}'::jsonb
                WHERE trading_epoch_id=:epoch AND lifecycle_id=:life
            """),{"epoch":epoch,"life":lifecycle})
        assert record_confirmed_stop(position,exit_type="hard_stop_loss",
            target_qty=4,pnl_pct=-.080,signal_price=92.0,
            policy={"soft_stop":.03,"hard_stop":.06}) is False
        with engine.begin() as conn:
            assert conn.execute(text("""
                SELECT obligation_state FROM us_protective_stop_obligations
                WHERE trading_epoch_id=:epoch AND lifecycle_id=:life
            """),{"epoch":epoch,"life":lifecycle}).scalar_one()=="SATISFIED"
    finally:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM us_protective_stop_obligations WHERE trading_epoch_id=:e"),{"e":epoch})
        engine.dispose()
