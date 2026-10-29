"""PR #255 follow-on: Stage4 STARTING requires a widened DB constraint.

No KIS HTTP, trading account mutation or scheduler interaction is permitted.
"""
from __future__ import annotations

import os
from pathlib import Path
import uuid

import pytest

from trader.us.execution import authoritative_reconcile_worker as worker

ROOT = Path(__file__).resolve().parents[2]


def test_bootstrap_marker_precedes_native_kis_provider_init():
    source = (ROOT / "trader/us/execution/authoritative_reconcile_worker.py").read_text()
    marker = 'publish_health(conn, scope=scope, generation=generation, state="STARTING")'
    provider = 'provider=USDataProvider(offline=False,env="practice")'
    assert marker in source and provider in source
    assert source.index(marker) < source.index(provider)
    migration = (ROOT / "migrations/0060_us_stage4_starting_health.sql").read_text()
    assert "'STARTING'" in migration
    assert "us_stage4_reconcile_health_worker_state_check" in migration


@pytest.mark.skipif(not os.getenv("PBCORE_TEST_POSTGRES_URL"),
                    reason="requires real PostgreSQL integration")
def test_stage4_starting_db_migration_and_first_tick_fence(monkeypatch):
    from sqlalchemy import create_engine, text
    from trader.us.db import repos

    engine = create_engine(os.environ["PBCORE_TEST_POSTGRES_URL"], future=True)
    scope = {
        "env": "practice", "account_id": uuid.uuid4().hex * 2,
        "trading_epoch_id": "starting-" + uuid.uuid4().hex,
    }
    generation = str(uuid.uuid4())
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql((ROOT / "migrations/0059_us_stage4_reconcile_health.sql").read_text())
            conn.exec_driver_sql((ROOT / "migrations/0060_us_stage4_starting_health.sql").read_text())
            worker.publish_health(conn, scope=scope, generation=generation, state="STARTING")
            row = conn.execute(text("""
                SELECT worker_state, evidence_at, reconcile_status, replay_status
                  FROM us_stage4_reconcile_health
                 WHERE env=:env AND account_fingerprint=:account
                   AND trading_epoch_id=:epoch
            """), {"env": scope["env"], "account": scope["account_id"],
                   "epoch": scope["trading_epoch_id"]}).mappings().one()
            assert row["worker_state"] == "STARTING"
            assert row["evidence_at"] is None
            assert row["reconcile_status"] not in {"OK", "WARN"}

        monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: scope)
        monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
        monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
        monkeypatch.setenv("US_STAGE4_WORKER_ALIVE", "1")
        monkeypatch.delenv("US_STAGE4_PROCESS_PROOF_REQUIRED", raising=False)
        monkeypatch.setenv("US_STAGE4_EXPECTED_GENERATION", generation)
        monkeypatch.setenv("US_STAGE4_WRITER_AUTHORIZED", "1")
        monkeypatch.setenv("US_PROTECTIVE_STOP_LEDGER_MODE", "ENFORCE")
        with pytest.raises(RuntimeError, match="US_STAGE4_WRITER_NOT_READY"):
            worker.read_worker_health(max_age_seconds=60)

        with engine.begin() as conn:
            worker.publish_health(
                conn, scope=scope, generation=generation, state="READY",
                result={"reconcile_status": "OK", "replay_status": "OK",
                        "pending_count": 0, "unresolved_count": 0, "filled_rows": 0},
            )
        assert worker.read_worker_health(max_age_seconds=60)["worker_state"] == "READY"
    finally:
        with engine.begin() as conn:
            conn.execute(text(
                "DELETE FROM us_stage4_reconcile_health WHERE trading_epoch_id=:epoch"
            ), {"epoch": scope["trading_epoch_id"]})
        engine.dispose()
