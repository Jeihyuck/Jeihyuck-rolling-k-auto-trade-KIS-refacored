"""2026-10-26 AM/Afternoon incident: Stage4 startup and terminal safety contracts.

Practice-only; never submits KIS orders. The optional PostgreSQL test uses
PBCORE_TEST_POSTGRES_URL and isolated account+epoch fixture rows.
"""
from __future__ import annotations

import os
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from trader.us.execution import authoritative_reconcile_worker as worker
from trader.us.execution.stage4_lifecycle import (
    stop_stage4_worker,
    wait_for_stage4_ready,
)


def test_terminal_state_never_fabricates_unresolved_order_count():
    recorded = []

    class Connection:
        def execute(self, query, params):
            recorded.append((str(query), params))
            return SimpleNamespace(rowcount=1)

    scope = {"env": "practice", "account_id": "a"*64,
             "trading_epoch_id": str(uuid.uuid4())}
    generation = str(uuid.uuid4())
    for state in ("FAILED", "STOPPED"):
        worker.publish_health(
            Connection(), scope=scope, generation=generation, state=state,
        )
        sql, params = recorded[-1]
        assert "UPDATE us_stage4_reconcile_health" in sql
        assert "account_fingerprint=:account" in sql
        assert "trading_epoch_id=:epoch" in sql
        assert "writer_generation=CAST(:generation AS uuid)" in sql
        assert "evidence_at=NULL" in sql
        assert "unresolved_count" not in sql
        assert "reconcile_status" not in sql
        assert "replay_status" not in sql
        assert params["state"] == state
        assert params["generation"] == generation


def test_ready_row_still_records_true_broker_evidence():
    captured = []

    class Conn:
        def execute(self, query, params):
            captured.append((str(query), params))

    worker.publish_health(
        Conn(),
        scope={"env": "practice", "account_id": "a"*64,
               "trading_epoch_id": str(uuid.uuid4())},
        generation=str(uuid.uuid4()), state="READY",
        result={"reconcile_status": "OK", "replay_status": "OK",
                "pending_count": 0, "unresolved_count": 0, "filled_rows": 0},
    )
    assert "INSERT INTO us_stage4_reconcile_health" in captured[0][0]
    assert captured[0][1]["unresolved"] == 0
    assert captured[0][1]["replay"] == "OK"


def test_stage4_unexpected_child_exit_is_reaped_not_discarded():
    source = (Path(__file__).resolve().parents[2]
              / "trader/us/runner/trade_session_runner.py").read_text()
    assert 'final_reason = "stage4_worker_exited"' in source
    assert 'if _reconcile_mode == "WORKER":\n                        os.environ["US_STAGE4_WORKER_ALIVE"] = "0"' in source
    assert 'stop_stage4_worker(' in source
    assert 'if _stage4_stop_unverified:' in source


def test_log_mail_includes_stage4_evidence_and_separate_execution_failure():
    source = (Path(__file__).resolve().parents[2]
              / "scripts/wsl/send-market-log-mail.sh").read_text()
    assert 'stage4_purpose in am afternoon' in source
    assert "REQUIRED_STAGE4_WORKER_LOG_MISSING_" in source
    assert "record_missing" in source
    assert "data['daily_execution_status']" in source
    assert "data['close_report_status']" in source
    assert "[EXECUTION_FAILED]" in source


@pytest.mark.skipif(not os.getenv("PBCORE_TEST_POSTGRES_URL"),
                    reason="isolated PostgreSQL integration is not configured")
def test_postgres_delayed_ready_mismatched_generation_and_terminal_counts(monkeypatch):
    from sqlalchemy import create_engine, text
    from trader.us.db import repos

    engine = create_engine(os.environ["PBCORE_TEST_POSTGRES_URL"], future=True)
    generation = str(uuid.uuid4())
    prior_generation = str(uuid.uuid4())
    scope = {"env": "practice", "account_id": uuid.uuid4().hex * 2,
             "trading_epoch_id": "stage4-test-" + uuid.uuid4().hex}
    # Test only a new account+epoch; no market KIS API, orders or real claims.
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: scope)
    for k, v in {
        "US_RECONCILE_WORKER_MODE": "WORKER",
        "US_STAGE4_WRITER_AUTHORIZED": "1",
        "US_PROTECTIVE_STOP_LEDGER_MODE": "ENFORCE",
        "US_STAGE4_WORKER_ALIVE": "1",
        "US_STAGE4_EXPECTED_GENERATION": generation,
    }.items():
        monkeypatch.setenv(k, v)

    def publish(current_generation, *, pending=0, unresolved=0):
        with engine.begin() as conn:
            worker.publish_health(
                conn, scope=scope, generation=current_generation,
                state="READY", result={
                    "reconcile_status": "OK", "replay_status": "OK",
                    "pending_count": pending, "unresolved_count": unresolved,
                    "filled_rows": 0,
                },
            )

    class Alive:
        @staticmethod
        def poll():
            return None

    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(
                Path("migrations/0059_us_stage4_reconcile_health.sql").read_text()
            )
        # An old session's READY row exists; the new session must NOT
        # proceed until the *current-generation* writer is ready.
        publish(prior_generation, pending=1, unresolved=0)
        def late_writer():
            time.sleep(.15)
            publish(generation, pending=2, unresolved=0)
        thread = threading.Thread(target=late_writer, daemon=True)
        thread.start()
        evidence = wait_for_stage4_ready(
            Alive(), timeout_sec=4.0, poll_sec=.05,
            health_max_age_seconds=30,
        )
        thread.join(timeout=2)
        assert str(uuid.UUID(str(evidence["writer_generation"]))) == generation
        assert evidence["pending_count"] == 2

        # Terminal publication retains true broker results and does not allow
        # the earlier writer to overwrite a newer generation.
        with engine.begin() as conn:
            worker.publish_health(
                conn, scope=scope, generation=prior_generation, state="STOPPED",
            )
            worker.publish_health(
                conn, scope=scope, generation=generation, state="STOPPED",
            )
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT worker_state, reconcile_status, replay_status,
                       pending_count, unresolved_count, evidence_at
                FROM us_stage4_reconcile_health
                WHERE env=:env AND account_fingerprint=:account
                  AND trading_epoch_id=:epoch
            """), {"env": "practice", "account": scope["account_id"],
                    "epoch": scope["trading_epoch_id"]}).mappings().one()
        assert row["worker_state"] == "STOPPED"
        assert row["reconcile_status"] == "OK"
        assert row["replay_status"] == "OK"
        assert row["pending_count"] == 2
        assert row["unresolved_count"] == 0
        assert row["evidence_at"] is None
    finally:
        with engine.begin() as conn:
            conn.execute(text("""
                DELETE FROM us_stage4_reconcile_health
                WHERE env=:env AND account_fingerprint=:account
                  AND trading_epoch_id=:epoch
            """), {"env": "practice", "account": scope["account_id"],
                    "epoch": scope["trading_epoch_id"]})
        engine.dispose()
