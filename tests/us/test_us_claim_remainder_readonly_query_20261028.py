"""Exercise actual Claim SQL on a local DB: no KIS/claim mutation allowed."""
from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text


@pytest.fixture()
def claim_probe_db(monkeypatch):
    from trader.us.db import repos

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.exec_driver_sql("""
            CREATE TABLE us_execution_claims (
                action_key TEXT PRIMARY KEY, trade_date DATE,
                env TEXT, account_id TEXT, market TEXT, trading_epoch_id TEXT,
                action_state TEXT, target_qty INTEGER,
                cumulative_filled_qty INTEGER, remaining_target_qty INTEGER,
                active_attempt_id TEXT
            )
        """)
        conn.exec_driver_sql("""
            CREATE TABLE us_execution_attempts (
                attempt_id TEXT PRIMARY KEY, action_key TEXT, attempt_no INTEGER,
                requested_qty INTEGER, cumulative_filled_qty INTEGER,
                attempt_state TEXT, authoritative BOOLEAN, created_at TEXT
            )
        """)
        conn.execute(text("""
            INSERT INTO us_execution_claims
                (action_key,trade_date,env,account_id,market,trading_epoch_id,
                 action_state,target_qty,cumulative_filled_qty,remaining_target_qty)
            VALUES
                ('syy','2026-10-27','practice',:account,'US','epoch-1',
                 'SATISFIED',41,41,0),
                ('other','2026-10-27','practice',:account,'US','epoch-2',
                 'OPEN',42,0,42)
        """), {"account": "a" * 64})
        conn.execute(text("""
            INSERT INTO us_execution_attempts
                (attempt_id,action_key,attempt_no,requested_qty,cumulative_filled_qty,
                 attempt_state,authoritative,created_at)
            VALUES
                ('a1','syy',1,41,NULL,'ABANDONED_PRE_SUBMIT',1,'2026-10-27T14:51:55Z'),
                ('a2','syy',2,41,41,'FILLED',1,'2026-10-27T15:00:26Z'),
                ('b1','other',1,42,NULL,'OPEN',0,'2026-10-27T14:51:55Z')
        """))
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: {
        "env": "practice", "account_id": "a" * 64,
        "trading_epoch_id": "epoch-1", "market": "US",
    })
    yield engine
    engine.dispose()


def test_sql_reads_only_exact_syy_claim_and_two_historical_attempts(claim_probe_db):
    from trader.us.db.repos import inspect_us_execution_claim_remainder

    proof = inspect_us_execution_claim_remainder(
        action_key="syy", trade_date="2026-10-27", env="practice",
    )
    assert proof["status"] == "VERIFIED"
    assert proof["action_state"] == "SATISFIED"
    assert proof["target_qty"] == 41
    assert proof["cumulative_filled_qty"] == 41
    assert proof["remaining_target_qty"] == 0
    assert [a["attempt_state"] for a in proof["recent_attempts"]] == [
        "ABANDONED_PRE_SUBMIT", "FILLED",
    ]
    assert [a["requested_qty"] for a in proof["recent_attempts"]] == [41, 41]
    assert not proof["broker_terminal_verified"]
    assert not proof["automatic_retry_authorized"]
    with claim_probe_db.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM us_execution_claims")).scalar_one() == 2
        assert conn.execute(text("SELECT COUNT(*) FROM us_execution_attempts")).scalar_one() == 3


def test_sql_does_not_leak_other_epoch_or_missing_action(claim_probe_db):
    from trader.us.db.repos import inspect_us_execution_claim_remainder
    assert inspect_us_execution_claim_remainder(
        action_key="other", trade_date="2026-10-27",
    )["status"] == "UNVERIFIED"
    assert inspect_us_execution_claim_remainder(
        action_key="missing", trade_date="2026-10-27",
    )["status"] == "UNVERIFIED"


def test_nonpractice_scope_fails_before_any_claim_read(claim_probe_db, monkeypatch):
    from trader.us.db import repos
    with pytest.raises(RuntimeError, match="US_CLAIM_REMAINDER_SCOPE_UNVERIFIED"):
        repos.inspect_us_execution_claim_remainder(
            action_key="syy", trade_date="2026-10-27", env="live",
        )
    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: {
        "env": "live", "account_id": "a" * 64, "trading_epoch_id": "epoch-1",
    })
    with pytest.raises(RuntimeError, match="US_CLAIM_REMAINDER_SCOPE_UNVERIFIED"):
        repos.inspect_us_execution_claim_remainder(
            action_key="syy", trade_date="2026-10-27",
        )
