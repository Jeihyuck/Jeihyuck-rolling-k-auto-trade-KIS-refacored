"""Disposable real PostgreSQL verification for KR Infinite economic fill CAS.

Never uses the canonical trading DB or KIS. Runs only against explicitly
provided local `PBCORE_TEST_POSTGRES_URL` and creates its own DROP-able schema.
"""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from decimal import Decimal
from threading import Barrier
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.engine import make_url

from trader.kr.infinite.models import BrokerOrderState, OrderIntent, State, Status
from trader.kr.infinite.repository import InfiniteRepository


DAY = date(2026, 11, 18)
CYCLE = "KRINF-PG-ATOMIC-TEST"


@pytest.fixture
def journal_pg(monkeypatch):
    db_url = os.getenv("PBCORE_TEST_POSTGRES_URL", "")
    if not db_url:
        pytest.skip("Disposable local PostgreSQL CI DSN not provided")
    parsed = make_url(db_url)
    if (
        parsed.host not in {"localhost", "127.0.0.1"}
        or parsed.database != "kr_ci"
        or parsed.username != "postgres"
        or not parsed.drivername.startswith("postgresql")
    ):
        pytest.skip("Refuse any PostgreSQL URL that is not disposable local kr_ci")
    namespace = "krinf_atomic_" + uuid4().hex[:16]
    admin = sa.create_engine(db_url, future=True)
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{namespace}"'))
    db = sa.create_engine(
        db_url, future=True,
        connect_args={"options": f"-c search_path={namespace}"},
    )
    try:
        with db.begin() as conn:
            conn.execute(text("""
                CREATE TABLE kr_infinite_state (
                    trading_epoch_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL, symbol TEXT NOT NULL,
                    cycle_id TEXT, version INTEGER NOT NULL,
                    units_used INTEGER NOT NULL,
                    core_filled_notional NUMERIC(20,4) NOT NULL,
                    reserve_filled_notional NUMERIC(20,4) NOT NULL,
                    status TEXT NOT NULL,
                    PRIMARY KEY(trading_epoch_id,strategy_id,symbol)
                )
            """))
            conn.execute(text("""
                CREATE TABLE kr_infinite_order_intents (
                    id BIGINT PRIMARY KEY,
                    trading_epoch_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL, symbol TEXT NOT NULL,
                    cycle_id TEXT NOT NULL,side TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,broker_order_id TEXT,
                    trade_date DATE NOT NULL,requested_qty INTEGER NOT NULL,
                    filled_qty INTEGER NOT NULL,
                    filled_notional_krw NUMERIC(20,4) NOT NULL,
                    filled_avg_price NUMERIC(20,8),
                    status TEXT NOT NULL,updated_at TIMESTAMPTZ
                )
            """))
            conn.execute(text("""
                INSERT INTO kr_infinite_state(
                    trading_epoch_id,strategy_id,symbol,cycle_id,version,
                    units_used,core_filled_notional,reserve_filled_notional,status
                ) VALUES('epoch-test','KR_INFINITE_V1','122630',
                         :cycle,8,1,690052,0,'ACTIVE')
            """), {"cycle": CYCLE})
            conn.execute(text("""
                INSERT INTO kr_infinite_order_intents(
                    id,trading_epoch_id,strategy_id,symbol,cycle_id,side,
                    idempotency_key,broker_order_id,trade_date,requested_qty,
                    filled_qty,filled_notional_krw,status
                ) VALUES(13,'epoch-test','KR_INFINITE_V1','122630',:cycle,
                         'BUY','key-10702','0000010702',:day,13,0,0,'SUBMITTED')
            """), {"cycle": CYCLE, "day": DAY})
        repo = InfiniteRepository(db)
        monkeypatch.setattr(repo, "_epoch_id",
                            lambda _bind, required=True: "epoch-test")

        def save_economics(conn, state, *, epoch_id):
            result = conn.execute(text("""
                UPDATE kr_infinite_state
                SET units_used=:units,core_filled_notional=:core,
                    reserve_filled_notional=:reserve,
                    status=:status,version=version+1
                WHERE trading_epoch_id=:epoch AND cycle_id=:cycle
            """), {
                "units": state.units_used,
                "core": state.core_filled_notional,
                "reserve": state.reserve_filled_notional,
                "status": state.status.value,
                "epoch": epoch_id, "cycle": state.cycle_id,
            })
            if result.rowcount != 1:
                raise RuntimeError("KR_INF_TEST_STATE_JOURNAL_MISSING")

        monkeypatch.setattr(repo, "_save_state", save_economics)
        yield db, repo
    finally:
        db.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE'))
        admin.dispose()


def _input():
    state = State(
        cycle_id=CYCLE, version=8, status=Status.ACTIVE,
        units_used=2,core_units_used=2,
        core_filled_notional=Decimal("1408952"),
        reserve_filled_notional=Decimal("0"),
    )
    intent = OrderIntent(
        id=13,cycle_id=CYCLE,trade_date=DAY,side="BUY",
        idempotency_key="key-10702",requested_qty=13,unit_sequence=2,
        broker_order_id="0000010702",status="SUBMITTED",
        filled_qty=0,filled_notional_krw=Decimal("0"),
    )
    broker = BrokerOrderState("FILLED",13,Decimal("718900"),Decimal("55300"))
    return state,intent,broker


def _snapshot(engine):
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT s.core_filled_notional AS state_notional,
                   s.units_used AS units,s.version AS version,
                   i.filled_qty AS filled_qty,
                   i.filled_notional_krw AS intent_notional
            FROM kr_infinite_state s CROSS JOIN kr_infinite_order_intents i
            WHERE s.trading_epoch_id='epoch-test' AND i.id=13
        """)).mappings().one()
        return dict(row)


def test_real_pg_13share_fill_is_atomic_and_stale_replay_cannot_duplicate(journal_pg):
    db,repo = journal_pg
    state,intent,broker = _input()
    repo.persist_reconciliation(state,[(intent,broker)])
    after = _snapshot(db)
    assert after["state_notional"] == Decimal("1408952")
    assert after["intent_notional"] == Decimal("718900")
    assert after["filled_qty"] == 13
    assert after["units"] == 2
    with pytest.raises(RuntimeError,match="INTENT_RACE_RELOAD"):
        repo.persist_reconciliation(state,[(intent,broker)])
    assert _snapshot(db) == after


def test_real_pg_two_writers_only_one_commits_kodex_fill(journal_pg):
    db,repo = journal_pg
    state,intent,broker = _input()
    gate = Barrier(2)

    def worker(_n):
        gate.wait(timeout=10)
        try:
            repo.persist_reconciliation(state,[(intent,broker)])
            return "COMMITTED"
        except RuntimeError as exc:
            assert "KR_INF_RECONCILE_INTENT_RACE_RELOAD" in str(exc)
            return "FENCED"

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(worker,[0,1]))
    assert sorted(outcomes) == ["COMMITTED","FENCED"]
    final = _snapshot(db)
    assert final["state_notional"] == Decimal("1408952")
    assert final["filled_qty"] == 13
    assert final["units"] == 2
