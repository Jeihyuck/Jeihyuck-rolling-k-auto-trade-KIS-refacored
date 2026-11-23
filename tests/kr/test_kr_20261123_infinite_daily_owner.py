"""2026-11-23 incident: exact KR_INFINITE KIS order cannot poison PB1.

All tests use a local sqlite database and never submit broker orders.
"""
from datetime import date
from types import SimpleNamespace

import pytest
import sqlalchemy as sa

from trader.kr.infinite.daily_ccld_owner import prove_kr_infinite_daily_owner


EPOCH = "current-epoch"
ORDER_DATE = date(2026, 11, 23)
BASE_ROW = {"ord_qty": "15", "tot_ccld_qty": "15", "odno": "0000009745"}


@pytest.fixture()
def db():
    engine = sa.create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(sa.text("""
            CREATE TABLE kr_infinite_state (
                strategy_id TEXT, symbol TEXT, cycle_id TEXT,
                trading_epoch_id TEXT, status TEXT
            )
        """))
        conn.execute(sa.text("""
            CREATE TABLE kr_infinite_order_intents (
                id INTEGER PRIMARY KEY,
                strategy_id TEXT, symbol TEXT, cycle_id TEXT,
                side TEXT, broker_order_id TEXT, trade_date DATE,
                trading_epoch_id TEXT, requested_qty INTEGER,
                filled_qty INTEGER, status TEXT
            )
        """))
        conn.execute(sa.text("""
            INSERT INTO kr_infinite_state
              (strategy_id, symbol, cycle_id, trading_epoch_id, status)
            VALUES ('KR_INFINITE_V1', '122630', 'cycle-1',
                    'current-epoch', 'ACTIVE')
        """))
        conn.execute(sa.text("""
            INSERT INTO kr_infinite_order_intents
              (id, strategy_id, symbol, cycle_id, side, broker_order_id,
               trade_date, trading_epoch_id, requested_qty, filled_qty, status)
            VALUES (14, 'KR_INFINITE_V1', '122630', 'cycle-1', 'BUY',
                    '0000009745', '2026-11-23', 'current-epoch', 15, 15, 'FILLED')
        """))
    return engine


def verify(db, *, scope=None, row=None, code="122630", side="BUY",
           odno="0000009745", d=ORDER_DATE, authenticated=True):
    return prove_kr_infinite_daily_owner(
        engine=db,
        scope=scope or SimpleNamespace(
            env="practice", account_key="practice:broker:01",
            trading_epoch_id=EPOCH,
        ),
        broker_row=BASE_ROW if row is None else row,
        code=code, side=side, kis_odno=odno,
        order_date=d, broker_authenticated=authenticated,
    )


def test_20261123_kodex_15_fill_proves_owner_without_pb1_synthetic_order(db):
    result = verify(db)
    assert result is not None
    assert result["owner"] == "KR_INFINITE_V1"
    assert result["filled_qty"] == 15
    assert result["kis_odno"] == "0000009745"
    with db.connect() as conn:
        assert conn.execute(sa.text(
            "SELECT filled_qty FROM kr_infinite_order_intents WHERE id=14"
        )).scalar_one() == 15  # Read-only identification.


@pytest.mark.parametrize("changes", [
    {"code": "251970"},
    {"side": "SELL"},
    {"odno": "0000009746"},
    {"d": date(2026, 11, 20)},
    {"authenticated": False},
    {"row": {"ord_qty": "14", "tot_ccld_qty": "15"}},
    {"row": {"ord_qty": "15", "tot_ccld_qty": "14"}},
    {"row": {"ord_qty": "15", "tot_ccld_qty": "0"}},
    {"row": {"ord_qty": "15", "tot_ccld_qty": ""}},
    {"row": {"tot_ccld_qty": "15"}},
])
def test_unknown_or_contradictory_kis_source_keeps_buy_fenced(db, changes):
    assert verify(db, **changes) is None


def test_foreign_epoch_does_not_authorize_source(db):
    scope = SimpleNamespace(
        env="practice", account_key="practice:broker:01",
        trading_epoch_id="foreign-epoch",
    )
    assert verify(db, scope=scope) is None


def test_ambiguous_broker_order_does_not_authorize_source(db):
    with db.begin() as conn:
        conn.execute(sa.text("""
            INSERT INTO kr_infinite_order_intents
              (id,strategy_id,symbol,cycle_id,side,broker_order_id,
               trade_date,trading_epoch_id,requested_qty,filled_qty,status)
            VALUES (15,'KR_INFINITE_V1','122630','cycle-1','BUY',
                    '0000009745','2026-11-23','current-epoch',15,15,'FILLED')
        """))
    assert verify(db) is None


def test_uncommitted_or_pending_fill_is_not_verified(db):
    with db.begin() as conn:
        conn.execute(sa.text("""
            UPDATE kr_infinite_order_intents
               SET status='SUBMITTED',filled_qty=0
             WHERE id=14
        """))
    assert verify(db) is None


def test_state_cycle_mismatch_fails_closed(db):
    with db.begin() as conn:
        conn.execute(sa.text("""
            UPDATE kr_infinite_state
               SET cycle_id='another-cycle'
             WHERE cycle_id='cycle-1'
        """))
    assert verify(db) is None


def test_old_claim_debt_is_not_permission_and_not_global_veto():
    """Exact current-day broker proof is the BUY authority, never old debt."""
    from trader.kr.historical_recovery_journal import (
        RecoveryLane, current_tick_reconcile_proves_buy_safe,
    )

    lane = RecoveryLane(
        "CURRENT_ONLY", "journal", reason="HISTORICAL_UNRESOLVED"
    )
    proven = {
        "ok": True,
        "degraded": None,
        "historical_recovery_executed": False,
        "current_only_fresh_broker_proven": True,
        "unresolved_broker_activity": False,
        "frozen_contract_invariant_violations": 0,
        "verified_infinite_owner_orders": 1,
        "execution_claim_health": {
            "available": True,
            "unresolved_execution_actions": 2,
            "unresolved_source_date_prior": 2,
            "unresolved_source_date_current": 0,
            "unresolved_source_date_other_or_unknown": 0,
        },
    }
    assert current_tick_reconcile_proves_buy_safe(
        lane=lane, result=proven, durable_record_ok=True,
    )
    assert not current_tick_reconcile_proves_buy_safe(
        lane=lane,
        result={**proven, "ok": False, "degraded": "KIS_DAILY_CCLD_UNPROVEN"},
        durable_record_ok=True,
    )
    assert not current_tick_reconcile_proves_buy_safe(
        lane=lane,
        result={**proven, "unresolved_broker_activity": True},
        durable_record_ok=True,
    )
