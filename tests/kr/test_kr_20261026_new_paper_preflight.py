from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4

import pytest
import sqlalchemy as sa

from trader.db.schema import schema_for_engine
from trader.kr.new_paper_epoch_preflight import (
    KrPracticeReadinessError, verify_kr_practice_first_day,
)


@pytest.fixture()
def db():
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    new_id = str(uuid4())
    old_id = str(uuid4())
    with engine.begin() as conn:
        conn.execute(schema.trading_epochs.insert(), [
            {
                "trading_epoch_id": new_id, "env": "practice",
                "account_id": "practice:new:01", "status": "ACTIVE",
                "reason": "NEW_KIS_PRACTICE_ACCOUNT_20261025",
                "started_at": datetime(2026,10,25,13,53,tzinfo=timezone.utc),
            },
            {
                "trading_epoch_id": old_id, "env": "practice",
                "account_id": "practice:old:01", "status": "ACTIVE",
                "reason": "PRACTICE_RESET_20260922",
                "started_at": datetime(2026,9,22,tzinfo=timezone.utc),
            },
        ])
    yield engine, schema, new_id
    engine.dispose()


def verify(db, **kw):
    engine = db[0]
    x = dict(
        env="practice", account_id="practice:new:01",
        trade_date=date(2026,10,26),
        expected_capital_krw=100000000,max_capital_krw=100000000,
    )
    x.update(kw)
    return verify_kr_practice_first_day(engine,**x)


def test_new_epoch_passes_without_creating_portfolio_or_orders(db):
    r=verify(db)
    assert r["status"]=="KR_NEW_ACCOUNT_EPOCH_AND_100M_OK"
    assert r["pb1_portfolio_epoch_count"]==0
    schema=db[1]
    with db[0].connect() as conn:
        assert conn.execute(sa.select(sa.func.count()).select_from(schema.portfolio_epochs)).scalar_one()==0
        assert conn.execute(sa.select(sa.func.count()).select_from(schema.orders)).scalar_one()==0


def test_old_account_active_epoch_must_not_authorize_new_rollout(db):
    with pytest.raises(KrPracticeReadinessError,match="EPOCH_PROVENANCE_MISMATCH"):
        verify(db,account_id="practice:old:01")


@pytest.mark.parametrize("expected,maxcap",[
    (50000000,100000000),(100000000,50000000),(50000000,50000000),
])
def test_mismatched_capital_fails_closed(db,expected,maxcap):
    with pytest.raises(KrPracticeReadinessError,match="100M_CAPITAL_MISMATCH"):
        verify(db,expected_capital_krw=expected,max_capital_krw=maxcap)


def test_wrong_env_cannot_run_first_day(db):
    with pytest.raises(KrPracticeReadinessError,match="PRACTICE_SCOPE_REQUIRED"):
        verify(db,env="real")


def test_later_dates_not_pinned_to_first_day_epoch(db):
    assert verify(db,trade_date=date(2026,10,27))["status"]=="OUTSIDE_GO_LIVE_DATE"


def test_stale_portfolio_child_blocks(db):
    engine,schema,epoch=db
    with engine.begin() as conn:
        conn.execute(schema.portfolio_epochs.insert().values(
            portfolio_epoch_id=str(uuid4()),env="practice",
            account_id="practice:new:01",strategy="pb1_pullback_close",
            sid=1,mode=1,status="ACTIVE",trading_epoch_id=str(uuid4()),
        ))
    with pytest.raises(KrPracticeReadinessError,match="STALE_PORTFOLIO_EPOCH"):
        verify(db)
