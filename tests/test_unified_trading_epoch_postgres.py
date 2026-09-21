from __future__ import annotations

import os
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy import text

from trader.db.migrate import split_postgres_sql
from trader.db.trading_epoch import active_trading_epoch_id, start_new_trading_epoch


URL = os.getenv("PBCORE_TEST_POSTGRES_URL")
pytestmark = pytest.mark.skipif(not URL, reason="PBCORE_TEST_POSTGRES_URL not configured")


PRE_0052 = """
CREATE TABLE portfolio_epochs (
    portfolio_epoch_id TEXT PRIMARY KEY,
    env TEXT NOT NULL,
    account_id TEXT NOT NULL,
    sid INTEGER NOT NULL,
    mode INTEGER NOT NULL,
    strategy TEXT NOT NULL,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    reason TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE orders (
    order_id TEXT PRIMARY KEY,
    portfolio_epoch_id TEXT,
    env TEXT NOT NULL,
    client_order_key TEXT NOT NULL,
    broker_order_id TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE fills (
    fill_id TEXT PRIMARY KEY,
    portfolio_epoch_id TEXT,
    env TEXT NOT NULL,
    broker_fill_id TEXT,
    filled_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE positions (
    position_id TEXT PRIMARY KEY,
    portfolio_epoch_id TEXT,
    status TEXT NOT NULL,
    code TEXT NOT NULL,
    closed_ts TIMESTAMPTZ,
    closed_reason TEXT
);

CREATE TABLE us_order_intents (
    id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    client_order_key TEXT NOT NULL,
    strategy TEXT,
    meta JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(client_order_key)
);
CREATE TABLE us_orders (
    id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    client_order_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    order_no TEXT,
    client_order_id TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(client_order_key)
);
CREATE UNIQUE INDEX uq_us_orders_trade_date_order_no_nonblank
    ON us_orders(trade_date, order_no)
    WHERE order_no IS NOT NULL AND btrim(order_no) <> '';
CREATE UNIQUE INDEX uq_us_orders_client_order_id
    ON us_orders(client_order_id) WHERE client_order_id IS NOT NULL;

CREATE TABLE us_fills (
    id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    client_order_key TEXT NOT NULL,
    order_no TEXT,
    fill_idempotency_key TEXT,
    filled_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE UNIQUE INDEX uq_us_fills_idempotency_key
    ON us_fills(fill_idempotency_key);

CREATE TABLE us_positions (
    id BIGSERIAL PRIMARY KEY,
    as_of DATE NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    qty INTEGER NOT NULL DEFAULT 0,
    UNIQUE(as_of, symbol, exchange)
);
CREATE TABLE us_order_events (
    event_id UUID PRIMARY KEY,
    trade_date DATE NOT NULL
);
CREATE TABLE us_profit_capture_lifecycle (
    trade_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    position_lifecycle_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    PRIMARY KEY(trade_date, symbol, position_lifecycle_id, stage)
);
CREATE TABLE us_tqqq_infinite_state (
    strategy_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'IDLE',
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY(strategy_id, symbol)
);
CREATE TABLE kr_infinite_state (
    strategy_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'IDLE',
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY(strategy_id, symbol)
);
CREATE TABLE kr_infinite_order_intents (
    id BIGSERIAL PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'INTENT',
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
"""


def _engine():
    return sa.create_engine(URL, future=True)


def _apply(conn, sql: str) -> None:
    for statement in split_postgres_sql(sql):
        conn.execute(text(statement))


def test_0052_executes_on_pr135_shape_and_isolates_state():
    engine = _engine()
    migration = Path("migrations/0052_unified_trading_epoch.sql").read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.exec_driver_sql("DROP SCHEMA IF EXISTS public CASCADE")
        conn.exec_driver_sql("CREATE SCHEMA public")
        _apply(conn, PRE_0052)
        conn.execute(text("""
            INSERT INTO portfolio_epochs(
                portfolio_epoch_id,env,account_id,sid,mode,strategy,status,reason
            ) VALUES ('old-portfolio','practice','practice:test',1,1,'pb1','ACTIVE','LEGACY')
        """))
        conn.execute(text("""
            INSERT INTO positions(position_id,portfolio_epoch_id,status,code)
            VALUES ('old-position','old-portfolio','OPEN','005930')
        """))
        conn.execute(text("""
            INSERT INTO us_positions(as_of,symbol,exchange,qty)
            VALUES ('2026-09-21','AAPL','NASDAQ',1)
        """))
        conn.execute(text("""
            INSERT INTO us_tqqq_infinite_state(strategy_id,symbol,status)
            VALUES ('TQQQ_INFINITE_V3','TQQQ','ACTIVE')
        """))
        conn.execute(text("""
            INSERT INTO kr_infinite_state(strategy_id,symbol,status)
            VALUES ('KR_INFINITE_V1','122630','ACTIVE')
        """))
        conn.execute(text("""
            INSERT INTO kr_infinite_order_intents(idempotency_key,status)
            VALUES ('legacy-intent','SUBMITTED')
        """))
        _apply(conn, migration)

    with engine.connect() as conn:
        cols = {
            (r[0], r[1])
            for r in conn.execute(text("""
                SELECT table_name,column_name
                FROM information_schema.columns
                WHERE table_schema='public' AND column_name='trading_epoch_id'
            """))
        }
        for table in (
            "portfolio_epochs","orders","fills","positions",
            "us_order_intents","us_orders","us_fills","us_positions",
            "us_order_events","us_profit_capture_lifecycle",
            "us_position_risk_state","us_tqqq_infinite_state",
            "kr_infinite_state","kr_infinite_order_intents",
        ):
            assert (table, "trading_epoch_id") in cols

        assert conn.execute(text("""
            SELECT trading_epoch_id FROM us_tqqq_infinite_state
            WHERE strategy_id='TQQQ_INFINITE_V3' AND symbol='TQQQ'
        """)).scalar_one() == "legacy-unscoped-0052"
        assert conn.execute(text("""
            SELECT trading_epoch_id FROM kr_infinite_state
            WHERE strategy_id='KR_INFINITE_V1' AND symbol='122630'
        """)).scalar_one() == "legacy-unscoped-0052"
        assert conn.execute(text("""
            SELECT trading_epoch_id FROM kr_infinite_order_intents
            WHERE idempotency_key='legacy-intent'
        """)).scalar_one() == "legacy-unscoped-0052"

    epoch_id = start_new_trading_epoch(
        engine,
        env="practice",
        account_id="practice:test",
        reason="TEST_CLEAN_GENERATION",
    )
    assert active_trading_epoch_id(
        engine, env="practice", account_id="practice:test"
    ) == epoch_id

    with engine.begin() as conn:
        assert conn.execute(text("""
            SELECT status FROM portfolio_epochs
            WHERE portfolio_epoch_id='old-portfolio'
        """)).scalar_one() == "ENDED"
        assert conn.execute(text("""
            SELECT status FROM positions
            WHERE position_id='old-position'
        """)).scalar_one() == "CLOSED"

        # Same strategy/symbol can start clean in the new generation.
        conn.execute(text("""
            INSERT INTO us_tqqq_infinite_state(
                trading_epoch_id,strategy_id,symbol,status
            ) VALUES (:epoch,'TQQQ_INFINITE_V3','TQQQ','IDLE')
        """), {"epoch": epoch_id})
        conn.execute(text("""
            INSERT INTO kr_infinite_state(
                trading_epoch_id,strategy_id,symbol,status
            ) VALUES (:epoch,'KR_INFINITE_V1','122630','IDLE')
        """), {"epoch": epoch_id})

        # Same-day position snapshots are isolated by epoch rather than forcing
        # deletion of the legacy snapshot.
        conn.execute(text("""
            INSERT INTO us_positions(
                trading_epoch_id,as_of,symbol,exchange,qty
            ) VALUES (:epoch,'2026-09-21','AAPL','NASDAQ',0)
        """), {"epoch": epoch_id})

        tqqq_rows = conn.execute(text("""
            SELECT trading_epoch_id,status FROM us_tqqq_infinite_state
            WHERE strategy_id='TQQQ_INFINITE_V3' AND symbol='TQQQ'
            ORDER BY trading_epoch_id
        """)).all()
        assert len(tqqq_rows) == 2
        assert {row[0] for row in tqqq_rows} == {"legacy-unscoped-0052", epoch_id}

        position_rows = conn.execute(text("""
            SELECT trading_epoch_id,qty FROM us_positions
            WHERE as_of='2026-09-21' AND symbol='AAPL' AND exchange='NASDAQ'
        """)).all()
        assert len(position_rows) == 2
