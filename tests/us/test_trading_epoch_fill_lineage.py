from __future__ import annotations

import os
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy import text

from trader.db.migrate import split_postgres_sql


URL = os.getenv("PBCORE_TEST_POSTGRES_URL")
pytestmark = pytest.mark.skipif(not URL, reason="real PostgreSQL integration URL not configured")


@pytest.fixture()
def epoch_pg(monkeypatch):
    from trader.us.db import repos

    admin = sa.create_engine(URL, future=True)
    schema_name = "us_epoch_lineage_pr138"
    with admin.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {schema_name}"))
    admin.dispose()

    engine = sa.create_engine(
        URL,
        future=True,
        connect_args={"options": f"-csearch_path={schema_name},public"},
    )
    try:
        with engine.begin() as conn:
            for migration in (
                "migrations/0038_us_agent_tables.sql",
                "migrations/0043_us_fills_idempotency_and_order_reconcile_fix.sql",
                "migrations/0046_us_orders_committed_notional.sql",
                "migrations/0047_us_order_events_profit_lifecycle.sql",
            ):
                conn.exec_driver_sql(Path(migration).read_text(encoding="utf-8"))

            # Apply the exact US epoch-column statements from production 0052.
            migration_0052 = Path("migrations/0052_unified_trading_epoch.sql").read_text(encoding="utf-8")
            wanted_prefixes = (
                "ALTER TABLE us_order_intents ADD COLUMN IF NOT EXISTS trading_epoch_id",
                "ALTER TABLE us_orders ADD COLUMN IF NOT EXISTS trading_epoch_id",
                "ALTER TABLE us_fills ADD COLUMN IF NOT EXISTS trading_epoch_id",
                "ALTER TABLE us_positions ADD COLUMN IF NOT EXISTS trading_epoch_id",
                "ALTER TABLE us_order_events ADD COLUMN IF NOT EXISTS trading_epoch_id",
                "ALTER TABLE us_profit_capture_lifecycle ADD COLUMN IF NOT EXISTS trading_epoch_id",
                "CREATE INDEX IF NOT EXISTS ix_us_order_intents_trading_epoch",
                "CREATE INDEX IF NOT EXISTS ix_us_orders_trading_epoch",
                "CREATE INDEX IF NOT EXISTS ix_us_fills_trading_epoch",
            )
            for statement in split_postgres_sql(migration_0052):
                normalized = statement.strip()
                if normalized.startswith(wanted_prefixes):
                    conn.exec_driver_sql(normalized)

        monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
        monkeypatch.setattr(
            repos,
            "_active_us_epoch",
            lambda bind=None, required=None: "epoch-new",
        )
        yield engine
    finally:
        engine.dispose()
        cleanup = sa.create_engine(URL, future=True)
        with cleanup.begin() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
        cleanup.dispose()


def _insert_order(engine, *, key: str, order_no: str, qty_filled: int = 0):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO us_orders(
                trade_date,client_order_key,symbol,exchange,side,
                qty_requested,qty_filled,avg_price_usd,order_no,status,
                dry_run,env,trading_epoch_id,meta
            ) VALUES (
                '2026-09-22',:key,'AAPL','NASDAQ','BUY',
                10,:qty_filled,NULL,:order_no,'ACK',
                false,'practice','epoch-new',
                '{"trading_epoch_id":"epoch-new"}'::jsonb
            )
        """), {"key": key, "order_no": order_no, "qty_filled": qty_filled})


def _insert_fill(
    engine,
    *,
    epoch: str | None,
    idem: str,
    order_no: str,
    key: str,
    qty: int,
    synthetic: bool,
    active: bool = True,
):
    import json

    meta = {
        "fill_evidence_type": (
            "BALANCE_DELTA_SYNTHETIC" if synthetic
            else "KIS_ORDER_CUMULATIVE_ACTUAL"
        ),
        "is_synthetic": synthetic,
        "accounting_active": active,
        "cumulative_filled_qty": qty,
    }
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO us_fills(
                trade_date,symbol,exchange,side,qty,price_usd,
                order_no,client_order_key,filled_at,trading_epoch_id,
                meta,fill_idempotency_key
            ) VALUES (
                '2026-09-22','AAPL','NASDAQ','BUY',:qty,100,
                :order_no,:key,NOW(),:epoch,CAST(:meta AS jsonb),:idem
            )
        """), {
            "qty": qty, "order_no": order_no, "key": key,
            "epoch": epoch, "meta": json.dumps(meta), "idem": idem,
        })


def test_us_reconcile_ignores_previous_epoch_fill_and_persists_current_epoch(epoch_pg):
    from trader.us.db import repos

    _insert_order(epoch_pg, key="NEW-ORDER", order_no="BROKER-1")
    _insert_fill(
        epoch_pg, epoch="epoch-old", idem="old-fill",
        order_no="BROKER-1", key="OLD-ORDER", qty=9, synthetic=False,
    )

    result = repos.mark_order_filled_by_reconcile(
        order_no="BROKER-1", client_order_key="NEW-ORDER",
        symbol="AAPL", side="BUY", filled_qty=2,
        requested_qty=10, cumulative_filled_qty=2, avg_price_usd=101,
        trade_date="2026-09-22",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        source="fills_by_order_no",
    )
    assert result["status"] == "OK"
    assert result["qty_filled"] == 2

    with epoch_pg.connect() as conn:
        order_qty = conn.execute(text("""
            SELECT qty_filled FROM us_orders
            WHERE client_order_key='NEW-ORDER'
              AND trading_epoch_id='epoch-new'
        """)).scalar_one()
        current = conn.execute(text("""
            SELECT qty,trading_epoch_id FROM us_fills
            WHERE order_no='BROKER-1' AND trading_epoch_id='epoch-new'
        """)).mappings().one()
        old = conn.execute(text("""
            SELECT qty,trading_epoch_id FROM us_fills
            WHERE fill_idempotency_key='old-fill'
        """)).mappings().one()
    assert order_qty == 2
    assert current["qty"] == 2
    assert current["trading_epoch_id"] == "epoch-new"
    assert old["qty"] == 9 and old["trading_epoch_id"] == "epoch-old"
    assert repos.verify_order_fill_accounting(
        trade_date="2026-09-22", order_no="BROKER-1"
    )["status"] == "OK"


def test_us_actual_supersedes_only_same_epoch_synthetic_fill(epoch_pg):
    from trader.us.db import repos

    _insert_order(epoch_pg, key="PROMOTE-ORDER", order_no="BROKER-2", qty_filled=3)
    _insert_fill(
        epoch_pg, epoch="epoch-new", idem="new-synthetic",
        order_no="BROKER-2", key="PROMOTE-ORDER", qty=3, synthetic=True,
    )
    _insert_fill(
        epoch_pg, epoch="epoch-old", idem="old-synthetic",
        order_no="BROKER-2", key="OLD-PROMOTE", qty=7, synthetic=True,
    )

    result = repos.mark_order_filled_by_reconcile(
        order_no="BROKER-2", client_order_key="PROMOTE-ORDER",
        symbol="AAPL", side="BUY", filled_qty=5,
        requested_qty=10, cumulative_filled_qty=5, avg_price_usd=102,
        trade_date="2026-09-22",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        source="fills_by_order_no",
    )
    assert result["status"] == "OK"
    assert result["synthetic_superseded_count"] == 1

    with epoch_pg.connect() as conn:
        current_active = conn.execute(text("""
            SELECT COALESCE((meta->>'accounting_active')::boolean,true)
            FROM us_fills WHERE fill_idempotency_key='new-synthetic'
        """)).scalar_one()
        old_active = conn.execute(text("""
            SELECT COALESCE((meta->>'accounting_active')::boolean,true)
            FROM us_fills WHERE fill_idempotency_key='old-synthetic'
        """)).scalar_one()
        actual = conn.execute(text("""
            SELECT qty,trading_epoch_id FROM us_fills
            WHERE order_no='BROKER-2'
              AND trading_epoch_id='epoch-new'
              AND NOT COALESCE((meta->>'is_synthetic')::boolean,false)
        """)).mappings().one()
    assert current_active is False
    assert old_active is True
    assert actual["qty"] == 5
    assert actual["trading_epoch_id"] == "epoch-new"


def test_us_legacy_order_key_cannot_be_adopted_into_active_epoch(epoch_pg):
    from trader.us.db import repos

    with epoch_pg.begin() as conn:
        conn.execute(text("""
            INSERT INTO us_orders(
                trade_date,client_order_key,symbol,exchange,side,
                qty_requested,qty_filled,order_no,status,dry_run,env,meta
            ) VALUES (
                '2026-09-22','REUSED-KEY','AAPL','NASDAQ','BUY',
                1,0,'OLD-BROKER','ACK',false,'practice',
                '{"legacy":true}'::jsonb
            )
        """))

    saved = repos.save_order_ack({
        "client_order_key": "REUSED-KEY",
        "symbol": "AAPL", "exchange": "NASDAQ", "side": "BUY",
        "qty_requested": 1, "qty_filled": 0,
        "order_no": "NEW-BROKER", "status": "ACK",
        "env": "practice", "meta": {},
    }, trade_date="2026-09-22")
    assert saved is False

    with epoch_pg.connect() as conn:
        row = conn.execute(text("""
            SELECT order_no,trading_epoch_id,meta
            FROM us_orders WHERE client_order_key='REUSED-KEY'
        """)).mappings().one()
    assert row["order_no"] == "OLD-BROKER"
    assert row["trading_epoch_id"] is None
    assert row["meta"]["legacy"] is True
