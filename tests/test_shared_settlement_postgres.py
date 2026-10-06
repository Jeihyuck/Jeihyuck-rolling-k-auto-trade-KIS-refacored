"""Actual PostgreSQL 16 transactional/row-lock regression for the shared ledger.

Runs only on a disposable CI PostgreSQL DB; never connects to operating Supabase.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date
from decimal import Decimal
import os
import uuid

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from trader.settlement.core import SettlementObservation, settle_atomic
from trader.settlement.schema import metadata


@pytest.fixture
def pg_ledger():
    url = os.getenv("PBCORE_TEST_POSTGRES_URL") or os.getenv("SETTLEMENT_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("isolated PostgreSQL required; no test URL configured")
    engine = sa.create_engine(url, pool_size=8, max_overflow=4)
    if engine.dialect.name != "postgresql":
        pytest.fail("PostgreSQL-only acceptance test requires PostgreSQL engine")
    metadata.create_all(engine)
    projection = sa.Table(
        "test_settlement_projection", sa.MetaData(),
        sa.Column("settlement_key", sa.Text, primary_key=True),
        sa.Column("qty", sa.Integer, nullable=False),
        sa.Column("notional", sa.Numeric(24, 8), nullable=False),
    )
    projection.create(engine, checkfirst=True)
    try:
        yield engine, projection
    finally:
        projection.drop(engine, checkfirst=True)
        metadata.drop_all(engine)
        engine.dispose()


def _obs(key, market="KR", qty=5, price=Decimal("99.25")):
    return SettlementObservation(
        env="practice", market=market, account_scope="pg-test-account",
        trading_epoch_id="pg-test-epoch", strategy_owner="KR_STANDARD" if market == "KR" else "US_STANDARD",
        position_cycle_id="cycle-" + key, client_order_key=key,
        broker_trade_date=date(2026, 10, 6),
        exchange="KRX" if market == "KR" else "NASD",
        broker_order_no="1234567", side="BUY", requested_qty=qty,
        cumulative_qty=qty, evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        evidence_digest="sha-" + key, currency="KRW" if market == "KR" else "USD",
        execution_price=price,
    )


def _projection_callback(table):
    def apply(conn, obs, decision):
        values = {
            "settlement_key": decision.settlement_key,
            "qty": decision.qty_delta,
            "notional": decision.notional_delta,
        }
        conn.execute(
            pg_insert(table).values(**values).on_conflict_do_update(
                index_elements=[table.c.settlement_key],
                set_={
                    "qty": table.c.qty + decision.qty_delta,
                    "notional": table.c.notional + decision.notional_delta,
                },
            )
        )
    return apply


@pytest.mark.parametrize("market", ["KR", "US"])
def test_concurrent_broker_observations_apply_only_once(pg_ledger, market):
    engine, table = pg_ledger
    obs = _obs(str(uuid.uuid4()), market=market)
    apply = _projection_callback(table)
    with ThreadPoolExecutor(max_workers=8) as pool:
        decisions = list(pool.map(lambda _: settle_atomic(engine, obs, apply), range(16)))
    assert sum(d.qty_delta for d in decisions) == 5
    assert sum(d.notional_delta for d in decisions) == Decimal("496.25")
    with engine.connect() as conn:
        row = conn.execute(sa.select(table).where(
            table.c.settlement_key == decisions[0].settlement_key
        )).mappings().one()
        assert row["qty"] == 5
        assert row["notional"] == Decimal("496.25")


def test_crash_between_economic_update_and_watermark_rolls_back(pg_ledger):
    engine, table = pg_ledger
    obs = _obs(str(uuid.uuid4()))
    apply = _projection_callback(table)
    def crash(conn, observation, decision):
        apply(conn, observation, decision)
        raise RuntimeError("fault-injection-after-economic-update")
    with pytest.raises(RuntimeError, match="fault-injection"):
        settle_atomic(engine, obs, crash)
    with engine.connect() as conn:
        assert conn.execute(sa.select(sa.func.count()).select_from(table)).scalar_one() == 0
    decision = settle_atomic(engine, obs, apply)
    assert decision.qty_delta == 5


def test_delayed_execution_price_replay_never_decrements_quantity(pg_ledger):
    engine, table = pg_ledger
    original = _obs(str(uuid.uuid4()), market="US", qty=3, price=None)
    pending = replace(
        original, evidence_type="HOLDINGS_DELTA_EXCLUSIVE",
        exclusive_order_proof=True, pre_holding_qty=0, post_holding_qty=3,
    )
    apply = _projection_callback(table)
    assert settle_atomic(engine, pending, apply).qty_delta == 3
    priced = replace(
        original, evidence_digest="actual-broker-price-" + original.client_order_key,
        evidence_type="KIS_EXECUTION_ACTUAL", execution_price=Decimal("101.55"),
        execution_qty=3,
    )
    for _ in range(3):
        assert settle_atomic(engine, priced, apply).qty_delta == 0
    with engine.connect() as conn:
        row = conn.execute(sa.select(table)).mappings().one()
        assert row["qty"] == 3
        assert row["notional"] == Decimal("304.65")


def test_raw_migration_0055_can_create_pg_schema_and_rerun(pg_ledger):
    """Execute the shipped SQL, not merely SQLAlchemy create_all metadata."""
    from pathlib import Path
    engine, _projection = pg_ledger
    ddl = Path("migrations/0055_shared_settlement_ledger.sql").read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(sa.text("DROP TABLE broker_settlement_evidence"))
        conn.execute(sa.text("DROP TABLE settlement_applications"))
    # Migration SQL may contain semicolons inside whole-line comments.
    # Remove comments before splitting; otherwise comment fragments become SQL.
    ddl_no_comments = "\n".join(
        line for line in ddl.splitlines() if not line.lstrip().startswith("--")
    )
    statements = [part.strip() for part in ddl_no_comments.split(";") if part.strip()]
    assert len(statements) >= 4
    for _ in range(2):
        with engine.begin() as conn:
            for sql in statements:
                conn.execute(sa.text(sql))
    with engine.connect() as conn:
        columns = {row[0] for row in conn.execute(sa.text(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='public' "
            "AND table_name='settlement_applications'"
        ))}
    assert {"settlement_key", "requested_qty", "applied_qty", "applied_notional", "priced_qty", "price_status"} <= columns
    decision = settle_atomic(engine, _obs(str(uuid.uuid4())), _projection_callback(_projection))
    assert decision.qty_delta == 5
