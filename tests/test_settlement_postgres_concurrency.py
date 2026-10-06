"""Integration: same KIS execution concurrently settled by two workers.

Requires PBCORE_TEST_POSTGRES_URL pointing ONLY to an isolated CI database.
No live KIS orders, Supabase writes, or trading state mutations.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date
from decimal import Decimal
import os
import uuid

import pytest
import sqlalchemy as sa

from trader.settlement.core import SettlementObservation, settle_atomic
from trader.settlement.schema import metadata


def test_postgres_concurrent_same_order_applies_once():
    url = os.getenv("PBCORE_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("isolated PostgreSQL test URL not configured")
    if "localhost" not in url and "127.0.0.1" not in url:
        pytest.skip("refuse to run concurrency test outside localhost CI")
    engine = sa.create_engine(url, pool_size=5, max_overflow=2)
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS test_settlement_concurrent_position (
                test_key TEXT PRIMARY KEY, qty INTEGER NOT NULL
            )
        """))
    key = "ci-" + str(uuid.uuid4())
    observation = SettlementObservation(
        env="practice", market="US", account_scope="fake-account-hash",
        trading_epoch_id=key, strategy_owner="US_STANDARD",
        position_cycle_id=key, client_order_key=key,
        broker_trade_date=date(2026, 10, 6), exchange="NASD",
        broker_order_no="01001001", side="BUY", requested_qty=3,
        cumulative_qty=3, evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        evidence_digest=key, currency="USD", execution_price=Decimal("12"),
    )
    def apply(conn, obs, decision):
        conn.execute(sa.text("""
            INSERT INTO test_settlement_concurrent_position(test_key, qty)
            VALUES(:key,:qty)
            ON CONFLICT(test_key) DO UPDATE SET
            qty=test_settlement_concurrent_position.qty+EXCLUDED.qty
        """), {"key": key, "qty": decision.qty_delta})
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: settle_atomic(engine, observation, apply), range(2)))
    assert sorted(r.qty_delta for r in results) == [0, 3]
    with engine.connect() as conn:
        assert conn.execute(sa.text(
            "SELECT qty FROM test_settlement_concurrent_position WHERE test_key=:key"
        ), {"key": key}).scalar() == 3
        assert conn.execute(sa.text(
            "SELECT applied_qty FROM settlement_applications WHERE settlement_key=:key"
        ), {"key": results[0].settlement_key}).scalar() == 3
