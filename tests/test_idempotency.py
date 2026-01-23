import os
import sys
from pathlib import Path

import sqlalchemy as sa
from datetime import datetime
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from trader.db.migrate import run_migrations
from trader.db.repos import OrdersRepo, FillsRepo
from trader.db.schema import ORDERS, FILLS


def test_orders_upsert_idempotent(tmp_path):
    db_url = os.getenv("PBCORE_DB_URL")
    if not db_url:
        pytest.skip("PBCORE_DB_URL not set for Postgres-backed test")
    engine = sa.create_engine(db_url)
    run_migrations(engine)

    repo = OrdersRepo(engine)
    repo.upsert_reconciled_order(
        env="practice",
        run_id=None,
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="005930",
        market="KOSPI",
        side="BUY",
        ord_type="RECONCILED",
        qty=1,
        limit_price=100.0,
        stage="RECONCILE",
        client_order_key="practice:pb1:000",
        kis_odno="KIS123",
        status="ACKED",
        request_json={},
        response_json={},
        submitted_at=None,
        acked_at=None,
    )
    repo.upsert_reconciled_order(
        env="practice",
        run_id=None,
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="005930",
        market="KOSPI",
        side="BUY",
        ord_type="RECONCILED",
        qty=1,
        limit_price=100.0,
        stage="RECONCILE",
        client_order_key="practice:pb1:000",
        kis_odno="KIS123",
        status="ACKED",
        request_json={},
        response_json={},
        submitted_at=None,
        acked_at=None,
    )
    with engine.begin() as conn:
        count = conn.execute(sa.select(sa.func.count()).select_from(ORDERS)).scalar()
    assert count == 1


def test_fills_upsert_idempotent(tmp_path):
    db_url = os.getenv("PBCORE_DB_URL")
    if not db_url:
        pytest.skip("PBCORE_DB_URL not set for Postgres-backed test")
    engine = sa.create_engine(db_url)
    run_migrations(engine)

    repo = FillsRepo(engine)
    repo.upsert_fill(
        env="practice",
        run_id=None,
        order_id=None,
        kis_odno="KIS123",
        trade_id="TRD-1",
        code="005930",
        market="KOSPI",
        side="BUY",
        qty=1,
        price=100.0,
        fee=0.0,
        tax=0.0,
        filled_at=datetime.utcnow(),
        raw_json={},
    )
    repo.upsert_fill(
        env="practice",
        run_id=None,
        order_id=None,
        kis_odno="KIS123",
        trade_id="TRD-1",
        code="005930",
        market="KOSPI",
        side="BUY",
        qty=1,
        price=100.0,
        fee=0.0,
        tax=0.0,
        filled_at=datetime.utcnow(),
        raw_json={},
    )
    with engine.begin() as conn:
        count = conn.execute(sa.select(sa.func.count()).select_from(FILLS)).scalar()
    assert count == 1
