"""Exact 2026-10-23 053800 KIS notional on physical PostgreSQL REAL.

Runs against the isolated PostgreSQL service in settlement-contract-ci,
with KIS HTTP disabled. No operational database is read or modified.
"""
from __future__ import annotations

from datetime import datetime, timezone
import os
from uuid import uuid4

import pytest
import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.repos import FillsRepo, OrdersRepo
from trader.db.schema import schema_for_engine
from tests.kr.execution_claim_fixtures import create_schema_with_active_test_epoch


pytestmark = pytest.mark.skipif(
    not os.getenv("PBCORE_TEST_POSTGRES_URL"),
    reason="PostgreSQL isolated acceptance DB is not configured",
)

_TEST_SCHEMA = "kr_oct23_real_cumulative_ci"


@pytest.fixture()
def pg_kr_real_engine():
    url = os.environ["PBCORE_TEST_POSTGRES_URL"]
    admin = sa.create_engine(url)
    with admin.begin() as conn:
        conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {_TEST_SCHEMA} CASCADE"))
        conn.execute(sa.text(f"CREATE SCHEMA {_TEST_SCHEMA}"))
    admin.dispose()
    engine = sa.create_engine(
        url, connect_args={"options": f"-csearch_path={_TEST_SCHEMA}"},
    )
    try:
        create_schema_with_active_test_epoch(engine)
        with engine.begin() as conn:
            columns = {
                c["name"] for c in sa.inspect(conn).get_columns(
                    "fills", schema=_TEST_SCHEMA
                )
            }
            assert "price" in columns, "isolated test fixture did not create fills.price"
            conn.execute(sa.text(
                f"ALTER TABLE {_TEST_SCHEMA}.fills ALTER COLUMN price TYPE REAL"
            ))
        yield engine
    finally:
        engine.dispose()
        admin = sa.create_engine(url)
        with admin.begin() as conn:
            conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {_TEST_SCHEMA} CASCADE"))
        admin.dispose()


def test_original_kis_same_notional_and_monotonic_updates_on_real_postgres(
    pg_kr_real_engine,
):
    db = pg_kr_real_engine
    schema = schema_for_engine(db)
    orders = OrdersRepo(db)
    order_id, created = orders.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close",
        sid=1, mode=1, code="053800", market="KOSDAQ",
        side="SELL", ord_type="LIMIT", qty=13,
        limit_price=90308.33333333333, stage="FULL_EXIT",
        client_order_key="kr251-real-notional-" + uuid4().hex,
        request_json={},
        status="ACKED", account_id=get_account_key(env="practice"),
    )
    assert created
    repo = FillsRepo(db)
    evidence = {
        "ord_dt": "20261023", "odno": "0000012427",
        "pdno": "053800", "tot_ccld_qty": "12",
        "tot_ccld_amt": "1083700",
    }
    args = dict(
        env="practice", run_id=None, order_id=order_id,
        kis_odno="0000012427",
        trade_id="KR_KIS_CUM:kr251:20261023:0000012427:053800:SELL",
        code="053800", market="KOSDAQ", side="SELL",
        qty=12, price=1083700 / 12,
        fee=0, tax=0,
        filled_at=datetime(2026, 10, 23, 0, 36, tzinfo=timezone.utc),
        raw_json={"kis_response": evidence},
        fill_meta_json={
            "evidence_kind": "KIS_ORDER_CUMULATIVE_EXECUTION",
            "fill_source": "daily_ccld_order_cumulative",
        },
    )
    repo.upsert_fill(**args)
    with db.connect() as conn:
        row = conn.execute(sa.select(schema.fills).where(
            schema.fills.c.trade_id == args["trade_id"]
        )).mappings().one()
    assert abs(float(row["price"]) * 12 - 1083700) > 0.01

    # Crucially this reaches PostgreSQL ON CONFLICT WHERE, not only Python.
    repo.upsert_fill(**args)
    with db.connect() as conn:
        rows = conn.execute(sa.select(schema.fills).where(
            schema.fills.c.trade_id == args["trade_id"]
        )).mappings().all()
    assert len(rows) == 1
    assert rows[0]["qty"] == 12

    altered_evidence = {**evidence, "tot_ccld_amt": "1084700"}
    with pytest.raises(
        RuntimeError, match="KR_CUMULATIVE_FILL_SAME_QTY_NOTIONAL_CONFLICT",
    ):
        repo.upsert_fill(**{
            **args, "price": 1084700 / 12,
            "raw_json": {"kis_response": altered_evidence},
        })
    # Moving the broker's amount up with qty must remain legal.
    increased = {**evidence, "tot_ccld_qty": "13", "tot_ccld_amt": "1174000"}
    repo.upsert_fill(**{
        **args, "qty": 13, "price": 1174000 / 13,
        "raw_json": {"kis_response": increased},
    })
    repo.upsert_fill(**args)  # stale snapshot cannot regress to 12
    with db.connect() as conn:
        rows = conn.execute(sa.select(schema.fills).where(
            schema.fills.c.trade_id == args["trade_id"]
        )).mappings().all()
    assert len(rows) == 1
    assert rows[0]["qty"] == 13
    assert rows[0]["raw_json"]["kis_response"]["tot_ccld_amt"] == "1174000"
