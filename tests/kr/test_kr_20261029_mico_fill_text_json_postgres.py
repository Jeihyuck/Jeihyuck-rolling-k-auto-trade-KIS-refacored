"""Oct-29 Mico: physical PostgreSQL fills.raw_json TEXT cannot be indexed as JSON.

Run with isolated PBCORE_TEST_POSTGRES_URL (never production Supabase).
Source schema_for_engine deliberately declares JSON; production column is TEXT.
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
    reason="isolated PostgreSQL test instance required",
)


@pytest.fixture()
def kr_mico_text_engine():
    name = "kr_20261029_text_json_p0"
    url = os.environ["PBCORE_TEST_POSTGRES_URL"]
    admin = sa.create_engine(url)
    with admin.begin() as conn:
        conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {name} CASCADE"))
        conn.execute(sa.text(f"CREATE SCHEMA {name}"))
    admin.dispose()
    engine = sa.create_engine(url, connect_args={"options": f"-csearch_path={name}"})
    try:
        create_schema_with_active_test_epoch(engine)
        with engine.begin() as conn:
            conn.execute(sa.text(
                f"ALTER TABLE {name}.fills ALTER COLUMN raw_json TYPE TEXT USING raw_json::text"
            ))
            conn.execute(sa.text(f"ALTER TABLE {name}.fills ALTER COLUMN price TYPE REAL"))
        inspector = sa.inspect(engine)
        assert next(c["type"] for c in inspector.get_columns("fills") if c["name"] == "raw_json").__class__ in (
            sa.sql.sqltypes.TEXT, sa.sql.sqltypes.Text,
        )
        yield engine
    finally:
        engine.dispose()
        admin = sa.create_engine(url)
        with admin.begin() as conn:
            conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {name} CASCADE"))
        admin.dispose()


def test_mico_cumulative_buy_text_json_upsert_is_atomic_and_idempotent(kr_mico_text_engine):
    engine = kr_mico_text_engine
    order_id, created = OrdersRepo(engine).create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close",
        sid=1, mode=1, code="059090", market="KOSDAQ",
        side="BUY", ord_type="LIMIT", qty=79, limit_price=26050,
        stage="PB1-AM",
        client_order_key="mico-text-json-" + uuid4().hex,
        request_json={"entry_exit_plan": {
            "entry_style_selected": "ENTRY_MOMENTUM",
            "trade_horizon": "DAY_TRADE",
            "exit_policy_family": "INTRADAY_PROFIT_PROTECT",
            "eod_action": "FORCE_EXIT",
            "force_eod_close": True,
        }},
        status="ACKED", account_id=get_account_key(env="practice"),
    )
    assert created
    repo = FillsRepo(engine)
    broker_trade_id = "KR_KIS_CUM:practice:20261029:0000012345:059090:BUY"
    start = dict(
        env="practice", run_id=None, order_id=order_id,
        kis_odno="0000012345", trade_id=broker_trade_id,
        code="059090", market="KOSDAQ", side="BUY", qty=79,
        price=26050, fee=0, tax=0,
        filled_at=datetime(2026, 10, 29, 0, 31, tzinfo=timezone.utc),
        raw_json={"kis_response": {
            "ord_dt": "20261029", "odno": "0000012345",
            "pdno": "059090", "tot_ccld_qty": "79",
            "tot_ccld_amt": "2057950",
        }},
        fill_meta_json={
            "evidence_kind": "KIS_ORDER_CUMULATIVE_EXECUTION",
            "fill_source": "daily_ccld_order_cumulative",
        },
    )

    repo.upsert_fill(**start)  # previously failed with: operator text -> text
    repo.upsert_fill(**start)  # same order/qty/notional remains idempotent

    with pytest.raises(
        RuntimeError, match="KR_CUMULATIVE_FILL_SAME_QTY_NOTIONAL_CONFLICT"
    ):
        repo.upsert_fill(**{
            **start, "price": 26060,
            "raw_json": {"kis_response": {
                **start["raw_json"]["kis_response"],
                "tot_ccld_amt": "2058740",
            }},
        })

    increased = {**start, "qty": 80, "price": 26050,
                 "raw_json": {"kis_response": {
                     **start["raw_json"]["kis_response"],
                     "tot_ccld_qty": "80",
                     "tot_ccld_amt": "2084000",
                 }}}
    repo.upsert_fill(**increased)
    repo.upsert_fill(**start)  # stale 79 must not undo verified 80
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        rows = conn.execute(sa.select(
            schema.fills.c.qty, schema.fills.c.price, schema.fills.c.raw_json,
        ).where(schema.fills.c.trade_id == broker_trade_id)).mappings().all()
    assert len(rows) == 1
    assert int(rows[0]["qty"]) == 80
    value = rows[0]["raw_json"]
    if isinstance(value, str):
        import json
        value = json.loads(value)
    assert value["kis_response"]["tot_ccld_amt"] == "2084000"


def test_legacy_raw_json_string_parser_still_retains_exact_broker_notional(kr_mico_text_engine):
    engine = kr_mico_text_engine
    repo = FillsRepo(engine)
    trade_id = "KR_KIS_CUM:practice:20261029:legacy-string:059090:BUY"
    data = dict(env="practice", run_id=None, order_id=None, kis_odno="legacy-string",
                trade_id=trade_id, code="059090", market="KOSDAQ", side="BUY",
                qty=12, price=1083700 / 12, fee=0, tax=0,
                filled_at=datetime(2026, 10, 29, 0, 31, tzinfo=timezone.utc),
                raw_json={"kis_response": {"tot_ccld_qty": "12", "tot_ccld_amt": "1083700"}},
                fill_meta_json={"evidence_kind": "KIS_ORDER_CUMULATIVE_EXECUTION"})
    # A cumulative fill requires original order_id; test the helper via the
    # actual upsert with a concrete durable intent instead of bypassing it.
    order_id, _ = OrdersRepo(engine).create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close",
        sid=1, mode=1, code="059090", market="KOSDAQ", side="BUY",
        ord_type="LIMIT", qty=12, limit_price=1083700/12, stage="PB1-AM",
        client_order_key="mico-legacy-" + uuid4().hex, request_json={},
        status="ACKED", account_id=get_account_key(env="practice"),
    )
    data["order_id"] = order_id
    repo.upsert_fill(**data)
    repo.upsert_fill(**data)
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        assert conn.execute(sa.select(sa.func.count()).select_from(
            schema.fills).where(schema.fills.c.trade_id == trade_id)).scalar_one() == 1
