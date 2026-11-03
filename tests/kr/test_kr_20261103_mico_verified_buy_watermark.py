"""Strict 11/03 Mico KIS BUY watermark certification, no broker/network I/O."""
from copy import deepcopy
from datetime import datetime, timezone
from uuid import uuid4

import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.repos import OrdersRepo, PositionsRepo, FillsRepo
from trader.db.schema import schema_for_engine
from trader.kr.verified_buy_watermark import certify_existing_pb1_buy_watermark
from tests.kr.execution_claim_fixtures import create_schema_with_active_test_epoch


def _fixture(*, phantom=False):
    engine = sa.create_engine("sqlite:///:memory:")
    schema = create_schema_with_active_test_epoch(engine)
    order_id, created = OrdersRepo(engine).create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close",
        sid=1, mode=1, code="059090", market="KOSDAQ",
        side="BUY", ord_type="LIMIT", qty=91, limit_price=21900,
        stage="PB1-AM", client_order_key="mico-certify-" + uuid4().hex,
        status="FILLED", account_id=get_account_key(env="practice"),
        request_json={"pre_order_holding_qty": 0}, position_cycle_id=str(uuid4()),
    )
    assert created
    with engine.begin() as conn:
        conn.execute(sa.update(schema.orders).where(schema.orders.c.order_id == order_id).values(
            created_at=datetime(2026, 11, 3, 0, 32, 13, tzinfo=timezone.utc),
            kis_odno="0000011762", broker_order_id="0000011762",
            response_json={"confirmed_fill_qty": 91,
                           "confirmed_fill_notional": 1982400},
        ))
    with engine.connect() as conn:
        order = dict(conn.execute(sa.select(schema.orders).where(
            schema.orders.c.order_id == order_id,
        )).mappings().one())
    PositionsRepo(engine).apply_fill(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="059090", market="KOSDAQ", side="BUY",
        qty=113 if phantom else 91, price=1982400 / (113 if phantom else 91),
        fee=0, tax=0, filled_at=datetime(2026,11,3,0,32,tzinfo=timezone.utc),
        portfolio_epoch_id=str(order["portfolio_epoch_id"]),
        position_cycle_id=str(order["position_cycle_id"]),
        order_id=str(order_id),
    )
    FillsRepo(engine).upsert_fill(
        env="practice", run_id=None, order_id=str(order_id),
        kis_odno="0000011762", trade_id="KR_KIS_CUM:ORIGINAL_MICO_91",
        code="059090", market="KOSDAQ", side="BUY", qty=91,
        price=1982400 / 91, fee=0, tax=0,
        filled_at=datetime(2026,11,3,0,32,tzinfo=timezone.utc),
        raw_json={"kis_response": {
            "ord_dt": "20261103", "pdno": "059090",
            "odno": "0000011762", "tot_ccld_qty": "91",
            "tot_ccld_amt": "1982400",
        }},
        fill_meta_json={"evidence_kind": "KIS_ORDER_CUMULATIVE_EXECUTION"},
        position_cycle_id=str(order["position_cycle_id"]),
        portfolio_epoch_id=str(order["portfolio_epoch_id"]),
    )
    balance = {"rt_cd": "0", "output1": [
        {"pdno": "059090", "hldg_qty": "91", "pchs_amt": "1982400"}
    ]}
    ccld = {"rt_cd": "0", "output1": [{
        "ord_dt": "20261103", "pdno": "059090",
        "odno": "0000011762", "sll_buy_dvsn_cd": "02",
        "tot_ccld_qty": "91", "tot_ccld_amt": "1982400",
        "rmn_qty": "0", "cncl_yn": "N",
    }]}
    return engine, schema, str(order_id), balance, ccld


def _certify(engine, order_id, balance, ccld, *, apply=False, source="api"):
    return certify_existing_pb1_buy_watermark(
        engine=engine, order_id=order_id, kis_balance=balance,
        balance_source=source, original_day_ccld=ccld, apply=apply,
    )


def test_exact_original_mico_certifies_metadata_only_and_replays_idempotently():
    engine, schema, order_id, balance, ccld = _fixture()
    assert _certify(engine, order_id, balance, ccld)["state"] == "READY_TO_CERTIFY"
    with engine.connect() as conn:
        before = dict(conn.execute(sa.select(schema.positions)).mappings().one())
        assert not (before["entry_meta_json"] or {}).get("broker_truth_buy_applied_orders")
    done = _certify(engine, order_id, balance, ccld, apply=True)
    assert done["state"] == "CERTIFIED" and done["applied"]
    assert _certify(engine, order_id, balance, ccld, apply=True)["state"] == "ALREADY_CERTIFIED"
    with engine.connect() as conn:
        after = dict(conn.execute(sa.select(schema.positions)).mappings().one())
    assert after["qty"] == before["qty"] == 91
    assert after["total_cost"] == before["total_cost"] == 1982400
    assert after["realized_pnl"] == before["realized_pnl"]
    assert after["entry_meta_json"]["broker_truth_buy_applied_orders"][order_id]["qty"] == 91


def test_phantom_113_cannot_be_certified_as_mico_91():
    engine, schema, order_id, balance, ccld = _fixture(phantom=True)
    result = _certify(engine, order_id, balance, ccld, apply=True)
    assert result["state"] == "BLOCKED"
    assert result["reason"] == "OPEN_POSITION_CYCLE_OR_ECONOMICS_MISMATCH"


def test_no_original_broker_price_or_stale_balance_never_certifies():
    engine, schema, order_id, balance, ccld = _fixture()
    for bad_balance, source, bad_ccld in (
        (balance, "wrapper_cache", ccld),
        (balance, "api", {"rt_cd": "1", "output1": []}),
        ({"rt_cd": "0", "output1": [{"pdno": "059090", "hldg_qty": "91",
                                   "pchs_amt": "0"}]}, "api", ccld),
        (balance, "api", {"rt_cd": "0", "output1": [{
            **ccld["output1"][0], "tot_ccld_amt": "1982500",
        }]}),
    ):
        assert _certify(engine, order_id, bad_balance, bad_ccld,
                        apply=True, source=source)["state"] == "BLOCKED"
    with engine.connect() as conn:
        position = dict(conn.execute(sa.select(schema.positions)).mappings().one())
    assert not (position["entry_meta_json"] or {}).get("broker_truth_buy_applied_orders")


def test_other_cycle_order_cannot_be_hidden_inside_certification():
    engine, schema, order_id, balance, ccld = _fixture()
    with engine.connect() as conn:
        order = dict(conn.execute(sa.select(schema.orders)).mappings().one())
    with engine.begin() as conn:
        conn.execute(sa.insert(schema.orders).values(
            order_id=str(uuid4()), env="practice", strategy="pb1_pullback_close",
            sid=1, mode=1, code="059090", market="KOSDAQ",
            side="SELL", ord_type="LIMIT", qty=5, stage="EXIT",
            client_order_key="unexpected-same-cycle-sell", status="ACKED",
            position_cycle_id=order["position_cycle_id"],
            portfolio_epoch_id=order["portfolio_epoch_id"],
            trading_epoch_id=order["trading_epoch_id"],
        ))
    result = _certify(engine, order_id, balance, ccld, apply=True)
    assert result == {"state": "BLOCKED",
                      "reason": "OTHER_SAME_CYCLE_ORDER_PRESENT", "applied": False}
