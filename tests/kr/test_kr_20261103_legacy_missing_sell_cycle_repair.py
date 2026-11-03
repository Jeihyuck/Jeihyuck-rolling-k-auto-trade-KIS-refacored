"""No broker I/O: 122630 original BUY11 and SELL5+6 can only bind exact PB1 cycle."""
from datetime import datetime, timezone
from uuid import uuid4

import pytest
import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.repos import PositionsRepo
from trader.db.schema import schema_for_engine
from trader.db.trading_epoch import active_trading_epoch_id
from trader.kr.legacy_missing_cycle_repair import certify_legacy_pb1_missing_cycle
from tests.kr.execution_claim_fixtures import create_schema_with_active_test_epoch


def _fixture():
    engine = sa.create_engine("sqlite:///:memory:")
    schema = create_schema_with_active_test_epoch(engine)
    pos = PositionsRepo(engine).get_or_create_imported_cycle_for_kis_holding(
        env="practice", strategy="pb1_pullback_close",
        account_id=get_account_key(env="practice"), sid=1, mode=1,
        code="122630", market="J", qty=11, avg_price=740800/11,
    )[0]
    portfolio = str(pos["portfolio_epoch_id"])
    created = [
        datetime(2026,10,26,0,33,tzinfo=timezone.utc),
        datetime(2026,10,27,0,17,tzinfo=timezone.utc),
        datetime(2026,10,27,1,30,tzinfo=timezone.utc),
    ]
    data = [
        ("BUY",11,"0000011438","RECONCILED",created[0]),
        ("SELL",5,"0000008799","FILLED",created[1]),
        ("SELL",6,"0000017474","FILLED",created[2]),
    ]
    with engine.begin() as conn:
        epoch = active_trading_epoch_id(
            conn,env="practice",account_id=get_account_key(env="practice"),required=True,
        )
        for ix,(side,qty,odno,status,when) in enumerate(data):
            conn.execute(sa.insert(schema.orders).values(
                order_id=str(uuid4()),env="practice",strategy="pb1_pullback_close",
                sid=1,mode=1,code="122630",market="J",side=side,
                ord_type="LIMIT",qty=qty,stage="PB1",status=status,
                client_order_key=f"legacy-pb1-122630-{ix}",
                portfolio_epoch_id=portfolio,trading_epoch_id=str(epoch),
                position_cycle_id=None,kis_odno=odno,broker_order_id=odno,
                created_at=when,submitted_at=when,
                request_json={"strategy_owner":"KR_STANDARD"},response_json={},
            ))
    pages = {
        "20261026":{"rt_cd":"0","_sep29_complete":True,"output1":[
            {"ord_dt":"20261026","pdno":"122630","odno":"0000011438",
             "sll_buy_dvsn_cd":"02","tot_ccld_qty":"11","tot_ccld_amt":"740800"},
        ]},
        "20261027":{"rt_cd":"0","_sep29_complete":True,"output1":[
            {"ord_dt":"20261027","pdno":"122630","odno":"0000008799",
             "sll_buy_dvsn_cd":"01","tot_ccld_qty":"5","tot_ccld_amt":"345000"},
            {"ord_dt":"20261027","pdno":"122630","odno":"0000017474",
             "sll_buy_dvsn_cd":"01","tot_ccld_qty":"6","tot_ccld_amt":"414000"},
        ]},
    }
    broker={"rt_cd":"0","output1":[]}
    return engine,schema,pos,pages,broker


def _call(engine,pages,broker,**kwargs):
    return certify_legacy_pb1_missing_cycle(
        engine=engine,code="122630",original_daily_pages=pages,
        broker_balance=broker,balance_source="api",**kwargs,
    )


def test_122630_original_kis_buy11_sell5_6_binds_only_cycle_metadata():
    engine,schema,pos,pages,broker=_fixture()
    before=_call(engine,pages,broker)
    assert before["state"]=="READY_TO_BIND"
    assert before["buy_qty"]==before["sell_qty"]==11
    assert before["updated_orders"]==0
    with engine.connect() as conn:
        unlinked=conn.execute(sa.select(sa.func.count()).select_from(schema.orders).where(
            schema.orders.c.position_cycle_id.is_(None)
        )).scalar_one()
    assert unlinked==3
    after=_call(engine,pages,broker,apply=True)
    assert after["state"]=="BOUND" and after["updated_orders"]==3
    with engine.connect() as conn:
        orders=conn.execute(sa.select(schema.orders)).mappings().all()
        position=conn.execute(sa.select(schema.positions)).mappings().one()
        fills=conn.execute(sa.select(sa.func.count()).select_from(schema.fills)).scalar_one()
    assert all(str(o["position_cycle_id"])==str(pos["position_cycle_id"]) for o in orders)
    assert position["qty"]==11 and position["status"]=="OPEN"
    assert fills==0  # Do NOT infer an original-day execution from a cycle link.
    assert _call(engine,pages,broker,apply=True)["state"]=="BLOCKED"


@pytest.mark.parametrize("change,reason",[
    ("missing_page","ORIGINAL_DAY_ALL_PAGES_UNVERIFIED"),
    ("wrong_side","ORIGINAL_ORDER_KIS_MATCH_NOT_UNIQUE"),
    ("wrong_sell_qty","ORIGINAL_KIS_FULL_FILL_NOT_PROVEN"),
    ("broker_nonzero","BROKER_HOLDING_NONZERO"),
])
def test_legacy_missing_cycle_fail_closed_without_mutations(change,reason):
    engine,schema,pos,pages,broker=_fixture()
    if change=="missing_page":
        pages["20261027"]["_sep29_complete"]=False
    elif change=="wrong_side":
        pages["20261027"]["output1"][0]["sll_buy_dvsn_cd"]="02"
    elif change=="wrong_sell_qty":
        pages["20261027"]["output1"][0]["tot_ccld_qty"]="4"
    elif change=="broker_nonzero":
        broker["output1"]=[{"pdno":"122630","hldg_qty":"11"}]
    out=_call(engine,pages,broker,apply=True)
    assert out["state"]=="BLOCKED" and out["reason"]==reason
    with engine.connect() as conn:
        assert conn.execute(sa.select(sa.func.count()).select_from(schema.orders).where(
            schema.orders.c.position_cycle_id.is_(None)
        )).scalar_one()==3
