"""Exercise the real signed PB1 BUY reconciliation crash/replay path."""
from datetime import date, datetime
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
import sqlalchemy as sa

import trader.reconcile_kis as rk
from trader.account_state import get_account_key
from trader.db.repos import OrdersRepo, PositionsRepo, PortfolioEpochsRepo
from trader.db.schema import schema_for_engine
from trader.run_context import RunContext
from tests.kr.test_kr_20261006_execution_convergence import _db


class KISBuyDailyReplay:
    def __init__(self, row):
        self.row = dict(row)
        self.calls = 0

    def inquire_daily_ccld(self, **kwargs):
        self.calls += 1
        assert kwargs["start_date"] == "20261022"
        assert kwargs["end_date"] == "20261022"
        return {"rt_cd": "0", "output1": [dict(self.row)], "output2": []}

    def __getattr__(self, key):
        if any(tag in key.lower() for tag in ("order", "cancel", "submit", "sell", "buy")):
            raise AssertionError("KIS broker order/cancel path must never be called")
        raise AttributeError(key)


def _signed_buy_order(engine, *, code, odno, key):
    schema = schema_for_engine(engine)
    env = "practice"
    account = get_account_key(env=env)
    portfolio = PortfolioEpochsRepo(engine).get_or_create_active(
        env=env, account_id=account, sid=1, mode=1,
        strategy="pb1_pullback_close",
    )
    cycle = str(uuid4())
    plan = {
        "entry_style_selected": "ENTRY_PULLBACK",
        "entry_reason": "ENTRY_PULLBACK",
        "exit_policy_family": "SWING_STAGED_EXIT",
        "trade_horizon": "SWING",
        "profit_plan": {"tp1_profit_pct": 3.0, "tp1_sell_pct": 0.25},
    }
    request = {
        "enforce_entry_contract": True,
        "pre_order_holding_qty": 0,
        "requested_qty": 10,
        "submitted_qty": 10,
        "balance_snapshot_id": "pr359-buy-snapshot",
        "entry_exit_plan": plan,
    }
    orders = OrdersRepo(engine)
    order_id, created = orders.create_intent_idempotent(
        env=env, run_id=None, strategy="pb1_pullback_close",
        sid=1, mode=1, code=code, market="J", side="BUY",
        ord_type="LIMIT", qty=10, limit_price=2000.0, stage="PB1-AM",
        client_order_key=key, request_json=request, status="CREATED",
        account_id=account, portfolio_epoch_id=portfolio,
        position_cycle_id=cycle,
    )
    assert created
    assert orders.mark_submitted(
        env, key, odno, {"rt_cd": "0", "msg1": "accepted"},
    ) == "SUBMITTED"
    order_time = datetime(2026, 10, 22, 6, 18, tzinfo=ZoneInfo("UTC"))
    with engine.begin() as conn:
        conn.execute(sa.update(schema.orders).where(
            schema.orders.c.order_id == order_id
        ).values(
            created_at=order_time,
            submitted_at=order_time,
            acked_at=order_time,
        ))
    return str(order_id), cycle


def _buy_row(code, odno):
    return {
        "pdno": code,
        "sll_buy_dvsn_cd": "02",
        "ord_qty": "10",
        "ord_unpr": "2000",
        "tot_ccld_qty": "10",
        "tot_ccld_amt": "20000",
        "avg_prvs": "2000",
        "odno": odno,
        "ord_dt": "20261022",
        "ord_tmd": "151810",
        "ord_stat_cd": "FILLED",
        "market": "J",
        "cncl_yn": "N",
    }


def test_signed_buy_fill_and_position_rollback_then_replay_once(monkeypatch):
    engine = _db()
    schema = schema_for_engine(engine)
    code, odno, key = "251970", "0000041001", "pr359-signed-buy-crash"
    order_id, cycle = _signed_buy_order(
        engine, code=code, odno=odno, key=key,
    )
    kis = KISBuyDailyReplay(_buy_row(code, odno))
    ctx = RunContext(
        run_id=None, env="practice", strategy="pb1_pullback_close",
        started_at=datetime(2026, 10, 22, 15, 18, tzinfo=ZoneInfo("Asia/Seoul")),
        dry_run=True,
    )
    monkeypatch.setattr(rk, "_execution_claim_integrity_reason", lambda *a, **k: None)

    def crash(*args, **kwargs):
        raise RuntimeError("INJECTED_BUY_POSITION_FAILURE")

    with monkeypatch.context() as failures:
        failures.setattr(PositionsRepo, "apply_fill", crash)
        with pytest.raises(RuntimeError, match="INJECTED_BUY_POSITION_FAILURE"):
            rk.reconcile_today(
                engine=engine, kis=kis, ctx=ctx, trade_date=date(2026, 10, 22),
            )

    with engine.connect() as conn:
        fills = conn.execute(sa.select(schema.fills).where(
            schema.fills.c.order_id == order_id
        )).all()
        position = conn.execute(sa.select(schema.positions).where(
            schema.positions.c.position_cycle_id == cycle
        )).mappings().first()
        order = conn.execute(sa.select(schema.orders).where(
            schema.orders.c.order_id == order_id
        )).mappings().one()
    assert fills == [], "native BUY fill committed although position application failed"
    assert position is None, "BUY position was partially committed"
    assert order["status"] == "SUBMITTED"
    assert _json_dict(order["response_json"]).get("confirmed_fill_qty") is None

    for _ in range(2):
        rk.reconcile_today(
            engine=engine, kis=kis, ctx=ctx, trade_date=date(2026, 10, 22),
        )

    with engine.connect() as conn:
        fills = conn.execute(sa.select(schema.fills).where(
            schema.fills.c.order_id == order_id
        )).mappings().all()
        position = conn.execute(sa.select(schema.positions).where(
            schema.positions.c.position_cycle_id == cycle
        )).mappings().one()
        order = conn.execute(sa.select(schema.orders).where(
            schema.orders.c.order_id == order_id
        )).mappings().one()
    response = _json_dict(order["response_json"])
    assert len(fills) == 1
    assert fills[0]["qty"] == 10
    assert fills[0]["price"] == pytest.approx(2000.0)
    assert str(fills[0]["position_cycle_id"]) == cycle
    assert position["qty"] == 10
    assert position["position_origin"] == "SYSTEM"
    assert position["entry_meta_json"]["source_buy_order_id"] == order_id
    assert position["entry_meta_json"]["entry_contract_sha256"]
    assert position["entry_exit_plan_json"]["exit_policy_family"] == "SWING_STAGED_EXIT"
    assert int(response["confirmed_fill_qty"]) == 10
    assert response["applied_broker_fill_ids"]
    assert order["status"] == "FILLED"
    assert kis.calls == 3


def _json_dict(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        import json
        return json.loads(value)
    return {}
