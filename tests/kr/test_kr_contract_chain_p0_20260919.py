from datetime import datetime, timezone
import copy

import pytest
import sqlalchemy as sa

from trader.db.repos import OrdersRepo, PositionsRepo, _assert_kr_buy_entry_contract
from trader.db.schema import schema_for_engine
from trader.exit_policy.router import resolve_exit_policy_for_position
from trader.trade_plan import build_entry_exit_plan


def _plan():
    return build_entry_exit_plan(
        code="005930",
        market="KOSPI",
        entry_style_selected="ENTRY_PULLBACK",
        entry_reason="ENTRY_PULLBACK",
        entry_price=100.0,
        features={"stop_price": 95.0},
    ).to_dict()


def test_kr_buy_reason_is_cryptographically_bound_through_position_restart(monkeypatch):
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    orders = OrdersRepo(engine)
    positions = PositionsRepo(engine)

    plan = _plan()
    order_id, created = orders.create_intent_idempotent(
        env="practice",
        run_id=None,
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="005930",
        market="KOSPI",
        side="BUY",
        ord_type="LIMIT",
        qty=10,
        limit_price=100.0,
        stage="PB1-ENTRY",
        client_order_key="kr-contract-chain-20260919",
        request_json={
            "enforce_entry_contract": True,
            "entry_exit_plan": plan,
            "pre_order_holding_qty": 0,
            "requested_qty": 10,
            "submitted_qty": 10,
            "balance_snapshot_id": "balance-contract-chain",
        },
        entry_meta_json={
            "entry_reason": "ENTRY_PULLBACK",
            "entry_style_selected": "ENTRY_PULLBACK",
        },
        status="ACKED",
        account_id="acct",
    )
    assert created

    order = orders.get_order_by_client_order_key("practice", "kr-contract-chain-20260919")
    request = order["request_json"]
    root_sha = request["entry_contract_sha256"]
    assert request["entry_exit_plan"]["entry_reason"] == "ENTRY_PULLBACK"
    assert request["entry_exit_plan"]["entry_style_selected"] == "ENTRY_PULLBACK"
    _assert_kr_buy_entry_contract(request)

    positions.apply_fill(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="005930",
        market="KOSPI",
        side="BUY",
        qty=10,
        price=100.0,
        fee=0.0,
        tax=0.0,
        filled_at=datetime.now(timezone.utc),
        order_id=order_id,
        account_id="acct",
    )

    restarted = PositionsRepo(engine)
    with restarted.engine.connect() as conn:
        position = dict(conn.execute(sa.select(schema.positions)).mappings().one())

    assert position["entry_exit_plan_json"]["entry_reason"] == "ENTRY_PULLBACK"
    assert position["entry_exit_plan_json"]["entry_style_selected"] == "ENTRY_PULLBACK"
    assert position["entry_meta_json"]["entry_contract_sha256"] == root_sha

    monkeypatch.setenv("PB1_SWING_TP1_PROFIT_PCT", "99")
    policy = resolve_exit_policy_for_position(
        position,
        {},
        {"current_return_pct": 0.13, "trading_days_held": 2, "mark": 113},
        {"ma20": 100, "ma50": 95, "regime": "NORMAL"},
    )
    assert policy["policy_source"] == "ENTRY_EXIT_PLAN"

    tampered = copy.deepcopy(request)
    tampered["entry_exit_plan"]["entry_reason"] = "ENTRY_BREAKOUT"
    with pytest.raises(RuntimeError, match="entry_contract_sha256"):
        _assert_kr_buy_entry_contract(tampered)
