from datetime import datetime, timezone

import pytest
import sqlalchemy as sa

from trader.db.repos import OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.exit_policy.router import resolve_exit_policy_for_position
from trader.kr.market_state_overlay import generate_kr_profit_capture_intents
from trader.trade_plan import build_entry_exit_plan


def _plan(style: str = "ENTRY_PULLBACK"):
    return build_entry_exit_plan(
        code="005930", market="KOSPI",
        entry_style_selected=style, entry_reason=style,
        entry_price=100.0, features={"stop_price": 95.0},
    ).to_dict()


def test_kr_router_executes_buy_time_plan_after_env_changes(monkeypatch):
    monkeypatch.setenv("PB1_SWING_TP1_PROFIT_PCT", "12")
    monkeypatch.setenv("PB1_SWING_TP1_SELL_PCT", "0.33")
    plan = _plan()
    monkeypatch.setenv("PB1_SWING_TP1_PROFIT_PCT", "99")
    monkeypatch.setenv("PB1_SWING_TP1_SELL_PCT", "0.90")

    policy = resolve_exit_policy_for_position(
        {"code": "005930", "entry_exit_plan_json": plan,
         "entry_style_selected": "ENTRY_PULLBACK",
         "exit_policy_family": "SWING_STAGED_EXIT", "trade_horizon": "SWING"},
        {}, {"current_return_pct": 0, "trading_days_held": 1, "mark": 100},
        {"ma20": 90, "ma50": 80, "regime": "NORMAL"},
    )
    pct_rule = next(r for r in policy["partial_sell_rules"]
                    if r["trigger"] == "percent" and r.get("meta_flag") == "tp1_done")
    assert policy["policy_source"] == "ENTRY_EXIT_PLAN"
    assert pct_rule["profit_pct"] == 12.0
    assert pct_rule["sell_pct"] == pytest.approx(0.33)


def test_kr_day_plan_preserves_buy_time_50pct_tp1():
    plan = _plan("ENTRY_BREAKOUT")
    policy = resolve_exit_policy_for_position(
        {"code": "005930", "entry_exit_plan_json": plan,
         "entry_style_selected": "ENTRY_BREAKOUT",
         "exit_policy_family": "INTRADAY_PROFIT_PROTECT", "trade_horizon": "DAY_TRADE"},
        {}, {"current_return_pct": 0, "trading_days_held": 0, "mark": 100},
        {"ma20": 90, "ma50": 80, "regime": "NORMAL"},
    )
    tp1 = next(r for r in policy["partial_sell_rules"] if r["trigger"] == "r_hybrid")
    assert tp1["sell_pct"] == pytest.approx(0.50)


def test_kr_global_tp_cannot_front_run_standard_entry_plan():
    position = {
        "code": "005930", "qty": 20, "orderable_qty": 20,
        "unrealized_pnl_pct": 0.09,
        "entry_exit_plan_json": _plan(), "position_meta": {},
    }
    assert generate_kr_profit_capture_intents([position], {"market_state": "KR_NORMAL"}) == []


def test_kr_infinite_is_never_consumed_by_standard_profit_capture():
    position = {
        "code": "122630", "qty": 20, "orderable_qty": 20,
        "unrealized_pnl_pct": 0.20, "owner_strategy": "KR_INFINITE",
        "position_meta": {},
    }
    assert generate_kr_profit_capture_intents([position], {"market_state": "KR_NORMAL"}) == []


def test_kr_buy_contract_survives_order_fill_restart_and_drives_exit(monkeypatch):
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    orders = OrdersRepo(engine)
    positions = PositionsRepo(engine)

    monkeypatch.setenv("PB1_SWING_TP1_PROFIT_PCT", "12")
    monkeypatch.setenv("PB1_SWING_TP1_SELL_PCT", "0.33")
    plan = _plan()
    order_id, created = orders.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code="005930", market="KOSPI", side="BUY", ord_type="LIMIT", qty=10,
        limit_price=100.0, stage="PB1-ENTRY", client_order_key="kr-e2e-contract-buy",
        request_json={
            "enforce_entry_contract": True,
            "entry_exit_plan": plan,
            "pre_order_holding_qty": 0,
            "requested_qty": 10,
            "submitted_qty": 10,
            "balance_snapshot_id": "balance-e2e",
        },
        entry_meta_json={"entry_reason": "ENTRY_PULLBACK", "entry_style_selected": "ENTRY_PULLBACK"},
        status="ACKED", account_id="acct",
    )
    assert created
    order = orders.get_order_by_client_order_key("practice", "kr-e2e-contract-buy")
    root_sha = order["request_json"]["entry_contract_sha256"]

    # Deliberately omit entry_exit_plan here. apply_fill must recover and verify
    # the exact durable BUY order contract by order_id.
    positions.apply_fill(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="005930", market="KOSPI", side="BUY", qty=10, price=100.0,
        fee=0.0, tax=0.0, filled_at=datetime.now(timezone.utc),
        order_id=order_id,
    )

    with engine.connect() as conn:
        stored = dict(conn.execute(sa.select(schema.positions)).mappings().one())
    assert stored["entry_exit_plan_json"] == plan
    assert stored["entry_meta_json"]["entry_contract_sha256"] == root_sha
    assert stored["entry_meta_json"]["entry_contract_version"] == "kr_buy_entry_contract_v1"

    # Simulate a process restart by constructing a fresh repository object and
    # changing today's ENV. Existing lifecycle must still execute the BUY plan.
    restarted = PositionsRepo(engine)
    with restarted.engine.connect() as conn:
        reloaded = dict(conn.execute(sa.select(schema.positions)).mappings().one())
    monkeypatch.setenv("PB1_SWING_TP1_PROFIT_PCT", "99")
    monkeypatch.setenv("PB1_SWING_TP1_SELL_PCT", "0.90")
    policy = resolve_exit_policy_for_position(
        reloaded, {}, {"current_return_pct": 0.13, "trading_days_held": 2, "mark": 113},
        {"ma20": 100, "ma50": 95, "regime": "NORMAL"},
    )
    assert policy["policy_source"] == "ENTRY_EXIT_PLAN"
    assert reloaded["entry_meta_json"]["entry_contract_sha256"] == root_sha
    tp1 = next(r for r in policy["partial_sell_rules"]
               if r["trigger"] == "percent" and r.get("meta_flag") == "tp1_done")
    assert tp1["profit_pct"] == 12.0
    assert tp1["sell_pct"] == pytest.approx(0.33)
