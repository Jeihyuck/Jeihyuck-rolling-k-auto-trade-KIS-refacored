"""PR354: signed PB1 BUY/SELL broker-evidence crash/restart test.

Uses actual OrdersRepo, FillsRepo, PositionsRepo and active epoch in an
isolated SQLite database. Does not call KIS, submit orders, or alter operations.
"""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4
import pytest
import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.repos import (
    OrdersRepo, FillsRepo, PositionsRepo, PortfolioEpochsRepo,
    _kr_buy_entry_contract_hash,
)
from trader.db.schema import schema_for_engine
from trader.db.trading_epoch import active_trading_epoch_id
from tests.kr.execution_claim_fixtures import create_schema_with_active_test_epoch


def test_signed_buy_fill_crash_then_recovery_and_native_sell_once():
    engine = sa.create_engine("sqlite:///:memory:")
    schema = create_schema_with_active_test_epoch(engine)
    env, owner, code, market = "practice", "pb1_pullback_close", "035760", "KOSDAQ"
    account = get_account_key(env=env)
    portfolio = PortfolioEpochsRepo(engine).get_or_create_active(
        env=env, account_id=account, strategy=owner, sid=1, mode=1,
    )
    with engine.connect() as conn:
        epoch = active_trading_epoch_id(
            conn, env=env, account_id=account, required=True,
        )
    cycle = str(uuid4())
    key = "pr354-recovery-" + uuid4().hex
    plan = {
        "entry_style_selected": "ENTRY_PULLBACK",
        "entry_reason": "ENTRY_PULLBACK",
        "exit_policy_family": "SWING_STAGED_EXIT",
        "trade_horizon": "SWING",
        "profit_plan": {"tp1_profit_pct": 3.0, "tp1_sell_pct": 0.25},
    }
    request = {
        "enforce_entry_contract": True,
        "pre_order_holding_qty": 0, "requested_qty": 10,
        "submitted_qty": 10, "balance_snapshot_id": "pr354-snapshot",
        "entry_exit_plan": plan,
    }
    orders, fills, positions = OrdersRepo(engine), FillsRepo(engine), PositionsRepo(engine)
    order_id, created = orders.create_intent_idempotent(
        env=env, run_id=None, strategy=owner, sid=1, mode=1,
        code=code, market=market, side="BUY", ord_type="LIMIT",
        qty=10, limit_price=2000, stage="PB1-AM",
        client_order_key=key, request_json=request, status="CREATED",
        account_id=account, portfolio_epoch_id=portfolio,
        position_cycle_id=cycle,
    )
    assert created
    with engine.connect() as conn:
        buy_order = dict(conn.execute(
            sa.select(schema.orders).where(schema.orders.c.order_id == order_id)
        ).mappings().one())
    frozen = buy_order["request_json"]
    assert frozen["enforce_entry_contract"] is True
    assert frozen["entry_contract_sha256"] == _kr_buy_entry_contract_hash(frozen)
    assert frozen["position_cycle_id"] == cycle
    assert frozen["portfolio_epoch_id"] == portfolio
    assert frozen["trading_epoch_id"] == str(epoch)

    assert orders.mark_submitted(
        env, key, "0000011759", {"rt_cd": "0", "msg1": "accepted"},
    ) == "SUBMITTED"

    stamp = datetime(2026, 11, 6, 0, 32, tzinfo=timezone.utc)
    native_buy = dict(
        env=env, run_id=None, order_id=str(order_id),
        kis_odno="0000011759", trade_id="KR_KIS_CUM:PR354_BUY",
        code=code, market=market, side="BUY",
        qty=10, price=2000.0, fee=0, tax=0, filled_at=stamp,
        raw_json={"kis_response": {
            "ord_dt": "20261106", "pdno": code,
            "odno": "0000011759", "tot_ccld_qty": "10",
            "tot_ccld_amt": "20000",
        }},
        fill_meta_json={
            "evidence_kind": "KIS_ORDER_CUMULATIVE_EXECUTION",
            "filled_at_source": "ORDER_TIME_PROXY",
        },
        position_cycle_id=cycle, portfolio_epoch_id=portfolio,
        trading_epoch_id=str(epoch),
    )
    fills.upsert_fill(**native_buy)
    # Process dies after durable native KIS fill, before position accounting.
    assert positions.get_position(
        env=env, strategy=owner, sid=1, mode=1, code=code,
        position_cycle_id=cycle, portfolio_epoch_id=portfolio,
    ) is None

    # Reopened repository instance simulates restart; applies only broker-
    # proven delta to original signed BUY lifecycle, no imported cycle.
    positions = PositionsRepo(engine)
    positions.apply_fill(
        env=env, strategy=owner, sid=1, mode=1, code=code, market=market,
        side="BUY", qty=10, price=2000, fee=0, tax=0, filled_at=stamp,
        entry_exit_plan=plan, portfolio_epoch_id=portfolio,
        position_cycle_id=cycle, order_id=str(order_id),
        buy_application_cumulative_qty=10,
        buy_application_previous_confirmed_qty=0,
        buy_cost_delta_override=20000,
    )
    fills.upsert_fill(**native_buy)
    # Same broker cumulative snapshot replays with ZERO incremental quantity.
    positions.apply_fill(
        env=env, strategy=owner, sid=1, mode=1, code=code, market=market,
        side="BUY", qty=0, price=2000, fee=0, tax=0, filled_at=stamp,
        entry_exit_plan=plan, portfolio_epoch_id=portfolio,
        position_cycle_id=cycle, order_id=str(order_id),
        buy_application_cumulative_qty=10,
        buy_application_previous_confirmed_qty=10,
        buy_cost_delta_override=0,
    )
    opened = positions.get_position(
        env=env, strategy=owner, sid=1, mode=1, code=code,
        position_cycle_id=cycle, portfolio_epoch_id=portfolio,
    )
    assert opened["qty"] == 10
    assert opened["position_origin"] == "SYSTEM"
    assert opened["entry_meta_json"]["entry_contract_sha256"] == frozen["entry_contract_sha256"]
    assert opened["entry_meta_json"]["source_buy_order_id"] == str(order_id)
    assert opened["entry_exit_plan_json"] == plan

    sell_key = "pr354-sell-" + uuid4().hex
    sell_order_id, created = orders.create_intent_idempotent(
        env=env, run_id=None, strategy=owner, sid=1, mode=1,
        code=code, market=market, side="SELL", ord_type="LIMIT",
        qty=4, limit_price=2100, stage="TP1",
        client_order_key=sell_key, request_json={"source_buy_order_id": str(order_id)},
        status="CREATED", account_id=account,
        portfolio_epoch_id=portfolio, position_cycle_id=cycle,
    )
    assert created
    assert orders.mark_submitted(
        env, sell_key, "0000005003", {"rt_cd": "0"},
    ) == "SUBMITTED"
    native_sell = dict(
        native_buy, order_id=str(sell_order_id),
        side="SELL", qty=4, price=2100.0, kis_odno="0000005003",
        trade_id="KR_KIS_CUM:PR354_SELL",
        raw_json={"kis_response": {
            "ord_dt": "20261106", "pdno": code,
            "odno": "0000005003", "tot_ccld_qty": "4",
            "tot_ccld_amt": "8400",
        }},
    )
    fills.upsert_fill(**native_sell)
    # Simulate second crash, between durable SELL fill and accounting.
    positions = PositionsRepo(engine)
    result = positions.reconcile_sell_execution(
        env=env, strategy=owner, sid=1, mode=1, code=code, market=market,
        confirmed_cumulative_qty=4, fill_price=2100.0,
        filled_at=stamp, position_cycle_id=cycle,
        portfolio_epoch_id=portfolio, order_id=str(sell_order_id),
        pre_order_holding_qty=10, broker_holding_qty=6,
        native_original_order_price_proof=True,
    )
    assert result["qty_applied"] == 4
    repeated = positions.reconcile_sell_execution(
        env=env, strategy=owner, sid=1, mode=1, code=code, market=market,
        confirmed_cumulative_qty=4, fill_price=2100.0,
        filled_at=stamp, position_cycle_id=cycle,
        portfolio_epoch_id=portfolio, order_id=str(sell_order_id),
        pre_order_holding_qty=10, broker_holding_qty=6,
        native_original_order_price_proof=True,
    )
    assert repeated["qty_applied"] == 0
    residual = positions.get_position(
        env=env, strategy=owner, sid=1, mode=1, code=code,
        position_cycle_id=cycle, portfolio_epoch_id=portfolio,
    )
    assert residual["qty"] == 6
    assert residual["entry_meta_json"]["entry_contract_sha256"] == frozen["entry_contract_sha256"]
    assert residual["entry_exit_plan_json"] == plan
    with engine.connect() as conn:
        rows = conn.execute(
            sa.select(schema.fills).where(schema.fills.c.trading_epoch_id == epoch)
        ).mappings().all()
    assert len(rows) == 2  # exact KIS BUY and SELL cumulative order identities


def test_pyramid_add_preserves_original_signed_buy_provenance():
    """An ADD order may change quantity/cost, never the root BUY identity."""
    engine = sa.create_engine("sqlite:///:memory:")
    create_schema_with_active_test_epoch(engine)
    env, owner, code, market = "practice", "pb1_pullback_close", "035760", "KOSDAQ"
    account = get_account_key(env=env)
    portfolio = PortfolioEpochsRepo(engine).get_or_create_active(
        env=env, account_id=account, strategy=owner, sid=1, mode=1,
    )
    cycle = str(uuid4())
    plan = {
        "entry_style_selected": "ENTRY_PULLBACK",
        "entry_reason": "ENTRY_PULLBACK",
        "exit_policy_family": "SWING_STAGED_EXIT",
        "trade_horizon": "SWING",
    }
    orders = OrdersRepo(engine)
    positions = PositionsRepo(engine)
    root_key = "kr359-root-" + uuid4().hex
    root_id, created = orders.create_intent_idempotent(
        env=env, run_id=None, strategy=owner, sid=1, mode=1, code=code,
        market=market, side="BUY", ord_type="LIMIT", qty=10,
        limit_price=2000, stage="PB1-AM", client_order_key=root_key,
        request_json={
            "enforce_entry_contract": True,
            "pre_order_holding_qty": 0,
            "requested_qty": 10, "submitted_qty": 10,
            "balance_snapshot_id": "kr359-root-snapshot",
            "entry_exit_plan": plan,
        }, status="CREATED", account_id=account,
        portfolio_epoch_id=portfolio, position_cycle_id=cycle,
    )
    assert created
    stamp = datetime(2026, 11, 13, 0, 31, tzinfo=timezone.utc)
    positions.apply_fill(
        env=env, strategy=owner, sid=1, mode=1, code=code, market=market,
        side="BUY", qty=10, price=2000.0, fee=0, tax=0, filled_at=stamp,
        entry_exit_plan=plan, portfolio_epoch_id=portfolio,
        position_cycle_id=cycle, order_id=str(root_id),
        buy_application_cumulative_qty=10,
        buy_application_previous_confirmed_qty=0,
        buy_cost_delta_override=20000,
    )
    before = positions.get_position(
        env=env, strategy=owner, sid=1, mode=1, code=code,
        position_cycle_id=cycle, portfolio_epoch_id=portfolio,
    )
    assert before["qty"] == 10
    root_meta = dict(before["entry_meta_json"])
    root_sha = root_meta["entry_contract_sha256"]

    add_key = "kr359-add-" + uuid4().hex
    add_id, created = orders.create_intent_idempotent(
        env=env, run_id=None, strategy=owner, sid=1, mode=1, code=code,
        market=market, side="BUY", ord_type="LIMIT", qty=3,
        limit_price=2200, stage="PB1-ADD", client_order_key=add_key,
        request_json={
            "enforce_entry_contract": True,
            "pre_order_holding_qty": 10, "requested_qty": 3,
            "submitted_qty": 3,
            "balance_snapshot_id": "kr359-add-snapshot",
            "parent_entry_contract_sha256": root_sha,
            "parent_position_cycle_id": cycle,
            "parent_portfolio_epoch_id": portfolio,
            "entry_exit_plan": plan,
        }, status="CREATED", account_id=account,
        portfolio_epoch_id=portfolio, position_cycle_id=cycle,
    )
    assert created
    positions.apply_fill(
        env=env, strategy=owner, sid=1, mode=1, code=code, market=market,
        side="BUY", qty=3, price=2200.0, fee=0, tax=0, filled_at=stamp,
        entry_exit_plan=plan, portfolio_epoch_id=portfolio,
        position_cycle_id=cycle, order_id=str(add_id),
        buy_application_cumulative_qty=3,
        buy_application_previous_confirmed_qty=0,
        buy_cost_delta_override=6600,
    )
    after = positions.get_position(
        env=env, strategy=owner, sid=1, mode=1, code=code,
        position_cycle_id=cycle, portfolio_epoch_id=portfolio,
    )
    assert after["qty"] == 13
    assert after["total_cost"] == pytest.approx(26600)
    assert after["entry_exit_plan_json"] == before["entry_exit_plan_json"]
    after_meta = after["entry_meta_json"]
    for frozen_key in (
        "entry_contract_sha256", "source_buy_order_id",
        "source_buy_client_order_key", "entry_exit_plan_sha256",
    ):
        assert after_meta[frozen_key] == root_meta[frozen_key]
    assert after_meta["last_buy_order_id"] == str(add_id)
    assert after_meta["last_buy_client_order_key"] == add_key
    assert after_meta["last_buy_order_contract_sha256"] != root_sha
    assert after_meta["broker_truth_buy_applied_orders"][str(add_id)]["qty"] == 3
