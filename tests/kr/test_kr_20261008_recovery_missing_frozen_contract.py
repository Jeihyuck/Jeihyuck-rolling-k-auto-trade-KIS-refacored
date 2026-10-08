"""2026-10-08 KR incident: a restored PB1 holding has a policy FAMILY but no contract.

The broker-proven filled BUY contract must be reattached without changing qty,
cycle identity, frozen strategy thresholds, or an unrelated strategy's holding.
"""
from __future__ import annotations

from uuid import uuid4

import pytest
import sqlalchemy as sa

from trader.db.repos import (
    KR_BUY_ENTRY_CONTRACT_VERSION,
    _kr_buy_entry_contract_hash,
    _kr_entry_exit_plan_sha256,
)
from trader.db.schema import schema_for_engine
from trader.kr.broker_truth_review_fixes import _recover_proven_policy_positions_fixed
from trader.time_utils import now_kst


STRATEGY = "pb1_pullback_close"


def _incident(*, code: str, qty: int, proven_cycle: bool = True, corrupt_hash: bool = False):
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    order_id, order_cycle, recovery_cycle, epoch = (str(uuid4()) for _ in range(4))
    filled_at = now_kst()
    plan = {
        "exit_policy_family": "SWING_STAGED_EXIT",
        "entry_reason": "ENTRY_PULLBACK_OVERRIDE",
        "trade_horizon": "SWING",
        "policy_version": "pb1_entry_exit_plan_v1",
        "risk_plan": {"initial_stop": 85_000.0, "risk_R": 5_000.0},
        "profit_plan": {"tp1_r": 2.0, "tp2_r": 3.0, "tp1_sell_pct": 0.33},
        "time_plan": {"max_trading_days": 10},
    }
    request = {
        "enforce_entry_contract": True,
        "entry_contract_version": KR_BUY_ENTRY_CONTRACT_VERSION,
        "entry_exit_plan": plan,
        "entry_meta": {"entry_reason": "ENTRY_PULLBACK_OVERRIDE", "trade_horizon": "SWING"},
        "pre_order_holding_qty": 0,
        "requested_qty": qty,
        "submitted_qty": qty,
        "balance_snapshot_id": "incident-balance",
        "client_order_key": f"pb1:20261008:{code}:buy",
        "position_cycle_id": order_cycle,
        "portfolio_epoch_id": epoch,
        "trading_epoch_id": None,
    }
    request["entry_contract_sha256"] = _kr_buy_entry_contract_hash(request)
    if corrupt_hash:
        request["entry_contract_sha256"] = "tampered-contract"
    evidence = {
        "provenance_verified": True,
        "recovered_from_cycle_id": order_cycle if proven_cycle else str(uuid4()),
        "recovered_from_epoch_id": epoch,
    }
    with engine.begin() as conn:
        conn.execute(sa.insert(schema.positions).values(
            position_id=str(uuid4()), position_cycle_id=recovery_cycle,
            portfolio_epoch_id=epoch, opened_at=filled_at,
            position_origin="RECOVERY", env="practice", strategy=STRATEGY,
            sid=1, mode=1, code=code, market="KOSPI", qty=qty,
            avg_buy_price=100_000.0, total_cost=qty * 100_000.0,
            status="OPEN", exit_policy_family="SWING_STAGED_EXIT",
            entry_exit_plan_json={}, entry_meta_json={}, position_meta=evidence,
            policy_source="recovered_net_fill_provenance",
        ))
        conn.execute(sa.insert(schema.orders).values(
            order_id=order_id, position_cycle_id=order_cycle, portfolio_epoch_id=epoch,
            env="practice", strategy=STRATEGY, sid=1, mode=1,
            code=code, market="KOSPI", side="BUY", ord_type="MARKET",
            qty=qty, stage="PB1-AM", client_order_key=request["client_order_key"],
            status="FILLED", kis_odno="0000011751", request_json=request,
            response_json={"rt_cd": "0"}, created_at=filled_at,
        ))
        conn.execute(sa.insert(schema.fills).values(
            fill_id=str(uuid4()), env="practice", order_id=order_id,
            kis_odno="0000011751", trade_id=f"incident:{code}",
            code=code, market="KOSPI", side="BUY", qty=qty, price=100_000.0,
            fee=0.0, tax=0.0, filled_at=filled_at,
            position_cycle_id=order_cycle, portfolio_epoch_id=epoch,
            raw_json={"source": "daily_ccld"},
        ))
    return engine, schema, plan, request, recovery_cycle


@pytest.mark.parametrize(("code", "qty"), [("036540", 140), ("003670", 8)])
def test_proven_recovery_family_with_empty_contract_restores_exact_frozen_plan(code, qty):
    engine, schema, plan, request, recovery_cycle = _incident(code=code, qty=qty)
    holdings = [{"pdno": code, "hldg_qty": str(qty), "pchs_avg_pric": "100000"}]
    first = _recover_proven_policy_positions_fixed(
        engine=engine, env="practice", strategy=STRATEGY, holdings_rows=holdings,
    )
    assert first == {"recovered": [code], "review_required": []}
    with engine.connect() as conn:
        row = conn.execute(sa.select(schema.positions)).mappings().one()
    assert str(row["position_cycle_id"]) == recovery_cycle
    assert row["qty"] == qty
    assert row["total_cost"] == qty * 100_000.0
    assert row["entry_exit_plan_json"] == plan
    assert row["entry_meta_json"]["entry_exit_plan_sha256"] == _kr_entry_exit_plan_sha256(plan)
    assert row["entry_meta_json"]["entry_contract_sha256"] == request["entry_contract_sha256"]
    assert row["entry_meta_json"]["source_buy_order_id"]
    assert row["position_meta"]["recovered_from_cycle_id"] == request["position_cycle_id"]

    second = _recover_proven_policy_positions_fixed(
        engine=engine, env="practice", strategy=STRATEGY, holdings_rows=holdings,
    )
    assert second["recovered"] == []
    with engine.connect() as conn:
        after = conn.execute(sa.select(schema.positions)).mappings().one()
    assert after["qty"] == qty and after["entry_exit_plan_json"] == plan


@pytest.mark.parametrize("failure", ["cross_cycle", "contract_hash"])
def test_recovery_refuses_ambiguous_or_tampered_buy_contract(failure):
    engine, schema, _, _, recovery_cycle = _incident(
        code="036540", qty=140,
        proven_cycle=failure != "cross_cycle",
        corrupt_hash=failure == "contract_hash",
    )
    outcome = _recover_proven_policy_positions_fixed(
        engine=engine, env="practice", strategy=STRATEGY,
        holdings_rows=[{"pdno": "036540", "hldg_qty": "140"}],
    )
    assert outcome == {"recovered": [], "review_required": ["036540"]}
    with engine.connect() as conn:
        row = conn.execute(sa.select(schema.positions)).mappings().one()
    assert row["position_cycle_id"] == recovery_cycle
    assert row["entry_exit_plan_json"] == {}
    assert row["qty"] == 140


@pytest.mark.parametrize(("code", "qty"), [("036540", 140), ("003670", 8)])
def test_exact_recovery_alias_repairs_buy_watermark_without_rebuy(code, qty):
    from trader.kr.holdings_promotion_repair import repair_promoted_buy_watermark

    engine, schema, plan, request, recovery_cycle = _incident(code=code, qty=qty)
    with engine.begin() as conn:
        row = conn.execute(sa.select(schema.orders)).mappings().one()
        conn.execute(sa.update(schema.orders).where(
            schema.orders.c.order_id == row["order_id"]
        ).values(
            response_json={
                "promotion_source": "kis_holdings",
                "confirmed_fill_qty": qty,
                "holding_qty": qty,
                "pre_order_holding_qty": 0,
            }
        ))
    holdings = [{"pdno": code, "hldg_qty": str(qty)}]
    # The watermark cannot authorize a RECOVERY alias before exact contract repair.
    with engine.connect() as conn:
        original = dict(conn.execute(sa.select(schema.orders)).mappings().one())
    assert not repair_promoted_buy_watermark(engine=engine, env="practice", order=original)
    result = _recover_proven_policy_positions_fixed(
        engine=engine, env="practice", strategy=STRATEGY, holdings_rows=holdings,
    )
    assert result["recovered"] == [code]
    assert repair_promoted_buy_watermark(engine=engine, env="practice", order=original)
    assert repair_promoted_buy_watermark(engine=engine, env="practice", order=original)

    with engine.connect() as conn:
        pos = conn.execute(sa.select(schema.positions)).mappings().one()
    assert str(pos["position_cycle_id"]) == recovery_cycle
    assert pos["qty"] == qty
    assert pos["entry_exit_plan_json"] == plan
    assert pos["entry_meta_json"]["broker_truth_buy_applied_orders"][str(original["order_id"])]["qty"] == qty


def test_recovery_alias_rejects_tampered_source_cycle_before_applying_watermark():
    from trader.kr.holdings_promotion_repair import repair_promoted_buy_watermark

    engine, schema, _, _, _ = _incident(code="036540", qty=140, proven_cycle=False)
    with engine.begin() as conn:
        row = conn.execute(sa.select(schema.orders)).mappings().one()
        conn.execute(sa.update(schema.orders).where(
            schema.orders.c.order_id == row["order_id"]
        ).values(response_json={
            "promotion_source": "kis_holdings", "confirmed_fill_qty": 140,
            "holding_qty": 140, "pre_order_holding_qty": 0,
        }))
    with engine.connect() as conn:
        order = dict(conn.execute(sa.select(schema.orders)).mappings().one())
    assert not repair_promoted_buy_watermark(engine=engine, env="practice", order=order)
    with engine.connect() as conn:
        pos = conn.execute(sa.select(schema.positions)).mappings().one()
    assert pos["qty"] == 140
    assert pos["entry_exit_plan_json"] == {}


def test_recovery_uses_root_buy_contract_not_later_pyramid_buy():
    from datetime import timedelta

    engine, schema, plan, root_request, _ = _incident(code="036540", qty=120)
    with engine.begin() as conn:
        root_order = conn.execute(sa.select(schema.orders)).mappings().one()
        root_fill = conn.execute(sa.select(schema.fills)).mappings().one()
        pos = conn.execute(sa.select(schema.positions)).mappings().one()
        conn.execute(sa.update(schema.positions).where(
            schema.positions.c.position_id == pos["position_id"]
        ).values(qty=140, total_cost=14_000_000.0))
        add_request = {
            "entry_exit_plan": {
                **plan, "risk_plan": {"initial_stop": 5.0, "risk_R": 9999.0},
            },
            "entry_meta": {"entry_reason": "ENTRY_PYRAMID", "trade_horizon": "SWING"},
            "pre_order_holding_qty": 120,
        }
        add_id = str(uuid4())
        conn.execute(sa.insert(schema.orders).values(
            order_id=add_id, position_cycle_id=root_order["position_cycle_id"],
            portfolio_epoch_id=root_order["portfolio_epoch_id"], env="practice",
            strategy=STRATEGY, sid=1, mode=1,
            code="036540", market="KOSPI", side="BUY", ord_type="LIMIT",
            qty=20, stage="PB1-ADD", status="FILLED",
            client_order_key="pyramid-after-root", kis_odno="0000011752",
            request_json=add_request, created_at=root_order["created_at"] + timedelta(seconds=1),
        ))
        conn.execute(sa.insert(schema.fills).values(
            fill_id=str(uuid4()), env="practice", order_id=add_id,
            kis_odno="0000011752", trade_id="pyramid-after-root-fill",
            code="036540", market="KOSPI", side="BUY", qty=20, price=100000.0,
            fee=0.0, tax=0.0,
            filled_at=root_fill["filled_at"] + timedelta(seconds=1),
            position_cycle_id=root_order["position_cycle_id"],
            portfolio_epoch_id=root_order["portfolio_epoch_id"],
            raw_json={"source": "daily_ccld"},
        ))
    result = _recover_proven_policy_positions_fixed(
        engine=engine, env="practice", strategy=STRATEGY,
        holdings_rows=[{"pdno": "036540", "hldg_qty": "140"}],
    )
    assert result == {"recovered": ["036540"], "review_required": []}
    with engine.connect() as conn:
        pos_after = conn.execute(sa.select(schema.positions)).mappings().one()
    assert pos_after["qty"] == 140
    assert pos_after["entry_exit_plan_json"] == plan
    assert pos_after["entry_meta_json"]["source_buy_order_id"] == str(root_order["order_id"])
    assert pos_after["entry_meta_json"]["entry_contract_sha256"] == root_request["entry_contract_sha256"]


def test_malformed_enforced_root_quantity_is_review_required_not_reconcile_crash():
    engine, schema, _, _, _ = _incident(code="003670", qty=8)
    with engine.begin() as conn:
        order = conn.execute(sa.select(schema.orders)).mappings().one()
        request = dict(order["request_json"])
        request["requested_qty"] = "not-an-integer"
        conn.execute(sa.update(schema.orders).where(
            schema.orders.c.order_id == order["order_id"]
        ).values(request_json=request))
    result = _recover_proven_policy_positions_fixed(
        engine=engine, env="practice", strategy=STRATEGY,
        holdings_rows=[{"pdno": "003670", "hldg_qty": "8"}],
    )
    assert result == {"recovered": [], "review_required": ["003670"]}
    with engine.connect() as conn:
        pos = conn.execute(sa.select(schema.positions)).mappings().one()
    assert pos["entry_exit_plan_json"] == {} and pos["qty"] == 8
