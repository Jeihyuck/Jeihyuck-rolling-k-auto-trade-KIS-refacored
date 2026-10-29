"""2026-10-29 Mico PB1 owner provenance at BUY order -> KR settlement boundary."""
from __future__ import annotations

from datetime import date
from uuid import uuid4

import pytest
import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.repos import OrdersRepo
from trader.db.schema import schema_for_engine
from trader.settlement.kr_cutover import build_kr_broker_observation
from tests.kr.execution_claim_fixtures import create_schema_with_active_test_epoch


def _durable_order(**changes):
    order = {
        "env": "practice",
        "strategy": "pb1_pullback_close",
        "position_cycle_id": str(uuid4()),
        "trading_epoch_id": str(uuid4()),
        "client_order_key": "mico-20261029-exact",
        "market": "KOSDAQ",
        "kis_odno": "0000012345",
        "side": "BUY",
    }
    order.update(changes)
    return order


def _observation(order, request):
    return build_kr_broker_observation(
        order=order, request_json=request,
        broker_trade_date=date(2026, 10, 29),
        requested_qty=79, cumulative_qty=79,
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        evidence_digest="immutable-daily-ccld:20261029:0000012345",
        # Cumulative order proof is an aggregate, not a single execution.
        # The settlement core rejects execution_qty for KIS_ORDER_CUMULATIVE_ACTUAL.
        execution_price=26050,
    )


def test_old_mico_order_with_durable_pb1_strategy_recovers_owner_not_style():
    obs = _observation(_durable_order(), {
        "entry_plan": {"entry_family": "ENTRY_MOMENTUM"},
        "entry_exit_plan": {"trade_horizon": "DAY_TRADE"},
    })
    assert obs.strategy_owner == "PB1"
    assert obs.cumulative_qty == 79


def test_missing_order_owner_and_unknown_strategy_never_inferred_from_momentum_label():
    with pytest.raises(RuntimeError, match="KR_SETTLEMENT_STRATEGY_OWNER_MISSING"):
        _observation(_durable_order(strategy="unknown_policy"), {
            "entry_plan": {"entry_family": "ENTRY_MOMENTUM"},
            "entry_exit_plan": {"trade_horizon": "DAY_TRADE"},
        })


def test_conflicting_explicit_owner_vs_durable_order_strategy_fails_closed():
    with pytest.raises(RuntimeError, match="KR_SETTLEMENT_STRATEGY_OWNER_CONFLICT"):
        _observation(_durable_order(), {"strategy_owner": "KR_INFINITE"})


def test_proven_infinite_owner_remains_separate():
    obs = _observation(_durable_order(strategy="kr_infinite_trade"), {
        "strategy_owner": "KR_INFINITE",
    })
    assert obs.strategy_owner == "KR_INFINITE"


def test_mico_new_order_persists_pb1_owner_at_source_not_only_reconciliation():
    engine = sa.create_engine("sqlite:///:memory:")
    create_schema_with_active_test_epoch(engine)
    repo = OrdersRepo(engine)
    order_id, created = repo.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close",
        sid=1, mode=1, code="059090", market="KOSDAQ",
        side="BUY", ord_type="LIMIT", qty=79, limit_price=26050,
        stage="PB1-AM", client_order_key="mico-owner-" + uuid4().hex,
        request_json={"entry_exit_plan": {
            "entry_style_selected": "ENTRY_MOMENTUM",
            "trade_horizon": "DAY_TRADE",
        }},
        status="ACKED",
        account_id=get_account_key(env="practice"),
    )
    assert created
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        row = dict(conn.execute(sa.select(schema.orders).where(
            schema.orders.c.order_id == order_id,
        )).mappings().one())
    assert row["request_json"]["strategy_owner"] == "PB1"
    assert row["request_json"]["entry_exit_plan"]["trade_horizon"] == "DAY_TRADE"
    obs = _observation(row, row["request_json"])
    assert obs.strategy_owner == "PB1"


def test_mico_new_order_rejects_infinite_owner_claim():
    engine = sa.create_engine("sqlite:///:memory:")
    create_schema_with_active_test_epoch(engine)
    with pytest.raises(RuntimeError, match="KR_ORDER_STRATEGY_OWNER_CONFLICT"):
        OrdersRepo(engine).create_intent_idempotent(
            env="practice", run_id=None, strategy="pb1_pullback_close",
            sid=1, mode=1, code="059090", market="KOSDAQ",
            side="BUY", ord_type="LIMIT", qty=79, limit_price=26050,
            stage="PB1-AM", client_order_key="mico-mismatch-" + uuid4().hex,
            request_json={"strategy_owner": "KR_INFINITE"},
            status="ACKED", account_id=get_account_key(env="practice"),
        )
