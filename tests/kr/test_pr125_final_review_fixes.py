from __future__ import annotations

from uuid import uuid4

import sqlalchemy as sa

from trader.db.repos import FillsRepo, OrdersRepo
from trader.db.schema import schema_for_engine
from trader.kr.broker_truth_final_review_fixes import (
    _BUY_APPLIED_KEY,
    _install_order_response_merge_guard,
    _link_buy_fills_retry_safe,
)
from trader.time_utils import now_kst


STRATEGY = "pb1_pullback_close"


def _db():
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    return engine


def _epoch(engine, epoch_id: str) -> None:
    schema = schema_for_engine(engine)
    with engine.begin() as conn:
        conn.execute(
            sa.insert(schema.portfolio_epochs).values(
                portfolio_epoch_id=epoch_id,
                env="practice",
                account_id="practice:test",
                sid=1,
                mode=1,
                strategy=STRATEGY,
                status="ACTIVE",
            )
        )


def _order(engine, *, order_id: str, cycle_id: str, epoch_id: str, odno: str, qty: int, baseline: int) -> None:
    schema = schema_for_engine(engine)
    ts = now_kst()
    with engine.begin() as conn:
        conn.execute(
            sa.insert(schema.orders).values(
                order_id=order_id,
                position_cycle_id=cycle_id,
                portfolio_epoch_id=epoch_id,
                env="practice",
                strategy=STRATEGY,
                sid=1,
                mode=1,
                code="005930",
                market="KOSPI",
                side="BUY",
                ord_type="MARKET",
                qty=qty,
                stage="ENTRY",
                client_order_key=f"final-review-{order_id}",
                status="ACKED",
                kis_odno=odno,
                broker_order_id=odno,
                request_json={
                    "pre_order_holding_qty": baseline,
                    "entry_meta": {
                        "entry_reason": "ENTRY_PULLBACK_CONFIRM",
                        "entry_style_selected": "PULLBACK",
                        "trade_horizon": "SWING",
                        "exit_policy_family": "SWING_STAGED_EXIT",
                    },
                    "entry_exit_plan": {
                        "entry_reason": "ENTRY_PULLBACK_CONFIRM",
                        "entry_style_selected": "PULLBACK",
                        "trade_horizon": "SWING",
                        "exit_policy_family": "SWING_STAGED_EXIT",
                        "entry_thesis": "PB1_PULLBACK",
                    },
                },
                response_json={"rt_cd": "0"},
                created_at=ts,
                submitted_at=ts,
                acked_at=ts,
            )
        )


def test_reconciled_order_upsert_preserves_sell_application_watermark() -> None:
    engine = _db()
    schema = schema_for_engine(engine)
    _install_order_response_merge_guard()
    repo = OrdersRepo(engine)
    ts = now_kst()
    order_id = str(uuid4())
    with engine.begin() as conn:
        conn.execute(
            sa.insert(schema.orders).values(
                order_id=order_id,
                env="practice",
                strategy=STRATEGY,
                sid=1,
                mode=1,
                code="122630",
                market="KOSPI",
                side="SELL",
                ord_type="MARKET",
                qty=3,
                stage="TP1",
                client_order_key="sell-watermark-test",
                status="FILLED",
                kis_odno="0000099999",
                broker_order_id="0000099999",
                request_json={},
                response_json={
                    "rt_cd": "0",
                    "broker_truth_sell_applied": {
                        "qty": 3,
                        "notional": 360000.0,
                        "fee": 0.0,
                        "tax": 0.0,
                    },
                },
                created_at=ts,
                submitted_at=ts,
                acked_at=ts,
            )
        )

    repo.upsert_reconciled_order(
        env="practice",
        run_id=None,
        strategy=STRATEGY,
        sid=1,
        mode=1,
        code="122630",
        market="KOSPI",
        side="SELL",
        ord_type="MARKET",
        qty=3,
        limit_price=None,
        stage="TP1",
        client_order_key="sell-watermark-test",
        kis_odno="0000099999",
        status="FILLED",
        request_json={"source": "daily_ccld"},
        response_json={"rt_cd": "0", "msg1": "latest reconcile row"},
        submitted_at=ts,
        acked_at=ts,
    )

    with engine.connect() as conn:
        response = conn.execute(
            sa.select(schema.orders.c.response_json).where(schema.orders.c.order_id == order_id)
        ).scalar_one()
    assert response["msg1"] == "latest reconcile row"
    assert response["broker_truth_sell_applied"]["qty"] == 3
    assert response["broker_truth_sell_applied"]["notional"] == 360000.0


def test_owned_buy_fill_without_position_is_retried_and_applied_once() -> None:
    engine = _db()
    schema = schema_for_engine(engine)
    epoch_id = str(uuid4())
    cycle_id = str(uuid4())
    order_id = str(uuid4())
    odno = "0000077777"
    _epoch(engine, epoch_id)
    _order(engine, order_id=order_id, cycle_id=cycle_id, epoch_id=epoch_id, odno=odno, qty=5, baseline=0)
    ts = now_kst()
    FillsRepo(engine).upsert_fill(
        env="practice", run_id=None, order_id=order_id, kis_odno=odno,
        trade_id="owned-buy-1", code="005930", market="KOSPI", side="BUY",
        qty=5, price=100000.0, fee=0.0, tax=0.0, filled_at=ts,
        raw_json={"source": "daily_ccld"}, fill_meta_json={"fill_source": "daily_ccld"},
        position_cycle_id=cycle_id, portfolio_epoch_id=epoch_id,
    )

    first = _link_buy_fills_retry_safe(engine=engine, env="practice", strategy=STRATEGY)
    assert first == {"linked_fills": 0, "positions_promoted": 1}
    with engine.connect() as conn:
        pos = dict(conn.execute(sa.select(schema.positions)).mappings().one())
    assert int(pos["qty"]) == 5
    assert float(pos["total_cost"]) == 500000.0
    marker = dict((pos["entry_meta_json"] or {}).get(_BUY_APPLIED_KEY) or {})
    assert int(marker[order_id]["qty"]) == 5

    second = _link_buy_fills_retry_safe(engine=engine, env="practice", strategy=STRATEGY)
    assert second == {"linked_fills": 0, "positions_promoted": 0}
    with engine.connect() as conn:
        pos2 = dict(conn.execute(sa.select(schema.positions)).mappings().one())
    assert int(pos2["qty"]) == 5
    assert float(pos2["total_cost"]) == 500000.0


def test_owned_adaptive_buy_fill_retries_against_immutable_baseline() -> None:
    engine = _db()
    schema = schema_for_engine(engine)
    epoch_id = str(uuid4())
    cycle_id = str(uuid4())
    order_id = str(uuid4())
    odno = "0000088888"
    _epoch(engine, epoch_id)
    _order(engine, order_id=order_id, cycle_id=cycle_id, epoch_id=epoch_id, odno=odno, qty=2, baseline=6)
    ts = now_kst()
    with engine.begin() as conn:
        conn.execute(
            sa.insert(schema.positions).values(
                position_id=str(uuid4()),
                position_cycle_id=cycle_id,
                portfolio_epoch_id=epoch_id,
                opened_at=ts,
                position_origin="SYSTEM",
                env="practice",
                strategy=STRATEGY,
                sid=1,
                mode=1,
                code="005930",
                market="KOSPI",
                qty=6,
                avg_buy_price=100000.0,
                total_cost=600000.0,
                realized_pnl=0.0,
                status="OPEN",
                entry_meta_json={"entry_reason": "ENTRY_PULLBACK_CONFIRM"},
                entry_exit_plan_json={},
                position_meta={},
            )
        )
    FillsRepo(engine).upsert_fill(
        env="practice", run_id=None, order_id=order_id, kis_odno=odno,
        trade_id="owned-buy-add", code="005930", market="KOSPI", side="BUY",
        qty=2, price=90000.0, fee=0.0, tax=0.0, filled_at=ts,
        raw_json={"source": "daily_ccld"}, fill_meta_json={"fill_source": "daily_ccld"},
        position_cycle_id=cycle_id, portfolio_epoch_id=epoch_id,
    )

    result = _link_buy_fills_retry_safe(engine=engine, env="practice", strategy=STRATEGY)
    assert result == {"linked_fills": 0, "positions_promoted": 1}
    with engine.connect() as conn:
        pos = dict(conn.execute(sa.select(schema.positions)).mappings().one())
    assert int(pos["qty"]) == 8
    assert float(pos["total_cost"]) == 780000.0
    marker = dict((pos["entry_meta_json"] or {}).get(_BUY_APPLIED_KEY) or {})
    assert int(marker[order_id]["qty"]) == 2
