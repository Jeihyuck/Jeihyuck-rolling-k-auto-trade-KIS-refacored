from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import sqlalchemy as sa

from trader.db.repos import FillsRepo
from trader.db.schema import schema_for_engine
from trader.kr.broker_truth_cross_date_unowned_retry import _retry_historical_unowned_fills
from trader.kr.broker_truth_historical_buy_retry import _retry_owned_buy_fills_all_dates
from trader.time_utils import now_kst


STRATEGY = "pb1_pullback_close"


def _db():
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    return engine


def test_prior_day_unowned_buy_is_linked_then_applied_to_exact_lifecycle() -> None:
    engine = _db()
    schema = schema_for_engine(engine)
    epoch_id = str(uuid4())
    cycle_id = str(uuid4())
    order_id = str(uuid4())
    ts = now_kst() - timedelta(days=1, hours=1)

    with engine.begin() as conn:
        conn.execute(
            sa.insert(schema.portfolio_epochs).values(
                portfolio_epoch_id=epoch_id,
                env="practice",
                account_id="practice:cross-date-buy",
                sid=1,
                mode=1,
                strategy=STRATEGY,
                status="ACTIVE",
            )
        )
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
                qty=4,
                stage="ENTRY",
                client_order_key="cross-date-unowned-buy",
                status="FILLED",
                kis_odno="0000042424",
                broker_order_id="0000042424",
                request_json={
                    "pre_order_holding_qty": 0,
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

    # Precise crash window: daily-ccld fill committed, process died before the
    # post-reconcile linker could attach order_id/cycle/epoch.
    FillsRepo(engine).upsert_fill(
        env="practice",
        run_id=None,
        order_id=None,
        kis_odno="0000042424",
        trade_id="cross-date-unowned-buy-fill",
        code="005930",
        market="KOSPI",
        side="BUY",
        qty=4,
        price=101000.0,
        fee=0.0,
        tax=0.0,
        filled_at=ts,
        raw_json={"source": "daily_ccld"},
        fill_meta_json={"fill_source": "daily_ccld"},
    )

    repaired = _retry_historical_unowned_fills(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
    )
    assert repaired == {"linked_fills": 1, "positions_promoted": 0}

    with engine.connect() as conn:
        fill = dict(conn.execute(sa.select(schema.fills)).mappings().one())
    assert str(fill["order_id"]) == order_id
    assert str(fill["position_cycle_id"]) == cycle_id
    assert str(fill["portfolio_epoch_id"]) == epoch_id

    # Once attributed, the existing cross-date owned-BUY retry performs the
    # atomic quantity/cost/entry-metadata application.
    assert _retry_owned_buy_fills_all_dates(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
    ) == 1
    with engine.connect() as conn:
        position = dict(conn.execute(sa.select(schema.positions)).mappings().one())
    assert str(position["position_cycle_id"]) == cycle_id
    assert int(position["qty"]) == 4
    assert float(position["total_cost"]) == 404000.0
    assert position["exit_policy_family"] == "SWING_STAGED_EXIT"

    assert _retry_historical_unowned_fills(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
    ) == {"linked_fills": 0, "positions_promoted": 0}
    assert _retry_owned_buy_fills_all_dates(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
    ) == 0


def test_prior_day_unowned_sell_is_applied_with_realized_pnl_once() -> None:
    engine = _db()
    schema = schema_for_engine(engine)
    epoch_id = str(uuid4())
    cycle_id = str(uuid4())
    position_id = str(uuid4())
    order_id = str(uuid4())
    ts = now_kst() - timedelta(days=1, hours=1)

    with engine.begin() as conn:
        conn.execute(
            sa.insert(schema.portfolio_epochs).values(
                portfolio_epoch_id=epoch_id,
                env="practice",
                account_id="practice:cross-date-sell",
                sid=1,
                mode=1,
                strategy=STRATEGY,
                status="ACTIVE",
            )
        )
        conn.execute(
            sa.insert(schema.positions).values(
                position_id=position_id,
                position_cycle_id=cycle_id,
                portfolio_epoch_id=epoch_id,
                opened_at=ts - timedelta(days=2),
                position_origin="SYSTEM",
                env="practice",
                strategy=STRATEGY,
                sid=1,
                mode=1,
                code="122630",
                market="KOSPI",
                qty=6,
                avg_buy_price=100.0,
                total_cost=600.0,
                realized_pnl=0.0,
                status="OPEN",
                entry_meta_json={},
                entry_exit_plan_json={},
                position_meta={},
            )
        )
        conn.execute(
            sa.insert(schema.orders).values(
                order_id=order_id,
                position_cycle_id=cycle_id,
                portfolio_epoch_id=epoch_id,
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
                client_order_key="cross-date-unowned-sell",
                status="ACKED",
                kis_odno="0000051515",
                broker_order_id="0000051515",
                request_json={},
                response_json={"rt_cd": "0"},
                created_at=ts,
                submitted_at=ts,
                acked_at=ts,
            )
        )

    FillsRepo(engine).upsert_fill(
        env="practice",
        run_id=None,
        order_id=None,
        kis_odno="0000051515",
        trade_id="cross-date-unowned-sell-fill",
        code="122630",
        market="KOSPI",
        side="SELL",
        qty=3,
        price=120.0,
        fee=3.0,
        tax=3.0,
        filled_at=ts,
        raw_json={"source": "daily_ccld"},
        fill_meta_json={"fill_source": "daily_ccld"},
    )

    repaired = _retry_historical_unowned_fills(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
    )
    assert repaired == {"linked_fills": 1, "positions_promoted": 1}

    with engine.connect() as conn:
        fill = dict(conn.execute(sa.select(schema.fills)).mappings().one())
        order = dict(
            conn.execute(sa.select(schema.orders).where(schema.orders.c.order_id == order_id)).mappings().one()
        )
        position = dict(
            conn.execute(sa.select(schema.positions).where(schema.positions.c.position_id == position_id)).mappings().one()
        )

    assert str(fill["order_id"]) == order_id
    assert order["status"] == "FILLED"
    assert int(position["qty"]) == 3
    assert float(position["total_cost"]) == 300.0
    # proceeds 360 - fee3 - tax3 = 354; sold cost basis = 300.
    assert float(position["realized_pnl"]) == 54.0
    marker = dict((order["response_json"] or {}).get("broker_truth_sell_applied") or {})
    assert int(marker["qty"]) == 3

    # No second realization on restart/retry.
    assert _retry_historical_unowned_fills(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
    ) == {"linked_fills": 0, "positions_promoted": 0}
    with engine.connect() as conn:
        position2 = dict(
            conn.execute(sa.select(schema.positions).where(schema.positions.c.position_id == position_id)).mappings().one()
        )
    assert int(position2["qty"]) == 3
    assert float(position2["realized_pnl"]) == 54.0
