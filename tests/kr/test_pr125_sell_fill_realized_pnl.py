from __future__ import annotations

from uuid import uuid4

import sqlalchemy as sa

from trader.db.repos import FillsRepo
from trader.db.schema import schema_for_engine
from trader.kr.broker_truth_sell_fixes import _link_unowned_daily_fills_with_sell
from trader.time_utils import now_kst


STRATEGY = "pb1_pullback_close"


def _db():
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    return engine


def test_linked_partial_sell_updates_exact_position_and_realized_pnl_once() -> None:
    engine = _db()
    schema = schema_for_engine(engine)
    order_id = str(uuid4())
    position_id = str(uuid4())
    cycle_id = str(uuid4())
    epoch_id = str(uuid4())
    ts = now_kst()

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
        conn.execute(
            sa.insert(schema.positions).values(
                position_id=position_id,
                position_cycle_id=cycle_id,
                portfolio_epoch_id=epoch_id,
                opened_at=ts,
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
                client_order_key="sell-realized-pnl-test",
                status="ACKED",
                kis_odno="0000099999",
                broker_order_id="0000099999",
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
        kis_odno="0000099999",
        trade_id="sell-fill-1",
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

    result = _link_unowned_daily_fills_with_sell(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
    )
    assert result == {"linked_fills": 1, "positions_promoted": 1}

    with engine.connect() as conn:
        order = conn.execute(
            sa.select(schema.orders).where(schema.orders.c.order_id == order_id)
        ).mappings().one()
        position = conn.execute(
            sa.select(schema.positions).where(schema.positions.c.position_id == position_id)
        ).mappings().one()
        fill = conn.execute(sa.select(schema.fills)).mappings().one()

    # proceeds = 3*120 - fee3 - tax3 = 354; cost basis = 3*100 = 300
    assert order["status"] == "FILLED"
    assert str(fill["order_id"]) == order_id
    assert str(fill["position_cycle_id"]) == cycle_id
    assert str(fill["portfolio_epoch_id"]) == epoch_id
    assert int(position["qty"]) == 3
    assert float(position["avg_buy_price"]) == 100.0
    assert float(position["total_cost"]) == 300.0
    assert float(position["realized_pnl"]) == 54.0
    applied = dict((order["response_json"] or {}).get("broker_truth_sell_applied") or {})
    assert int(applied["qty"]) == 3
    assert float(applied["notional"]) == 360.0

    # The fill is now owned; a second reconciliation cannot realize it twice.
    again = _link_unowned_daily_fills_with_sell(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
    )
    assert again == {"linked_fills": 0, "positions_promoted": 0}
    with engine.connect() as conn:
        position2 = conn.execute(
            sa.select(schema.positions).where(schema.positions.c.position_id == position_id)
        ).mappings().one()
    assert int(position2["qty"]) == 3
    assert float(position2["total_cost"]) == 300.0
    assert float(position2["realized_pnl"]) == 54.0


def test_full_sell_closes_exact_lifecycle_and_preserves_realized_pnl() -> None:
    engine = _db()
    schema = schema_for_engine(engine)
    order_id = str(uuid4())
    position_id = str(uuid4())
    cycle_id = str(uuid4())
    epoch_id = str(uuid4())
    ts = now_kst()

    with engine.begin() as conn:
        conn.execute(
            sa.insert(schema.portfolio_epochs).values(
                portfolio_epoch_id=epoch_id,
                env="practice",
                account_id="practice:test-full",
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
                opened_at=ts,
                position_origin="SYSTEM",
                env="practice",
                strategy=STRATEGY,
                sid=1,
                mode=1,
                code="005930",
                market="KOSPI",
                qty=2,
                avg_buy_price=90.0,
                total_cost=180.0,
                realized_pnl=10.0,
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
                code="005930",
                market="KOSPI",
                side="SELL",
                ord_type="MARKET",
                qty=2,
                stage="EXIT",
                client_order_key="sell-full-test",
                status="ACKED",
                kis_odno="0000088888",
                broker_order_id="0000088888",
                request_json={},
                response_json={},
                created_at=ts,
                submitted_at=ts,
                acked_at=ts,
            )
        )

    FillsRepo(engine).upsert_fill(
        env="practice", run_id=None, order_id=None, kis_odno="0000088888",
        trade_id="sell-full-1", code="005930", market="KOSPI", side="SELL",
        qty=2, price=100.0, fee=0.0, tax=0.0, filled_at=ts,
        raw_json={"source": "daily_ccld"}, fill_meta_json={"fill_source": "daily_ccld"},
    )

    _link_unowned_daily_fills_with_sell(engine=engine, env="practice", strategy=STRATEGY)
    with engine.connect() as conn:
        position = conn.execute(
            sa.select(schema.positions).where(schema.positions.c.position_id == position_id)
        ).mappings().one()
    assert int(position["qty"]) == 0
    assert float(position["total_cost"]) == 0.0
    assert float(position["realized_pnl"]) == 30.0  # prior10 + (200 - 180)
    assert position["status"] == "CLOSED"
    assert position["closed_reason"] == "FULL_SELL"
