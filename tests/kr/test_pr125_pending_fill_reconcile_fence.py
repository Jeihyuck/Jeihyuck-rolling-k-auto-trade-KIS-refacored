from __future__ import annotations

from uuid import uuid4

import sqlalchemy as sa

from trader.db.repos import FillsRepo
from trader.db.schema import schema_for_engine
from trader.kr.broker_truth_pending_fill_fence import (
    _close_stale_positions_guarded_with_fill_fence,
    _pending_fill_application_codes,
)
from trader.kr.broker_truth_sell_fixes import _atomic_link_and_apply_sell
from trader.time_utils import now_kst


STRATEGY = "pb1_pullback_close"


def _db():
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    return engine


def _epoch(conn, schema, epoch_id: str, account: str) -> None:
    conn.execute(
        sa.insert(schema.portfolio_epochs).values(
            portfolio_epoch_id=epoch_id,
            env="practice",
            account_id=account,
            sid=1,
            mode=1,
            strategy=STRATEGY,
            status="ACTIVE",
        )
    )


def test_owned_buy_fill_waiting_for_position_application_blocks_kis_qty_overwrite() -> None:
    engine = _db()
    schema = schema_for_engine(engine)
    epoch_id = str(uuid4())
    cycle_id = str(uuid4())
    position_id = str(uuid4())
    order_id = str(uuid4())
    ts = now_kst()

    with engine.begin() as conn:
        _epoch(conn, schema, epoch_id, "practice:pending-buy")
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
                avg_buy_price=100.0,
                total_cost=200.0,
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
                code="005930",
                market="KOSPI",
                side="BUY",
                ord_type="MARKET",
                qty=3,
                stage="ENTRY",
                client_order_key="pending-buy-before-qty-reconcile",
                status="FILLED",
                kis_odno="0000070707",
                broker_order_id="0000070707",
                request_json={"pre_order_holding_qty": 2, "pre_order_avg_price": 100.0},
                response_json={"rt_cd": "0"},
                created_at=ts,
                submitted_at=ts,
                acked_at=ts,
            )
        )

    # Fill is already owned, but the crash happened before PositionsRepo.apply_fill.
    FillsRepo(engine).upsert_fill(
        env="practice",
        run_id=None,
        order_id=order_id,
        kis_odno="0000070707",
        trade_id="pending-buy-fill",
        code="005930",
        market="KOSPI",
        side="BUY",
        qty=3,
        price=110.0,
        fee=0.0,
        tax=0.0,
        filled_at=ts,
        raw_json={"source": "daily_ccld"},
        fill_meta_json={"fill_source": "daily_ccld"},
        position_cycle_id=cycle_id,
        portfolio_epoch_id=epoch_id,
    )

    assert _pending_fill_application_codes(
        engine=engine, env="practice", strategy=STRATEGY
    ) == {"005930"}

    # Broker already shows 5, but DB must remain at the immutable baseline 2
    # until the fill applier atomically updates qty + metadata watermark.
    _close_stale_positions_guarded_with_fill_fence(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
        reason="exit_phase",
        ts=ts,
        kis_balance={
            "output1": [
                {
                    "pdno": "005930",
                    "hldg_qty": "5",
                    "ord_psbl_qty": "5",
                    "pchs_avg_pric": "106",
                }
            ]
        },
        runtime_dir=None,
    )
    with engine.connect() as conn:
        position = dict(
            conn.execute(sa.select(schema.positions).where(schema.positions.c.position_id == position_id))
            .mappings()
            .one()
        )
    assert int(position["qty"]) == 2
    assert float(position["total_cost"]) == 200.0


def test_unowned_sell_fill_blocks_kis_qty_overwrite_until_realized_pnl_is_applied() -> None:
    engine = _db()
    schema = schema_for_engine(engine)
    epoch_id = str(uuid4())
    cycle_id = str(uuid4())
    position_id = str(uuid4())
    order_id = str(uuid4())
    ts = now_kst()

    with engine.begin() as conn:
        _epoch(conn, schema, epoch_id, "practice:pending-sell")
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
                client_order_key="pending-sell-before-qty-reconcile",
                status="ACKED",
                kis_odno="0000080808",
                broker_order_id="0000080808",
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
        kis_odno="0000080808",
        trade_id="pending-sell-fill",
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

    assert _pending_fill_application_codes(
        engine=engine, env="practice", strategy=STRATEGY
    ) == {"122630"}

    # KIS already proves 6 -> 3, but changing DB qty here would destroy the
    # pre-sell basis needed for exact realized-P&L accounting.
    _close_stale_positions_guarded_with_fill_fence(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
        reason="exit_phase",
        ts=ts,
        kis_balance={
            "output1": [
                {
                    "pdno": "122630",
                    "hldg_qty": "3",
                    "ord_psbl_qty": "3",
                    "pchs_avg_pric": "100",
                }
            ]
        },
        runtime_dir=None,
    )
    with engine.connect() as conn:
        before = dict(
            conn.execute(sa.select(schema.positions).where(schema.positions.c.position_id == position_id))
            .mappings()
            .one()
        )
        fill_id = conn.execute(sa.select(schema.fills.c.fill_id)).scalar_one()
    assert int(before["qty"]) == 6
    assert float(before["realized_pnl"]) == 0.0

    # The exact SELL applier can now consume the original 6-share basis.
    linked, applied = _atomic_link_and_apply_sell(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
        kis_odno="0000080808",
        fill_ids=[fill_id],
    )
    assert (linked, applied) == (1, 1)
    assert _pending_fill_application_codes(
        engine=engine, env="practice", strategy=STRATEGY
    ) == set()

    with engine.connect() as conn:
        after = dict(
            conn.execute(sa.select(schema.positions).where(schema.positions.c.position_id == position_id))
            .mappings()
            .one()
        )
    assert int(after["qty"]) == 3
    assert float(after["total_cost"]) == 300.0
    assert float(after["realized_pnl"]) == 54.0
