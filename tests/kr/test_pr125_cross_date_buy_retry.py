from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import sqlalchemy as sa

from trader.db.repos import FillsRepo
from trader.db.schema import schema_for_engine
from trader.kr.broker_truth_final_review_fixes import _BUY_APPLIED_KEY
from trader.kr.broker_truth_historical_buy_retry import _retry_owned_buy_fills_all_dates
from trader.time_utils import now_kst


STRATEGY = "pb1_pullback_close"


def _engine():
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    return engine


def test_owned_buy_fill_from_prior_kst_date_is_retried_into_position() -> None:
    engine = _engine()
    schema = schema_for_engine(engine)
    epoch_id = str(uuid4())
    cycle_id = str(uuid4())
    order_id = str(uuid4())
    odno = "0000042424"
    fill_time = now_kst() - timedelta(days=1, hours=1)

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
                client_order_key="prior-day-owned-buy",
                status="FILLED",
                kis_odno=odno,
                broker_order_id=odno,
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
                created_at=fill_time,
                submitted_at=fill_time,
                acked_at=fill_time,
            )
        )

    # Simulate the precise crash window: the fill is already durably attributed
    # to yesterday's order, but no position accounting was committed before the
    # process died. A current-day-only query cannot see this fill.
    FillsRepo(engine).upsert_fill(
        env="practice",
        run_id=None,
        order_id=order_id,
        kis_odno=odno,
        trade_id="prior-day-owned-fill",
        code="005930",
        market="KOSPI",
        side="BUY",
        qty=4,
        price=101000.0,
        fee=0.0,
        tax=0.0,
        filled_at=fill_time,
        raw_json={"source": "daily_ccld"},
        fill_meta_json={"fill_source": "daily_ccld"},
        position_cycle_id=cycle_id,
        portfolio_epoch_id=epoch_id,
    )

    assert FillsRepo(engine).list_today_fills("practice", side="BUY") == []

    promoted = _retry_owned_buy_fills_all_dates(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
    )
    assert promoted == 1

    with engine.connect() as conn:
        pos = dict(conn.execute(sa.select(schema.positions)).mappings().one())
    assert int(pos["qty"]) == 4
    assert float(pos["total_cost"]) == 404000.0
    assert str(pos["entry_ts"]).startswith(str(fill_time.date()))
    marker = dict((pos["entry_meta_json"] or {}).get(_BUY_APPLIED_KEY) or {})
    assert int(marker[order_id]["qty"]) == 4

    # Re-running after another restart is idempotent.
    assert _retry_owned_buy_fills_all_dates(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
    ) == 0
    with engine.connect() as conn:
        pos2 = dict(conn.execute(sa.select(schema.positions)).mappings().one())
    assert int(pos2["qty"]) == 4
    assert float(pos2["total_cost"]) == 404000.0
