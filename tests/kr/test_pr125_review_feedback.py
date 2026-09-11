from __future__ import annotations

from datetime import date
from uuid import uuid4

import sqlalchemy as sa

from trader.db.repos import FillsRepo
from trader.db.schema import schema_for_engine
from trader.kr.broker_truth_review_fixes import (
    _health_after_reconcile_fixed,
    _link_unowned_daily_fills_fixed,
)
from trader.kr.infinite.config import InfiniteConfig
from trader.kr.infinite.models import Action, BrokerPosition, State, Status
from trader.kr.infinite.strategy import evaluate
from trader.time_utils import now_kst


STRATEGY = "pb1_pullback_close"


def _db():
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    return engine


def test_adaptive_add_buy_carries_immutable_broker_baseline() -> None:
    state = State(
        cycle_id="KRINF-20260901-test",
        cycle_start_date=date(2026, 9, 1),
        allocated_capital_krw=10_000_000.0,
        unit_krw=1_000_000.0,
        core_filled_notional=663_000.0,
        units_used=1,
        core_units_used=1,
        status=Status.ACTIVE,
        last_buy_price=110_500.0,
    )
    position = BrokerPosition(qty=6, orderable_qty=6, average_price=110_500.0, current_price=100_000.0)
    decision = evaluate(
        config=InfiniteConfig(),
        state=state,
        position=position,
        trade_date=date(2026, 9, 11),
        market_state="KR_NORMAL",
        trading_days_since_last_buy=2,
        orderable_cash=5_000_000.0,
        best_ask=100_000.0,
    )
    assert decision.action == Action.BUY
    assert decision.reason == "ADAPTIVE_ADD_BUY"
    assert decision.metadata["pre_order_holding_qty"] == 6
    assert decision.metadata["pre_order_avg_price"] == 110_500.0
    assert decision.metadata["reconcile_baseline_source"] == "KR_INFINITE_STRATEGY_BROKER_POSITION"


def test_split_daily_ccld_buy_3_plus_2_promotes_full_five_share_position_once() -> None:
    engine = _db()
    schema = schema_for_engine(engine)
    order_id = str(uuid4())
    cycle_id = str(uuid4())
    epoch_id = str(uuid4())
    ts = now_kst()
    entry_meta = {
        "entry_reason": "ENTRY_PULLBACK_CONFIRM",
        "entry_style_selected": "PULLBACK",
        "trade_horizon": "SWING",
        "exit_policy_family": "SWING",
    }
    entry_plan = {
        **entry_meta,
        "entry_thesis": "PB1_PULLBACK",
        "policy_source": "pb1_entry_plan",
        "policy_version": "review-fix-v1",
        "risk_plan": {"initial_stop": 95_000.0, "risk_R": 5_000.0},
        "time_plan": {"max_trading_days": 20},
    }
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
                qty=5,
                stage="ENTRY",
                client_order_key="split-fill-review-test",
                status="ACKED",
                kis_odno="0000012345",
                broker_order_id="0000012345",
                entry_meta_json=entry_meta,
                request_json={
                    "pre_order_holding_qty": 0,
                    "entry_meta": entry_meta,
                    "entry_exit_plan": entry_plan,
                },
                response_json={"rt_cd": "0"},
                created_at=ts,
                submitted_at=ts,
                acked_at=ts,
            )
        )
    fills = FillsRepo(engine)
    fills.upsert_fill(
        env="practice", run_id=None, order_id=None, kis_odno="0000012345",
        trade_id="split-1", code="005930", market="KOSPI", side="BUY",
        qty=3, price=100_000.0, fee=0.0, tax=0.0, filled_at=ts,
        raw_json={"source": "daily_ccld"}, fill_meta_json={"fill_source": "daily_ccld"},
    )
    fills.upsert_fill(
        env="practice", run_id=None, order_id=None, kis_odno="0000012345",
        trade_id="split-2", code="005930", market="KOSPI", side="BUY",
        qty=2, price=101_000.0, fee=0.0, tax=0.0, filled_at=ts,
        raw_json={"source": "daily_ccld"}, fill_meta_json={"fill_source": "daily_ccld"},
    )

    result = _link_unowned_daily_fills_fixed(engine=engine, env="practice", strategy=STRATEGY)
    assert result == {"linked_fills": 2, "positions_promoted": 1}

    with engine.connect() as conn:
        order = conn.execute(sa.select(schema.orders).where(schema.orders.c.order_id == order_id)).mappings().one()
        pos = conn.execute(sa.select(schema.positions)).mappings().one()
        linked_count = conn.execute(
            sa.select(sa.func.count()).select_from(schema.fills).where(schema.fills.c.order_id == order_id)
        ).scalar_one()
    assert order["status"] == "FILLED"
    assert linked_count == 2
    assert int(pos["qty"]) == 5
    assert abs(float(pos["avg_buy_price"]) - 100_400.0) < 0.01
    applied = dict((pos["position_meta"] or {}).get("broker_truth_applied_orders") or {})
    assert int(applied[order_id]["qty"]) == 5

    # Re-running must not double-apply already linked executions.
    again = _link_unowned_daily_fills_fixed(engine=engine, env="practice", strategy=STRATEGY)
    assert again == {"linked_fills": 0, "positions_promoted": 0}
    with engine.connect() as conn:
        pos2 = conn.execute(sa.select(schema.positions)).mappings().one()
    assert int(pos2["qty"]) == 5


def test_health_reports_db_positive_symbol_absent_from_fresh_kis_as_zero() -> None:
    engine = _db()
    schema = schema_for_engine(engine)
    with engine.begin() as conn:
        conn.execute(
            sa.insert(schema.positions).values(
                position_id=str(uuid4()),
                position_cycle_id=str(uuid4()),
                portfolio_epoch_id=str(uuid4()),
                opened_at=now_kst(),
                position_origin="SYSTEM",
                env="practice",
                strategy=STRATEGY,
                sid=1,
                mode=1,
                code="005930",
                market="KOSPI",
                qty=5,
                avg_buy_price=100_000.0,
                total_cost=500_000.0,
                realized_pnl=0.0,
                status="OPEN",
                entry_meta_json={},
                entry_exit_plan_json={},
                position_meta={},
            )
        )
    health = _health_after_reconcile_fixed(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
        holdings_rows=[],
    )
    assert health["qty_mismatch_count"] == 1
    assert health["qty_mismatches"] == [{"code": "005930", "db_qty": 5, "kis_qty": 0}]
