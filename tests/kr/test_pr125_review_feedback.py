from __future__ import annotations

from datetime import date, datetime
from uuid import uuid4

import sqlalchemy as sa

from trader.db.repos import FillsRepo
from trader.db.schema import schema_for_engine
from trader.kr.broker_truth_review_fixes import (
    _health_after_reconcile_fixed,
    _link_unowned_daily_fills_fixed,
    _recover_proven_policy_positions_fixed,
    _update_exact_open_position_fields,
)
from trader.kr.infinite.config import InfiniteConfig
from trader.kr.infinite.models import Action, BrokerPosition, State, Status
from trader.kr.infinite.strategy import evaluate
from trader.reconcile_db import close_stale_positions_guarded
from trader.time_utils import now_kst


STRATEGY = "pb1_pullback_close"


def _db():
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    return engine


def _insert_open_position(
    engine,
    *,
    code: str,
    qty: int,
    avg: float,
    position_id: str | None = None,
    cycle_id: str | None = None,
    epoch_id: str | None = None,
    exit_policy_family: str | None = None,
) -> dict[str, str]:
    schema = schema_for_engine(engine)
    ids = {
        "position_id": position_id or str(uuid4()),
        "cycle_id": cycle_id or str(uuid4()),
        "epoch_id": epoch_id or str(uuid4()),
    }
    with engine.begin() as conn:
        conn.execute(
            sa.insert(schema.positions).values(
                position_id=ids["position_id"],
                position_cycle_id=ids["cycle_id"],
                portfolio_epoch_id=ids["epoch_id"],
                opened_at=now_kst(),
                position_origin="SYSTEM",
                env="practice",
                strategy=STRATEGY,
                sid=1,
                mode=1,
                code=code,
                market="KOSPI",
                qty=qty,
                avg_buy_price=avg,
                total_cost=qty * avg,
                realized_pnl=0.0,
                status="OPEN",
                exit_policy_family=exit_policy_family,
                entry_meta_json={},
                entry_exit_plan_json={},
                position_meta={},
            )
        )
    return ids


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
    _insert_open_position(engine, code="005930", qty=5, avg=100_000.0)
    health = _health_after_reconcile_fixed(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
        holdings_rows=[],
    )
    assert health["qty_mismatch_count"] == 1
    assert health["qty_mismatches"] == [{"code": "005930", "db_qty": 5, "kis_qty": 0}]


def test_authoritative_reconcile_refuses_multiple_open_lifecycles() -> None:
    """One KIS aggregate must never be copied into two simultaneous OPEN rows."""
    engine = _db()
    schema = schema_for_engine(engine)
    first = _insert_open_position(engine, code="122630", qty=1, avg=110_000.0)
    second = _insert_open_position(engine, code="122630", qty=2, avg=111_000.0)

    kis_balance = {
        "output1": [
            {"pdno": "122630", "hldg_qty": "3", "ord_psbl_qty": "3", "pchs_avg_pric": "110500"}
        ]
    }
    close_stale_positions_guarded(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
        reason="daily_reconcile",
        ts=datetime(2026, 9, 11, 15, 30, 0),
        kis_balance=kis_balance,
        sell_fill_codes=[],
        runtime_dir=None,
    )

    with engine.connect() as conn:
        rows = conn.execute(
            sa.select(schema.positions.c.position_id, schema.positions.c.qty, schema.positions.c.total_cost)
            .where(schema.positions.c.code == "122630")
            .order_by(schema.positions.c.position_id)
        ).mappings().all()
    qty_by_id = {str(row["position_id"]): int(row["qty"]) for row in rows}
    assert qty_by_id[first["position_id"]] == 1
    assert qty_by_id[second["position_id"]] == 2

    health = _health_after_reconcile_fixed(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
        holdings_rows=kis_balance["output1"],
    )
    assert health["duplicate_open_lifecycle_count"] == 1
    assert health["qty_mismatch_count"] == 1
    assert health["qty_mismatches"][0]["reason"] == "MULTIPLE_OPEN_LIFECYCLES"
    assert health["qty_mismatches"][0]["db_qty"] == 3
    assert health["qty_mismatches"][0]["kis_qty"] == 3


def test_exact_position_update_never_writes_sibling_open_lifecycle() -> None:
    engine = _db()
    schema = schema_for_engine(engine)
    first = _insert_open_position(engine, code="067290", qty=10, avg=2_000.0, exit_policy_family="POLICY_MISSING")
    second = _insert_open_position(engine, code="067290", qty=5, avg=2_100.0, exit_policy_family="POLICY_MISSING")

    with engine.connect() as conn:
        selected = dict(
            conn.execute(
                sa.select(schema.positions).where(schema.positions.c.position_id == first["position_id"])
            ).mappings().one()
        )
    assert _update_exact_open_position_fields(
        engine=engine,
        schema=schema,
        position=selected,
        fields={"exit_policy_family": "SWING_STAGED_EXIT", "policy_source": "exact-row-test"},
    )

    with engine.connect() as conn:
        rows = conn.execute(
            sa.select(schema.positions.c.position_id, schema.positions.c.exit_policy_family, schema.positions.c.policy_source)
            .where(schema.positions.c.code == "067290")
        ).mappings().all()
    by_id = {str(row["position_id"]): dict(row) for row in rows}
    assert by_id[first["position_id"]]["exit_policy_family"] == "SWING_STAGED_EXIT"
    assert by_id[first["position_id"]]["policy_source"] == "exact-row-test"
    assert by_id[second["position_id"]]["exit_policy_family"] == "POLICY_MISSING"
    assert by_id[second["position_id"]]["policy_source"] is None


def test_policy_recovery_refuses_multiple_open_lifecycles_without_cross_write() -> None:
    engine = _db()
    schema = schema_for_engine(engine)
    first = _insert_open_position(engine, code="039030", qty=2, avg=500_000.0, exit_policy_family="POLICY_MISSING")
    second = _insert_open_position(engine, code="039030", qty=2, avg=500_000.0, exit_policy_family="POLICY_MISSING")

    result = _recover_proven_policy_positions_fixed(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
        holdings_rows=[{"pdno": "039030", "hldg_qty": "2", "pchs_avg_pric": "500000"}],
    )
    assert result == {"recovered": [], "review_required": ["039030"]}

    with engine.connect() as conn:
        rows = conn.execute(
            sa.select(schema.positions.c.position_id, schema.positions.c.exit_policy_family, schema.positions.c.policy_source)
            .where(schema.positions.c.code == "039030")
        ).mappings().all()
    by_id = {str(row["position_id"]): dict(row) for row in rows}
    for position_id in (first["position_id"], second["position_id"]):
        assert by_id[position_id]["exit_policy_family"] == "POLICY_MISSING"
        assert by_id[position_id]["policy_source"] is None
