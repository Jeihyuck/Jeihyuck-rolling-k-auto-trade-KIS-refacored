from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import sqlalchemy as sa

from trader.db.repos import FillsRepo
from trader.db.schema import schema_for_engine
from trader.kr.broker_truth_hardening import (
    _health_after_reconcile,
    _link_unowned_daily_fills,
    _recover_proven_policy_positions,
)
from trader.time_utils import now_kst


STRATEGY = "pb1_pullback_close"


def _db():
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    return engine


def _ids():
    return str(uuid4()), str(uuid4()), str(uuid4())


def test_post_tick_runtime_binding_resolves_db_engine_from_orders_repo(monkeypatch) -> None:
    """The live PB1 object need not expose .engine directly; its repository does."""
    import trader
    import trader.kr.broker_truth_hardening as broker_truth

    marker = object()
    seen = {}

    def fake_post_tick(engine_obj):
        seen["engine"] = getattr(engine_obj, "engine", None)

    monkeypatch.setattr(broker_truth, "_repo_engine_binding_installed", False, raising=False)
    monkeypatch.setattr(broker_truth, "_post_pb1_tick_reconcile", fake_post_tick)
    trader._install_broker_truth_repo_engine_binding()

    class Repo:
        engine = marker

    class EngineObj:
        orders_repo = Repo()

    obj = EngineObj()
    broker_truth._post_pb1_tick_reconcile(obj)

    assert getattr(obj, "engine", None) is marker
    assert seen["engine"] is marker


def test_daily_ccld_fill_links_exact_order_and_creates_system_position_with_entry_policy() -> None:
    """2026-09-11 class: broker fill must not remain order_id=NULL / fills=0."""
    engine = _db()
    schema = schema_for_engine(engine)
    order_id, cycle_id, epoch_id = _ids()
    filled_at = now_kst()
    plan = {
        "entry_thesis": "PB1_PULLBACK",
        "entry_reason": "ENTRY_PULLBACK_CONFIRM",
        "entry_style_selected": "PULLBACK",
        "trade_horizon": "SWING",
        "exit_policy_family": "SWING",
        "eod_action": "HOLD",
        "force_eod_close": False,
        "policy_source": "pb1_entry_plan",
        "policy_version": "test-v1",
        "risk_plan": {"initial_stop": 98000.0, "risk_R": 2000.0},
        "time_plan": {"max_trading_days": 20},
    }
    entry_meta = {
        "entry_reason": "ENTRY_PULLBACK_CONFIRM",
        "entry_style_selected": "PULLBACK",
        "trade_horizon": "SWING",
        "exit_policy_family": "SWING",
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
                client_order_key="incident-20260911-buy-005930",
                status="ACKED",
                kis_odno="0000001234",
                broker_order_id="0000001234",
                entry_meta_json=entry_meta,
                request_json={
                    "pre_order_holding_qty": 0,
                    "entry_meta": entry_meta,
                    "entry_exit_plan": plan,
                },
                response_json={"rt_cd": "0"},
                submitted_at=filled_at,
                acked_at=filled_at,
                created_at=filled_at,
            )
        )
    FillsRepo(engine).upsert_fill(
        env="practice",
        run_id=None,
        order_id=None,
        kis_odno="0000001234",
        trade_id="incident-20260911-fill-005930",
        code="005930",
        market="KOSPI",
        side="BUY",
        qty=5,
        price=100000.0,
        fee=0.0,
        tax=0.0,
        filled_at=filled_at,
        raw_json={"source": "daily_ccld"},
        fill_meta_json={"fill_source": "daily_ccld"},
    )

    result = _link_unowned_daily_fills(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
    )

    assert result == {"linked_fills": 1, "positions_promoted": 1}
    with engine.connect() as conn:
        order = conn.execute(sa.select(schema.orders).where(schema.orders.c.order_id == order_id)).mappings().one()
        fill = conn.execute(sa.select(schema.fills)).mappings().one()
        pos = conn.execute(sa.select(schema.positions)).mappings().one()

    assert order["status"] == "FILLED"
    assert str(fill["order_id"]) == order_id
    assert str(fill["position_cycle_id"]) == cycle_id
    assert str(fill["portfolio_epoch_id"]) == epoch_id
    assert pos["position_origin"] == "SYSTEM"
    assert str(pos["position_cycle_id"]) == cycle_id
    assert str(pos["portfolio_epoch_id"]) == epoch_id
    assert int(pos["qty"]) == 5
    assert pos["entry_reason"] == "ENTRY_PULLBACK_CONFIRM"
    assert pos["entry_style_selected"] == "PULLBACK"
    assert pos["trade_horizon"] == "SWING"
    assert pos["exit_policy_family"] == "SWING"
    assert str(pos["entry_ts"] or "").startswith(str(filled_at.date()))


def test_policy_missing_without_unique_fill_provenance_is_never_guessed() -> None:
    engine = _db()
    schema = schema_for_engine(engine)
    _, cycle_id, epoch_id = _ids()
    with engine.begin() as conn:
        conn.execute(
            sa.insert(schema.positions).values(
                position_id=str(uuid4()),
                position_cycle_id=cycle_id,
                portfolio_epoch_id=epoch_id,
                opened_at=now_kst(),
                position_origin="IMPORTED",
                env="practice",
                strategy=STRATEGY,
                sid=1,
                mode=1,
                code="000660",
                market="KOSPI",
                qty=2,
                avg_buy_price=250000.0,
                total_cost=500000.0,
                realized_pnl=0.0,
                status="OPEN",
                entry_thesis="POLICY_MISSING",
                exit_policy_family="POLICY_MISSING",
                entry_meta_json={},
                entry_exit_plan_json={},
                position_meta={"holding_age_unknown": True},
            )
        )

    result = _recover_proven_policy_positions(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
        holdings_rows=[{"pdno": "000660", "hldg_qty": "2", "pchs_avg_pric": "250000"}],
    )

    assert result["recovered"] == []
    assert result["review_required"] == ["000660"]
    with engine.connect() as conn:
        pos = conn.execute(sa.select(schema.positions)).mappings().one()
    assert pos["exit_policy_family"] == "POLICY_MISSING"
    assert pos["entry_thesis"] == "POLICY_MISSING"
    assert pos["position_origin"] == "IMPORTED"


def test_health_flags_db_kis_qty_mismatch_and_previous_day_open_ack() -> None:
    """122630 class: DB6/KIS3 and a stale ACK must be RED evidence, not hidden."""
    engine = _db()
    schema = schema_for_engine(engine)
    order_id, cycle_id, epoch_id = _ids()
    yesterday = now_kst() - timedelta(days=1)
    with engine.begin() as conn:
        conn.execute(
            sa.insert(schema.positions).values(
                position_id=str(uuid4()),
                position_cycle_id=cycle_id,
                portfolio_epoch_id=epoch_id,
                opened_at=yesterday,
                position_origin="SYSTEM",
                env="practice",
                strategy=STRATEGY,
                sid=1,
                mode=1,
                code="122630",
                market="KOSPI",
                qty=6,
                avg_buy_price=110500.0,
                total_cost=663000.0,
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
                client_order_key="incident-20260824-sell-122630",
                status="ACKED",
                request_json={"pre_order_holding_qty": 6},
                response_json={"rt_cd": "0"},
                created_at=yesterday,
                submitted_at=yesterday,
                acked_at=yesterday,
            )
        )

    health = _health_after_reconcile(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
        holdings_rows=[{"pdno": "122630", "hldg_qty": "3", "pchs_avg_pric": "110500"}],
    )

    assert health["qty_mismatch_count"] == 1
    assert health["qty_mismatches"] == [{"code": "122630", "db_qty": 6, "kis_qty": 3}]
    assert health["stale_open_order_count"] == 1
    assert health["stale_open_orders"][0]["code"] == "122630"
    assert health["stale_open_orders"][0]["status"] == "ACKED"
