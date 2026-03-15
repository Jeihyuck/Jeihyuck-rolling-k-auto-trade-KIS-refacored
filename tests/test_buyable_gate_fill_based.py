from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from trader.db.repos import FillsRepo, LedgerEventsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.pb1_engine import PB1Engine
from trader.window_router import WindowDecision


KST = ZoneInfo("Asia/Seoul")


def _make_engine(*, db_engine, now_kst: datetime, env: str = "practice", derived_as_of: str = "2026-03-12"):
    orders_repo = OrdersRepo(db_engine)
    fills_repo = FillsRepo(db_engine)
    positions_repo = PositionsRepo(db_engine)
    ledger_repo = LedgerEventsRepo(db_engine)
    engine = PB1Engine(
        universe_repo=object(),
        orders_repo=orders_repo,
        fills_repo=fills_repo,
        positions_repo=positions_repo,
        ledger_repo=ledger_repo,
        kis=None,
        window=WindowDecision(name="after", phase="entry"),
        window_label="after",
        phase="entry",
        dry_run=True,
        env=env,
        run_id=f"run-{env}",
        intended_live=False,
        now_kst_value=now_kst,
        derived_as_of=derived_as_of,
        compute_only_full_run=True,
        trading_day=False,
    )
    return engine, orders_repo, fills_repo, positions_repo, ledger_repo


def _new_db_engine():
    db_engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(db_engine)
    schema.metadata.create_all(db_engine)
    return db_engine


def _insert_fill(*, fills_repo: FillsRepo, positions_repo: PositionsRepo, env: str, side: str, code: str, filled_at: datetime):
    fills_repo.upsert_fill(
        env=env,
        run_id=None,
        order_id=None,
        kis_odno=f"ODNO-{env}-{code}-{side}",
        trade_id=f"TRD-{env}-{code}-{side}-{filled_at.isoformat()}",
        code=code,
        market="KOSPI",
        side=side,
        qty=1,
        price=100.0,
        fee=0.0,
        tax=0.0,
        filled_at=filled_at,
        raw_json={"filled": True},
    )
    positions_repo.apply_fill(
        env=env,
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code=code,
        market="KOSPI",
        side=side,
        qty=1,
        price=100.0,
        fee=0.0,
        tax=0.0,
        filled_at=filled_at,
    )


def _insert_order(*, orders_repo: OrdersRepo, env: str, code: str, created_at: datetime, status: str):
    orders_repo.upsert_reconciled_order(
        env=env,
        run_id=None,
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code=code,
        market="KOSPI",
        side="BUY",
        ord_type="RECONCILED",
        qty=1,
        limit_price=100.0,
        stage="PB1-CLOSE",
        client_order_key=f"{env}:{code}:{status}:{created_at.isoformat()}",
        kis_odno=f"ODNO-{env}-{code}-{status}",
        status=status,
        request_json={},
        response_json={},
        submitted_at=created_at,
        acked_at=created_at,
    )


def _snapshot(engine: PB1Engine, positions_repo: PositionsRepo, code: str) -> dict:
    positions = positions_repo.list_positions(engine.env, engine.STRATEGY_NAME)
    return engine._build_buyable_gate_context(codes=[code], positions=positions)[code]


def test_today_buy_exists_true_for_actual_buy_fill():
    db_engine = _new_db_engine()
    now_kst = datetime(2026, 3, 16, 15, 0, tzinfo=KST)
    engine, _orders_repo, fills_repo, positions_repo, _ledger_repo = _make_engine(db_engine=db_engine, now_kst=now_kst)

    _insert_fill(fills_repo=fills_repo, positions_repo=positions_repo, env="practice", side="BUY", code="005930", filled_at=now_kst)

    snapshot = _snapshot(engine, positions_repo, "005930")

    assert snapshot["today_buy_exists"] is True


def test_order_skip_only_does_not_create_today_buy_or_cooldown():
    db_engine = _new_db_engine()
    now_kst = datetime(2026, 3, 16, 15, 0, tzinfo=KST)
    engine, _orders_repo, _fills_repo, positions_repo, ledger_repo = _make_engine(db_engine=db_engine, now_kst=now_kst)

    ledger_repo.append_event(
        env="practice",
        run_id=None,
        strategy="pb1_pullback_close",
        event_type="ORDER_SKIP",
        ts=now_kst,
        code="005930",
        side="BUY",
        qty=1,
        price=100.0,
        ok=False,
        reasons=["rate_limit"],
        stage="PB1-CLOSE",
        payload_json={},
    )

    flags = engine._classify_ledger_event({"event_type": "ORDER_SKIP", "side": "BUY", "qty": 1, "payload_json": {}})
    snapshot = _snapshot(engine, positions_repo, "005930")

    assert flags["is_skip_event"] is True
    assert flags["is_buy_execution_event"] is False
    assert flags["starts_cooldown"] is False
    assert snapshot["today_buy_exists"] is False
    assert snapshot["cooldown_active"] is False


def test_submitted_order_only_does_not_create_today_buy_exists():
    db_engine = _new_db_engine()
    now_kst = datetime(2026, 3, 16, 15, 0, tzinfo=KST)
    engine, orders_repo, _fills_repo, positions_repo, _ledger_repo = _make_engine(db_engine=db_engine, now_kst=now_kst)

    _insert_order(orders_repo=orders_repo, env="practice", code="005930", created_at=now_kst, status="SUBMITTED")

    snapshot = _snapshot(engine, positions_repo, "005930")

    assert snapshot["today_buy_exists"] is False


def test_cooldown_inactive_when_until_has_passed():
    db_engine = _new_db_engine()
    now_kst = datetime(2026, 3, 16, 15, 0, tzinfo=KST)
    engine, _orders_repo, fills_repo, positions_repo, _ledger_repo = _make_engine(db_engine=db_engine, now_kst=now_kst)

    yesterday = datetime(2026, 3, 15, 10, 0, tzinfo=KST)
    _insert_fill(fills_repo=fills_repo, positions_repo=positions_repo, env="practice", side="BUY", code="005930", filled_at=yesterday)
    positions_repo.update_position_fields(
        env="practice",
        strategy=engine.STRATEGY_NAME,
        sid=1,
        mode=1,
        code="005930",
        fields={"cooldown_until": "2026-03-15"},
    )

    snapshot = _snapshot(engine, positions_repo, "005930")

    assert snapshot["cooldown_active"] is False


def test_cooldown_active_when_until_is_future_and_fill_exists():
    db_engine = _new_db_engine()
    now_kst = datetime(2026, 3, 16, 15, 0, tzinfo=KST)
    engine, _orders_repo, fills_repo, positions_repo, _ledger_repo = _make_engine(db_engine=db_engine, now_kst=now_kst)

    recent_fill = datetime(2026, 3, 16, 9, 5, tzinfo=KST)
    _insert_fill(fills_repo=fills_repo, positions_repo=positions_repo, env="practice", side="BUY", code="005930", filled_at=recent_fill)
    positions_repo.update_position_fields(
        env="practice",
        strategy=engine.STRATEGY_NAME,
        sid=1,
        mode=1,
        code="005930",
        fields={"cooldown_until": "2026-03-17"},
    )

    snapshot = _snapshot(engine, positions_repo, "005930")

    assert snapshot["cooldown_active"] is True


def test_nontrading_after_compute_only_uses_now_kst_date_not_derived_as_of():
    db_engine = _new_db_engine()
    now_kst = datetime(2026, 3, 15, 18, 0, tzinfo=KST)
    engine, _orders_repo, fills_repo, positions_repo, _ledger_repo = _make_engine(
        db_engine=db_engine,
        now_kst=now_kst,
        derived_as_of="2026-03-12",
    )

    past_fill = datetime(2026, 3, 12, 14, 0, tzinfo=KST)
    _insert_fill(fills_repo=fills_repo, positions_repo=positions_repo, env="practice", side="BUY", code="005930", filled_at=past_fill)

    snapshot = _snapshot(engine, positions_repo, "005930")

    assert snapshot["today_buy_exists"] is False


def test_env_isolation_between_practice_and_real():
    db_engine = _new_db_engine()
    now_kst = datetime(2026, 3, 16, 15, 0, tzinfo=KST)
    practice_engine, _orders_repo, fills_repo, positions_repo, _ledger_repo = _make_engine(db_engine=db_engine, now_kst=now_kst, env="practice")
    real_engine, _orders_repo2, _fills_repo2, _positions_repo2, _ledger_repo2 = _make_engine(db_engine=db_engine, now_kst=now_kst, env="real")

    _insert_fill(fills_repo=fills_repo, positions_repo=positions_repo, env="real", side="BUY", code="005930", filled_at=now_kst)

    practice_snapshot = _snapshot(practice_engine, positions_repo, "005930")
    real_snapshot = _snapshot(real_engine, positions_repo, "005930")

    assert practice_snapshot["today_buy_exists"] is False
    assert real_snapshot["today_buy_exists"] is True