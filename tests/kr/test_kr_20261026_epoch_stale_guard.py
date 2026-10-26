"""2026-10-26: cross-account trading epoch must not create false duplicates."""
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
import sqlalchemy as sa

from trader import reconcile_db as rd


@pytest.fixture
def epoch_db(monkeypatch):
    engine = sa.create_engine("sqlite:///:memory:")
    md = sa.MetaData()
    positions = sa.Table(
        "positions", md,
        sa.Column("position_id", sa.String, primary_key=True),
        sa.Column("env", sa.String),
        sa.Column("strategy", sa.String),
        sa.Column("code", sa.String),
        sa.Column("qty", sa.Integer),
        sa.Column("avg_buy_price", sa.Float),
        sa.Column("total_cost", sa.Float),
        sa.Column("status", sa.String),
        sa.Column("updated_at", sa.DateTime),
        sa.Column("trading_epoch_id", sa.String),
    )
    orders = sa.Table(
        "orders", md,
        sa.Column("order_id", sa.String, primary_key=True),
        sa.Column("env", sa.String),
        sa.Column("strategy", sa.String),
        sa.Column("code", sa.String),
        sa.Column("status", sa.String),
        sa.Column("trading_epoch_id", sa.String),
    )
    md.create_all(engine)
    with engine.begin() as conn:
        conn.execute(sa.insert(positions), [
            dict(position_id="new", env="practice", strategy="pb1_pullback_close",
                 code="293490", qty=8, avg_buy_price=10000.0,
                 total_cost=80000.0, status="OPEN", trading_epoch_id="new-epoch"),
            dict(position_id="old", env="practice", strategy="pb1_pullback_close",
                 code="293490", qty=36, avg_buy_price=9000.0,
                 total_cost=324000.0, status="OPEN", trading_epoch_id="old-epoch"),
        ])
        conn.execute(sa.insert(orders), [
            dict(order_id="old-order", env="practice", strategy="pb1_pullback_close",
                 code="293490", status="PARTIAL_FILLED", trading_epoch_id="old-epoch"),
        ])
    monkeypatch.setattr(rd, "schema_for_engine", lambda _eng: SimpleNamespace(positions=positions, orders=orders))
    monkeypatch.setattr(rd, "get_account_key", lambda **kwargs: "practice:new")
    monkeypatch.setattr(rd, "trading_epoch_enforced", lambda: True)
    monkeypatch.setattr(rd, "active_trading_epoch_id", lambda *args, **kwargs: "new-epoch")
    return engine, positions


def _run(engine, broker_qty):
    return rd.close_stale_positions_guarded(
        engine=engine,
        env="practice",
        strategy="pb1_pullback_close",
        reason="manual-review",
        ts=datetime.now(timezone.utc),
        kis_balance={"output1": [
            {"pdno": "293490", "hldg_qty": str(broker_qty),
             "ord_psbl_qty": str(broker_qty), "pchs_avg_pric": "10000"}
        ]},
    )


def test_prior_epoch_does_not_block_current_position_quantity_adjustment(epoch_db):
    engine, positions = epoch_db
    _run(engine, 11)
    with engine.connect() as conn:
        rows = {row["position_id"]: dict(row) for row in conn.execute(sa.select(positions)).mappings()}
    assert rows["new"]["qty"] == 11
    assert rows["old"]["qty"] == 36
    assert rows["old"]["status"] == "OPEN"


def test_prior_epoch_does_not_force_current_qty_to_total_across_epochs(epoch_db):
    engine, positions = epoch_db
    _run(engine, 8)
    with engine.connect() as conn:
        rows = {row["position_id"]: dict(row) for row in conn.execute(sa.select(positions)).mappings()}
    assert rows["new"]["qty"] == 8
    assert rows["old"]["qty"] == 36


def test_missing_authoritative_epoch_fails_closed(epoch_db, monkeypatch):
    engine, positions = epoch_db
    monkeypatch.setattr(rd, "active_trading_epoch_id", lambda *args, **kwargs: None)
    with pytest.raises(RuntimeError, match="KR_STALE_POSITION_ACTIVE_EPOCH_REQUIRED"):
        _run(engine, 11)
    with engine.connect() as conn:
        qtys = {row["position_id"]: row["qty"] for row in conn.execute(sa.select(positions)).mappings()}
    assert qtys == {"new": 8, "old": 36}
