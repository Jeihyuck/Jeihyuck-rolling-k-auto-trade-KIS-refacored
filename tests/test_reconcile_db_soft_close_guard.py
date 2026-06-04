from __future__ import annotations

from datetime import datetime
import sqlalchemy as sa

from trader.reconcile_db import close_stale_positions, save_reconcile_guard


def _make_engine():
    engine = sa.create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                CREATE TABLE positions (
                    position_id TEXT PRIMARY KEY,
                    env TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    code TEXT NOT NULL,
                    qty INTEGER NOT NULL,
                    avg_buy_price REAL,
                    total_cost REAL NOT NULL DEFAULT 0,
                    status TEXT,
                    closed_reason TEXT,
                    closed_ts TIMESTAMP,
                    updated_at TIMESTAMP
                )
                """
            )
        )
    return engine


def _insert_position(engine, *, code: str, qty: int = 1):
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT INTO positions (
                    position_id, env, strategy, code, qty, avg_buy_price, total_cost, status, updated_at
                ) VALUES (
                    :position_id, :env, :strategy, :code, :qty, :avg_buy_price, :total_cost, :status, :updated_at
                )
                """
            ),
            {
                "position_id": f"pos-{code}",
                "env": "practice",
                "strategy": "pb1_pullback_close",
                "code": code,
                "qty": qty,
                "avg_buy_price": 1000.0,
                "total_cost": 1000.0,
                "status": "OPEN",
                "updated_at": datetime.utcnow(),
            },
        )


def _load_qty(engine, code: str) -> int:
    with engine.connect() as conn:
        row = conn.execute(
            sa.text("SELECT qty FROM positions WHERE code = :code"),
            {"code": code},
        ).first()
    return int(row[0] if row else 0)


def test_soft_close_skips_when_kis_still_has_holdings(tmp_path):
    engine = _make_engine()
    _insert_position(engine, code="028260", qty=1)
    save_reconcile_guard(tmp_path, {"empty_streak": 2, "last_holdings_empty": True})

    closed = close_stale_positions(
        engine=engine,
        env="practice",
        strategy="pb1_pullback_close",
        reason="exit_phase",
        ts=datetime.utcnow(),
        kis_balance={"output1": [{"pdno": "028260", "hldg_qty": "1", "ord_psbl_qty": "1"}]},
        sell_fill_codes={"028260"},
        runtime_dir=tmp_path,
    )

    assert closed == 0
    assert _load_qty(engine, "028260") == 1


def test_soft_close_requires_sell_fill_and_stale_confirm(tmp_path):
    engine = _make_engine()
    _insert_position(engine, code="028260", qty=1)
    save_reconcile_guard(tmp_path, {"empty_streak": 2, "last_holdings_empty": True})

    closed = close_stale_positions(
        engine=engine,
        env="practice",
        strategy="pb1_pullback_close",
        reason="exit_phase",
        ts=datetime.utcnow(),
        kis_balance={"output1": []},
        sell_fill_codes={"028260"},
        runtime_dir=tmp_path,
    )

    assert closed == 1
    assert _load_qty(engine, "028260") == 0