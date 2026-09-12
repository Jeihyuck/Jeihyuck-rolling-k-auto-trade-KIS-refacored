from __future__ import annotations

from datetime import datetime
from uuid import uuid4

import sqlalchemy as sa

from trader.db.schema import schema_for_engine
from trader.reconcile_db import close_stale_positions_guarded, save_reconcile_guard


def _insert_position(engine, schema, *, code: str, qty: int, avg: float, status: str = "OPEN") -> str:
    position_id = str(uuid4())
    with engine.begin() as conn:
        conn.execute(
            sa.insert(schema.positions).values(
                position_id=position_id,
                env="practice",
                strategy="best_k_meta",
                sid=1,
                mode=1,
                code=code,
                market="KOSPI",
                qty=qty,
                avg_buy_price=avg,
                total_cost=qty * avg,
                status=status,
            )
        )
    return position_id


def test_position_qty_mismatch_adjusts_to_kis_when_guard_confirmed(tmp_path) -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    _insert_position(engine, schema, code="090430", qty=8, avg=10000.0)

    save_reconcile_guard(
        tmp_path,
        {
            "last_holdings_empty": True,
            "empty_streak": 2,
            "last_tick_ts": datetime(2026, 8, 4, 15, 30, 0).isoformat(),
        },
    )

    kis_balance = {
        "output1": [
            {"pdno": "090430", "hldg_qty": "1", "ord_psbl_qty": "1", "pchs_avg_pric": "10100"}
        ]
    }

    _ = close_stale_positions_guarded(
        engine=engine,
        env="practice",
        strategy="best_k_meta",
        reason="stale_db_holdings_empty",
        ts=datetime(2026, 8, 4, 15, 31, 0),
        kis_balance=kis_balance,
        sell_fill_codes=[],
        runtime_dir=tmp_path,
    )

    with engine.connect() as conn:
        row = conn.execute(
            sa.select(
                schema.positions.c.qty,
                schema.positions.c.avg_buy_price,
                schema.positions.c.total_cost,
                schema.positions.c.status,
            ).where(
                sa.and_(
                    schema.positions.c.env == "practice",
                    schema.positions.c.strategy == "best_k_meta",
                    schema.positions.c.code == "090430",
                )
            )
        ).first()

    assert row is not None
    assert int(row[0]) == 1
    assert float(row[1]) == 10100.0
    assert float(row[2]) == 10100.0
    assert str(row[3]).upper() == "OPEN"


def test_positive_kis_qty_is_authoritative_even_when_open_order_is_stale(tmp_path) -> None:
    """2026-08-24 shape: DB=6, KIS=3 and a stale/open SELL row must not freeze DB at six."""
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    open_id = _insert_position(engine, schema, code="122630", qty=6, avg=110500.0)

    with engine.begin() as conn:
        conn.execute(
            sa.insert(schema.orders).values(
                env="practice",
                strategy="best_k_meta",
                sid=1,
                mode=1,
                code="122630",
                market="KOSPI",
                side="SELL",
                ord_type="MARKET",
                qty=3,
                client_order_key="kr-inf-20260824-sell",
                status="ACKED",
                request_json={},
            )
        )

    kis_balance = {
        "output1": [
            {"pdno": "122630", "hldg_qty": "3", "ord_psbl_qty": "3", "pchs_avg_pric": "110500"}
        ]
    }

    _ = close_stale_positions_guarded(
        engine=engine,
        env="practice",
        strategy="best_k_meta",
        reason="daily_reconcile",
        ts=datetime(2026, 8, 24, 15, 30, 0),
        kis_balance=kis_balance,
        sell_fill_codes=[],
        runtime_dir=tmp_path,
    )

    with engine.connect() as conn:
        row = conn.execute(
            sa.select(
                schema.positions.c.qty,
                schema.positions.c.avg_buy_price,
                schema.positions.c.total_cost,
            ).where(schema.positions.c.position_id == open_id)
        ).first()

    assert row is not None
    assert int(row[0]) == 3
    assert float(row[1]) == 110500.0
    assert float(row[2]) == 331500.0


def test_authoritative_adjustment_updates_only_current_open_lifecycle(tmp_path) -> None:
    """A current KIS holding must never resurrect an old CLOSED row for the same symbol."""
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    closed_id = _insert_position(engine, schema, code="122630", qty=9, avg=90000.0, status="CLOSED")
    open_id = _insert_position(engine, schema, code="122630", qty=6, avg=110500.0, status="OPEN")

    kis_balance = {
        "output1": [
            {"pdno": "122630", "hldg_qty": "3", "ord_psbl_qty": "3", "pchs_avg_pric": "110500"}
        ]
    }
    _ = close_stale_positions_guarded(
        engine=engine,
        env="practice",
        strategy="best_k_meta",
        reason="daily_reconcile",
        ts=datetime(2026, 8, 24, 15, 30, 0),
        kis_balance=kis_balance,
        sell_fill_codes=[],
        runtime_dir=tmp_path,
    )

    with engine.connect() as conn:
        rows = {
            str(row["position_id"]): dict(row)
            for row in conn.execute(
                sa.select(
                    schema.positions.c.position_id,
                    schema.positions.c.qty,
                    schema.positions.c.avg_buy_price,
                    schema.positions.c.total_cost,
                    schema.positions.c.status,
                ).where(schema.positions.c.code == "122630")
            ).mappings().all()
        }

    assert int(rows[open_id]["qty"]) == 3
    assert float(rows[open_id]["avg_buy_price"]) == 110500.0
    assert float(rows[open_id]["total_cost"]) == 331500.0
    assert rows[open_id]["status"] == "OPEN"

    assert int(rows[closed_id]["qty"]) == 9
    assert float(rows[closed_id]["avg_buy_price"]) == 90000.0
    assert float(rows[closed_id]["total_cost"]) == 810000.0
    assert rows[closed_id]["status"] == "CLOSED"
