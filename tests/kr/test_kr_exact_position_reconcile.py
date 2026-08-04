from __future__ import annotations

from datetime import datetime

import sqlalchemy as sa

from trader.db.schema import schema_for_engine
from trader.reconcile_db import close_stale_positions_guarded, save_reconcile_guard


def test_position_qty_mismatch_adjusts_to_kis_when_guard_confirmed(tmp_path) -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(
            sa.insert(schema.positions).values(
                env="practice",
                strategy="best_k_meta",
                sid=1,
                mode=1,
                code="090430",
                market="KOSPI",
                qty=8,
                avg_buy_price=10000.0,
                total_cost=80000.0,
                status="OPEN",
            )
        )

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
            sa.select(schema.positions.c.qty, schema.positions.c.avg_buy_price, schema.positions.c.status).where(
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
    assert str(row[2]).upper() == "OPEN"
