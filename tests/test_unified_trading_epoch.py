from __future__ import annotations

from pathlib import Path

import sqlalchemy as sa

from trader.db.schema import schema_for_engine
from trader.db.repos import PortfolioEpochsRepo
from trader.db.trading_epoch import (
    active_trading_epoch_id,
    start_new_trading_epoch,
)


def _engine():
    engine = sa.create_engine("sqlite:///:memory:", future=True)
    schema_for_engine(engine).metadata.create_all(engine)
    return engine


def test_start_new_epoch_preserves_history_and_ends_prior_children():
    engine = _engine()
    schema = schema_for_engine(engine)

    first = start_new_trading_epoch(
        engine, env="practice", account_id="practice:test",
        reason="PRACTICE_RESET_A",
    )
    assert active_trading_epoch_id(
        engine, env="practice", account_id="practice:test"
    ) == first

    child = PortfolioEpochsRepo(engine).get_or_create_active(
        env="practice", account_id="practice:test", sid=1, mode=1, strategy="pb1"
    )
    with engine.connect() as conn:
        row = conn.execute(
            sa.select(schema.portfolio_epochs).where(
                schema.portfolio_epochs.c.portfolio_epoch_id == child
            )
        ).mappings().one()
        assert str(row["trading_epoch_id"]) == first
        assert row["status"] == "ACTIVE"

    second = start_new_trading_epoch(
        engine, env="practice", account_id="practice:test",
        reason="PRACTICE_RESET_B",
    )
    assert second != first
    assert active_trading_epoch_id(
        engine, env="practice", account_id="practice:test"
    ) == second

    with engine.connect() as conn:
        epochs = conn.execute(
            sa.select(schema.trading_epochs).where(
                schema.trading_epochs.c.env == "practice",
                schema.trading_epochs.c.account_id == "practice:test",
            ).order_by(schema.trading_epochs.c.created_at)
        ).mappings().all()
        children = conn.execute(
            sa.select(schema.portfolio_epochs).where(
                schema.portfolio_epochs.c.account_id == "practice:test"
            )
        ).mappings().all()

    assert len(epochs) == 2
    assert {row["status"] for row in epochs} == {"ACTIVE", "ENDED"}
    assert len(children) == 1
    assert children[0]["status"] == "ENDED"
    assert str(children[0]["trading_epoch_id"]) == first


def test_new_kr_portfolio_child_belongs_to_current_top_epoch():
    engine = _engine()
    top = start_new_trading_epoch(
        engine, env="practice", account_id="practice:test",
        reason="PRACTICE_CLEAN_RESTART",
    )
    child = PortfolioEpochsRepo(engine).get_or_create_active(
        env="practice", account_id="practice:test", sid=1, mode=1, strategy="pb1"
    )
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        row = conn.execute(
            sa.select(schema.portfolio_epochs).where(
                schema.portfolio_epochs.c.portfolio_epoch_id == child
            )
        ).mappings().one()
    assert str(row["trading_epoch_id"]) == top


def test_0052_migration_covers_kr_us_and_infinite_state():
    sql = Path("migrations/0052_unified_trading_epoch.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS trading_epochs" in sql
    for table in (
        "portfolio_epochs", "orders", "fills", "positions",
        "us_order_intents", "us_orders", "us_fills", "us_positions",
        "us_order_events", "us_profit_capture_lifecycle",
        "us_tqqq_infinite_state", "kr_infinite_state", "kr_infinite_order_intents",
    ):
        assert f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS trading_epoch_id" in sql
    assert "PRIMARY KEY(trading_epoch_id, strategy_id, symbol)" in sql
    assert "ON kr_infinite_order_intents(trading_epoch_id, idempotency_key)" in sql


def test_canonical_preflight_requires_active_epoch():
    source = Path("scripts/wsl/deploy-preflight.sh").read_text(encoding="utf-8")
    assert "TRADING_EPOCH_ENFORCE=1" in source
    assert "scripts/verify_active_trading_epoch.py" in source
    assert "active_trading_epoch_missing" in source


def test_pr137_db_replacement_is_not_part_of_restored_design():
    # This branch is intentionally rooted at PR135. The epoch reset does not
    # depend on the PR137 fresh-database cutover path.
    assert not Path("scripts/prepare_new_practice_database.py").exists()
    assert not Path("scripts/verify_new_practice_database_cutover.py").exists()
