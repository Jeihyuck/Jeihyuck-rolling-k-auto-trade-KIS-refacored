"""Regression for 2026-11-05 KR PREP/AM/PM/CLOSE PostgreSQL SAVEPOINT crash.

US migration 0062 was shared by the KR and US startup paths. Its top-level
COMMIT invalidated SQLAlchemy's statement SAVEPOINT before the migration
completion stamp could be inserted. The following startup retried that file.
"""
from pathlib import Path

import pytest

from trader.db.migrate import (
    assert_no_migration_transaction_control,
    split_postgres_sql,
)


MIGRATION = Path(__file__).resolve().parents[1] / (
    "migrations/0062_us_portfolio_position_cycle_recovery_scope.sql"
)


def test_0062_can_be_applied_by_sqlalchemy_owned_transaction():
    sql = MIGRATION.read_text(encoding="utf-8")
    statements = split_postgres_sql(sql)

    assert len(statements) > 15
    assert_no_migration_transaction_control(statements, version=MIGRATION.name)

    # Existing installations may already contain the schema/index changes,
    # while schema_migrations has no 0062 stamp due to the old rogue COMMIT.
    # Rerun must retain IF [NOT] EXISTS guards and never change historical rows.
    assert "ADD COLUMN IF NOT EXISTS account_id" in sql
    assert "CREATE UNIQUE INDEX IF NOT EXISTS uq_us_orders_broker_account_epoch_day_0062" in sql
    assert "CREATE UNIQUE INDEX IF NOT EXISTS uq_us_fills_broker_account_epoch_0062" in sql
    assert "CREATE UNIQUE INDEX IF NOT EXISTS uq_us_portfolio_epoch_owner_0062" in sql
    assert "DROP INDEX IF EXISTS public.uq_us_orders_trade_date_order_no_nonblank" in sql
    assert "DROP INDEX IF EXISTS public.uq_us_fills_idempotent" in sql
    assert "US_ORDER_BROKER_SCOPED_DUPLICATES_REQUIRE_MANUAL_EVIDENCE" in sql
    assert "US_FILL_BROKER_SCOPED_DUPLICATES_REQUIRE_MANUAL_EVIDENCE" in sql


@pytest.mark.parametrize(
    "sql",
    [
        "BEGIN; SELECT 1; COMMIT;",
        "-- leading comment\nCOMMIT;",
        "ROLLBACK;",
        "START TRANSACTION;",
        "SAVEPOINT manually_owned;",
        "RELEASE SAVEPOINT manually_owned;",
    ],
)
def test_runner_rejects_top_level_transaction_commands(sql):
    with pytest.raises(RuntimeError, match=r"TRANSACTION_CONTROL_FORBIDDEN"):
        assert_no_migration_transaction_control(
            split_postgres_sql(sql), version="test.sql"
        )


def test_runner_preserves_plpgsql_do_block_internal_begin_end():
    sql = (
        "DO $scope$ BEGIN IF TRUE THEN PERFORM 1; END IF; "
        "END $scope$; SELECT 1;"
    )
    statements = split_postgres_sql(sql)
    assert len(statements) == 2
    assert_no_migration_transaction_control(statements, version="test.sql")


def test_runner_allows_comments_before_safe_sql():
    sql = "-- this migration is safe\nCREATE TABLE IF NOT EXISTS test_example (id int);"
    statements = split_postgres_sql(sql)
    assert_no_migration_transaction_control(statements, version="test.sql")
