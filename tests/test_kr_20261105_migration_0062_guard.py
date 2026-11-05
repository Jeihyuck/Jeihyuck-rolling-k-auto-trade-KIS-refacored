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


@pytest.mark.parametrize("preapplied_schema_without_stamp", [False, True])
def test_0062_real_postgres_clean_and_orphaned_stamp(
    tmp_path, preapplied_schema_without_stamp
):
    """Run only against GitHub CI's disposable local PostgreSQL instance.

    Never mutate the production database. A separate temporary database is
    created and destroyed within the CI PostgreSQL 16 service.
    """
    import os
    import shutil
    import uuid

    import sqlalchemy as sa
    from sqlalchemy.engine import make_url

    from trader.db.migrate import run_migrations

    test_url = os.getenv("PBCORE_TEST_POSTGRES_URL")
    if not test_url:
        pytest.skip("requires CI's PBCORE_TEST_POSTGRES_URL")

    admin_url = make_url(test_url)
    if admin_url.host not in {"localhost", "127.0.0.1"} or admin_url.database != "kr_ci":
        pytest.skip("refuse to create a database except on isolated localhost kr_ci")

    name = "nullim_migration_0062_" + uuid.uuid4().hex[:12]
    admin = sa.create_engine(admin_url, isolation_level="AUTOCOMMIT")
    temp_db = None
    created = False
    try:
        with admin.connect() as conn:
            conn.exec_driver_sql(f'CREATE DATABASE "{name}"')
        created = True
        temp_db = sa.create_engine(admin_url.set(database=name))

        # Minimal canonical tables required by the US schema-only migration.
        schema = [
            "CREATE TABLE public.us_orders (id integer PRIMARY KEY, trade_date date, order_no text, env text, trading_epoch_id text)",
            "CREATE TABLE public.us_order_intents (id integer PRIMARY KEY)",
            "CREATE TABLE public.us_order_events (id integer PRIMARY KEY)",
            "CREATE TABLE public.us_fills (id integer PRIMARY KEY, trade_date date, symbol text, side text, order_no text, client_order_key text, qty integer, price_usd numeric, trading_epoch_id text)",
            "CREATE TABLE public.us_positions (id integer PRIMARY KEY)",
            "CREATE TABLE public.us_position_risk_state (id integer PRIMARY KEY)",
            "CREATE TABLE public.us_execution_claims (id integer PRIMARY KEY, env text, account_id text, trading_epoch_id text, action_key text)",
            "CREATE TABLE public.us_protective_stop_obligations (id integer PRIMARY KEY, env text, account_id text, trading_epoch_id text)",
            "CREATE TABLE public.portfolio_epochs (id integer PRIMARY KEY, env text, account_id text, trading_epoch_id text, strategy text, status text, sid integer, mode integer)",
        ]
        with temp_db.begin() as conn:
            for statement in schema:
                conn.exec_driver_sql(statement)
            conn.exec_driver_sql(
                "INSERT INTO public.us_orders "
                "(id, trade_date, order_no, env, trading_epoch_id) "
                "VALUES (1, '2026-11-04', '00000042', 'practice', 'legacy-epoch')"
            )
            conn.exec_driver_sql(
                "INSERT INTO public.us_fills "
                "(id, trade_date, symbol, side, order_no, client_order_key, qty, price_usd, trading_epoch_id) "
                "VALUES (1, '2026-11-04', 'AAPL', 'BUY', '00000042', 'legacy-key', 1, 125, 'legacy-epoch')"
            )

        if preapplied_schema_without_stamp:
            # Recreate the exact structural split observed in production:
            # all DDL present, but schema_migrations lacks version 0062.
            with temp_db.begin() as conn:
                for statement in split_postgres_sql(MIGRATION.read_text()):
                    conn.exec_driver_sql(statement)

        isolated_dir = tmp_path / "migrations"
        isolated_dir.mkdir()
        shutil.copyfile(MIGRATION, isolated_dir / MIGRATION.name)

        run_migrations(temp_db, migrations_dir=str(isolated_dir))
        with temp_db.connect() as conn:
            versions = conn.execute(sa.text(
                "SELECT version FROM public.schema_migrations WHERE version = :version"
            ), {"version": MIGRATION.name}).all()
            assert len(versions) == 1

            idx = {row[0] for row in conn.execute(sa.text(
                "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'"
            )).all()}
            assert "uq_us_orders_broker_account_epoch_day_0062" in idx
            assert "uq_us_fills_broker_account_epoch_0062" in idx
            assert "uq_us_portfolio_epoch_owner_0062" in idx

            claim = conn.execute(sa.text(
                "SELECT conname FROM pg_constraint "
                "WHERE conname = 'ck_us_order_scope_complete_0062'"
            )).scalar_one()
            assert claim == "ck_us_order_scope_complete_0062"
            assert conn.execute(sa.text("SELECT count(*) FROM public.us_orders")).scalar_one() == 1
            assert conn.execute(sa.text("SELECT count(*) FROM public.us_fills")).scalar_one() == 1

        # Force an actual replay check (bypassing only the process-local
        # optimization) while retaining the successfully stamped schema.
        from trader.db.migrate import _MIGRATION_GUARD
        key = f"{temp_db.url!s}|{isolated_dir.resolve()}"
        _MIGRATION_GUARD.discard(key)
        run_migrations(temp_db, migrations_dir=str(isolated_dir))
        with temp_db.connect() as conn:
            assert conn.execute(sa.text(
                "SELECT count(*) FROM schema_migrations WHERE version=:version"
            ), {"version": MIGRATION.name}).scalar_one() == 1
            assert conn.execute(sa.text("SELECT count(*) FROM public.us_orders")).scalar_one() == 1
    finally:
        if temp_db is not None:
            temp_db.dispose()
        if created:
            with admin.connect() as conn:
                conn.exec_driver_sql(f'DROP DATABASE "{name}" WITH (FORCE)')
        admin.dispose()
