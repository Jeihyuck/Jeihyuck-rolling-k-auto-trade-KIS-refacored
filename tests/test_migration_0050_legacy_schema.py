"""PR101 hotfix regression tests for migrations/0050_add_position_cycle_lifecycle.sql.

Production Postgres has legacy schemas where positions.last_trade_at (and
updated_at) are TEXT rather than TIMESTAMPTZ (see migrations/0001_pbcore.sql),
while newer schemas created via trader/db/schema.py use TIMESTAMPTZ. The
original migration mixed these types inside a single COALESCE, which
PostgreSQL rejects with "COALESCE types timestamp with time zone and text
cannot be matched".
"""
import re
from pathlib import Path

import pytest

MIGRATION_PATH = Path("migrations/0050_add_position_cycle_lifecycle.sql")
SQL = MIGRATION_PATH.read_text(encoding="utf-8")


def test_migration_never_mixes_types_in_opened_at_coalesce():
    # The old buggy expression mixed a TIMESTAMPTZ column with raw TEXT columns.
    assert "COALESCE(opened_at, last_trade_at, updated_at, NOW())" not in SQL
    assert "last_trade_at::text" in SQL and "updated_at::text" in SQL


def test_migration_defines_and_drops_safe_cast_helper():
    assert "CREATE OR REPLACE FUNCTION migration_0050_safe_timestamptz" in SQL
    assert "DROP FUNCTION IF EXISTS migration_0050_safe_timestamptz(TEXT);" in SQL


def test_migration_file_name_contract():
    assert re.fullmatch(r"\d{4}_.+\.sql", MIGRATION_PATH.name)


# ---------------------------------------------------------------------------
# Real PostgreSQL integration tests exercising legacy upgrade paths.
# ---------------------------------------------------------------------------
import os  # noqa: E402

pg_required = pytest.mark.skipif(
    not os.getenv("PBCORE_TEST_POSTGRES_URL"),
    reason="real PostgreSQL integration URL not configured",
)


def _create_legacy_tables(conn, last_trade_at_type: str):
    from sqlalchemy import text

    conn.execute(text("DROP TABLE IF EXISTS fills CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS orders CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS positions CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS portfolio_epochs CASCADE"))
    conn.execute(
        text(
            f"""
            CREATE TABLE positions (
                position_id TEXT PRIMARY KEY,
                env TEXT NOT NULL,
                strategy TEXT NOT NULL,
                sid INTEGER NOT NULL,
                mode INTEGER NOT NULL,
                code TEXT NOT NULL,
                status TEXT,
                closed_ts TIMESTAMPTZ,
                closed_reason TEXT,
                last_trade_at {last_trade_at_type},
                updated_at TEXT
            )
            """
        )
    )
    conn.execute(
        text(
            """
            CREATE TABLE orders (
                order_id TEXT PRIMARY KEY
            )
            """
        )
    )
    conn.execute(
        text(
            """
            CREATE TABLE fills (
                fill_id TEXT PRIMARY KEY
            )
            """
        )
    )


def _insert_position(conn, *, last_trade_at, updated_at="2026-01-01 00:00:00+00"):
    from sqlalchemy import text

    conn.execute(
        text(
            """
            INSERT INTO positions (position_id, env, strategy, sid, mode, code, last_trade_at, updated_at)
            VALUES ('pos-1', 'practice', 'pb1', 1, 1, '000001', :last_trade_at, :updated_at)
            """
        ),
        {"last_trade_at": last_trade_at, "updated_at": updated_at},
    )


def _run_migration(conn):
    from trader.db.migrate import split_postgres_sql
    from sqlalchemy import text

    for statement in split_postgres_sql(SQL):
        conn.execute(text(statement))


def _opened_at(conn):
    from sqlalchemy import text

    return conn.execute(text("SELECT opened_at FROM positions WHERE position_id = 'pos-1'")).scalar()


@pytest.fixture()
def pg_engine():
    from sqlalchemy import create_engine

    engine = create_engine(os.environ.get("PBCORE_TEST_POSTGRES_URL", ""), future=True)
    yield engine
    engine.dispose()


@pg_required
def test_legacy_text_last_trade_at_with_value_migrates(pg_engine):
    with pg_engine.begin() as conn:
        _create_legacy_tables(conn, "TEXT")
        _insert_position(conn, last_trade_at="2025-06-01 09:30:00+00")
        _run_migration(conn)
        assert _opened_at(conn) is not None


@pg_required
def test_legacy_text_last_trade_at_empty_string_migrates(pg_engine):
    with pg_engine.begin() as conn:
        _create_legacy_tables(conn, "TEXT")
        _insert_position(conn, last_trade_at="")
        _run_migration(conn)
        # Falls back to updated_at rather than failing.
        assert _opened_at(conn) is not None


@pg_required
def test_legacy_text_last_trade_at_null_migrates(pg_engine):
    with pg_engine.begin() as conn:
        _create_legacy_tables(conn, "TEXT")
        _insert_position(conn, last_trade_at=None)
        _run_migration(conn)
        assert _opened_at(conn) is not None


@pg_required
def test_timestamptz_last_trade_at_migrates(pg_engine):
    from datetime import datetime, timezone

    with pg_engine.begin() as conn:
        _create_legacy_tables(conn, "TIMESTAMPTZ")
        ts = datetime(2025, 6, 1, 9, 30, tzinfo=timezone.utc)
        _insert_position(conn, last_trade_at=ts)
        _run_migration(conn)
        assert _opened_at(conn) is not None


@pg_required
def test_migration_is_idempotent_and_preserves_opened_at(pg_engine):
    with pg_engine.begin() as conn:
        _create_legacy_tables(conn, "TEXT")
        _insert_position(conn, last_trade_at="2025-06-01 09:30:00+00")
        _run_migration(conn)
        first_opened_at = _opened_at(conn)
        # Re-running must not fail and must not change the already-populated value.
        _run_migration(conn)
        second_opened_at = _opened_at(conn)
        assert first_opened_at == second_opened_at
