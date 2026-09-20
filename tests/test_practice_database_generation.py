from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.engine import make_url

from trader.db.migrate import split_postgres_sql
from trader.db.practice_database_generation import (
    PracticeDatabaseGenerationError,
    prepare_new_practice_database,
    target_url_from_source,
    verify_fresh_target_database,
)
from trader.db.schema import schema_for_engine


def _drop_database(source_url: str, database_name: str) -> None:
    maintenance_url = make_url(source_url).set(database="postgres").render_as_string(hide_password=False)
    engine = sa.create_engine(maintenance_url, future=True)
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname=:name AND pid <> pg_backend_pid()"
                ),
                {"name": database_name},
            )
            conn.exec_driver_sql(f'DROP DATABASE IF EXISTS "{database_name}"')
    finally:
        engine.dispose()


def _seed_current_source_schema(engine: sa.Engine) -> None:
    """Build enough current KR/US source schema to exercise schema-only cloning."""
    schema_for_engine(engine).metadata.create_all(engine)

    us_sql = Path("migrations/0038_us_agent_tables.sql").read_text(encoding="utf-8")
    with engine.begin() as conn:
        for statement in split_postgres_sql(us_sql):
            if statement.strip():
                conn.exec_driver_sql(statement)
        conn.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        for version in sorted(path.name for path in Path("migrations").glob("*.sql")):
            conn.execute(
                text(
                    "INSERT INTO schema_migrations(version) VALUES (:version) "
                    "ON CONFLICT (version) DO NOTHING"
                ),
                {"version": version},
            )
        conn.exec_driver_sql(
            "CREATE TABLE IF NOT EXISTS db_generation_archive_sentinel("
            "id INTEGER PRIMARY KEY, note TEXT NOT NULL)"
        )
        conn.exec_driver_sql("DELETE FROM db_generation_archive_sentinel")
        conn.execute(
            text(
                "INSERT INTO db_generation_archive_sentinel(id,note) "
                "VALUES (1,'OLD_DB_MUST_SURVIVE')"
            )
        )


def test_target_url_changes_only_database_name():
    source = "postgresql+psycopg://user:pass@localhost:5432/old_db"
    target = target_url_from_source(source, "new_db")
    parsed = make_url(target)
    assert parsed.database == "new_db"
    assert parsed.host == "localhost"
    assert parsed.port == 5432
    assert parsed.username == "user"
    assert parsed.password == "pass"


def test_invalid_database_name_fails_closed():
    with pytest.raises(PracticeDatabaseGenerationError, match="INVALID_NEW_DATABASE_NAME"):
        target_url_from_source(
            "postgresql+psycopg://user:pass@localhost:5432/old_db",
            "new-db;drop",
        )


@pytest.mark.skipif(not os.getenv("PBCORE_TEST_POSTGRES_URL"), reason="real PostgreSQL URL not configured")
def test_old_database_is_preserved_and_new_database_is_schema_clone_without_data(monkeypatch):
    source_url = os.environ["PBCORE_TEST_POSTGRES_URL"]
    target_name = "pbcore_fresh_" + uuid4().hex[:12]
    target_url = target_url_from_source(source_url, target_name)
    source = sa.create_engine(source_url, future=True)

    try:
        _seed_current_source_schema(source)

        monkeypatch.setenv("STRATEGY_ENV", "practice")
        monkeypatch.setenv("KIS_ENV", "practice")
        monkeypatch.setenv("CREATE_NEW_PRACTICE_DB", "1")
        monkeypatch.setenv("DB_GENERATION_CONFIRM", "YES")

        result = prepare_new_practice_database(
            source_url=source_url,
            target_database_name=target_name,
        )

        assert result["status"] == "READY"
        assert result["source_preserved"] is True
        assert result["history_copied_to_new_db"] is False
        assert result["schema_cloned_from_source"] is True
        assert result["source"]["database"] == make_url(source_url).database
        assert result["target"]["database"] == target_name

        with source.connect() as conn:
            sentinel = conn.execute(
                text("SELECT note FROM db_generation_archive_sentinel WHERE id=1")
            ).scalar_one()
        assert sentinel == "OLD_DB_MUST_SURVIVE"

        target = sa.create_engine(target_url, future=True)
        try:
            with target.connect() as conn:
                target_sentinel_rows = conn.execute(
                    text("SELECT COUNT(*) FROM db_generation_archive_sentinel")
                ).scalar_one()
            assert target_sentinel_rows == 0
        finally:
            target.dispose()

        verified = verify_fresh_target_database(
            source_url=source_url,
            target_url=target_url,
        )
        assert verified["status"] == "CUTOVER_DB_READY"
        for table in (
            "orders", "fills", "positions",
            "us_order_intents", "us_orders", "us_fills", "us_positions",
        ):
            assert verified["target_state_row_counts"][table] == 0
        assert verified["migration_count"] == len(list(Path("migrations").glob("*.sql")))
    finally:
        source.dispose()
        _drop_database(source_url, target_name)
