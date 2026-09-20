from __future__ import annotations

import os
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.engine import make_url

from trader.db.practice_database_generation import (
    CRITICAL_STATE_TABLES,
    REQUIRED_CONTRACT_COLUMNS,
    PracticeDatabaseGenerationError,
    prepare_new_practice_database,
    target_url_from_source,
    verify_fresh_target_database,
)
from scripts.verify_new_practice_database_cutover import (
    _assert_us_balance_authoritative,
    _kr_pending_orders,
)


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
    """Build the actual latest schema by executing every SQL migration in PostgreSQL."""
    with engine.begin() as conn:
        conn.exec_driver_sql("DROP SCHEMA IF EXISTS public CASCADE")
        conn.exec_driver_sql("CREATE SCHEMA public")
        conn.exec_driver_sql(
            """
            CREATE TABLE schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    parsed = make_url(str(engine.url))
    cli_url = parsed.set(drivername="postgresql").render_as_string(hide_password=False)
    for migration in sorted(Path("migrations").glob("*.sql")):
        proc = subprocess.run(
            [
                "psql",
                "--no-psqlrc",
                "--set",
                "ON_ERROR_STOP=1",
                "--file",
                str(migration),
                cli_url,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        assert proc.returncode == 0, (
            f"migration failed: {migration.name}\n"
            f"stdout={proc.stdout[-4000:]}\n"
            f"stderr={proc.stderr[-4000:]}"
        )
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO schema_migrations(version) VALUES (:version) "
                    "ON CONFLICT (version) DO NOTHING"
                ),
                {"version": migration.name},
            )

    with engine.begin() as conn:
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
        for table in CRITICAL_STATE_TABLES:
            assert verified["target_state_row_counts"][table] == 0
        assert verified["migration_count"] == len(list(Path("migrations").glob("*.sql")))

        with target.connect() as conn:
            applied = {
                str(row[0])
                for row in conn.execute(text("SELECT version FROM schema_migrations")).all()
            }
        assert "0051_repair_kr_infinite_20260819_20260824.sql" in applied

        inspector = sa.inspect(target)
        for table_name, required_columns in REQUIRED_CONTRACT_COLUMNS.items():
            actual_columns = {
                str(col["name"])
                for col in inspector.get_columns(table_name, schema="public")
            }
            assert required_columns <= actual_columns
    finally:
        source.dispose()
        _drop_database(source_url, target_name)



def _complete_us_balance(**overrides):
    payload = {
        "positions": [],
        "balance_parse_status": "OK",
        "balance_complete": True,
        "balance_authoritative": True,
        "failed_exchanges": {},
        "queried_exchanges": ["NASD", "NYSE", "AMEX"],
        "exchange_result_counts": {"NASD": 0, "NYSE": 0, "AMEX": 0},
    }
    payload.update(overrides)
    return payload


def test_cutover_rejects_incomplete_us_balance_even_when_parse_status_ok():
    with pytest.raises(RuntimeError, match="US_KIS_BALANCE_INCOMPLETE"):
        _assert_us_balance_authoritative(
            _complete_us_balance(
                balance_complete=False,
                balance_authoritative=False,
                failed_exchanges={"NYSE": "timeout"},
            )
        )


def test_cutover_rejects_missing_us_exchange_coverage():
    with pytest.raises(RuntimeError, match="US_KIS_BALANCE_EXCHANGE_COVERAGE_INCOMPLETE"):
        _assert_us_balance_authoritative(
            _complete_us_balance(
                queried_exchanges=["NASD", "NYSE"],
                exchange_result_counts={"NASD": 0, "NYSE": 0},
            )
        )


def test_kr_cancelled_order_is_not_pending_and_all_pages_are_read():
    calls = []

    class FakeKis:
        def inquire_daily_ccld(self, **kwargs):
            calls.append(dict(kwargs))
            if len(calls) == 1:
                return {
                    "rt_cd": "0",
                    "output1": [{
                        "odno": "1",
                        "ord_qty": "10",
                        "tot_ccld_qty": "0",
                        "rmn_qty": "0",
                        "cncl_yn": "Y",
                        "status": "CANCEL_COMPLETE",
                    }],
                    "ctx_area_fk100": "NEXT_FK",
                    "ctx_area_nk100": "NEXT_NK",
                }
            return {
                "rt_cd": "0",
                "output1": [],
                "ctx_area_fk100": "",
                "ctx_area_nk100": "",
            }

    assert _kr_pending_orders(FakeKis()) == []
    assert len(calls) == 2
    assert calls[1]["ctx_area_fk100"] == "NEXT_FK"
    assert calls[1]["ctx_area_nk100"] == "NEXT_NK"


def test_kr_pending_order_on_later_page_blocks_cutover():
    calls = []

    class FakeKis:
        def inquire_daily_ccld(self, **kwargs):
            calls.append(dict(kwargs))
            if len(calls) == 1:
                return {
                    "rt_cd": "0",
                    "output1": [],
                    "CTX_AREA_FK100": "F2",
                    "CTX_AREA_NK100": "N2",
                }
            return {
                "rt_cd": "0",
                "output1": [{
                    "odno": "2",
                    "ord_qty": "10",
                    "tot_ccld_qty": "3",
                    "rmn_qty": "7",
                }],
                "CTX_AREA_FK100": "",
                "CTX_AREA_NK100": "",
            }

    pending = _kr_pending_orders(FakeKis())
    assert len(calls) == 2
    assert len(pending) == 1
    assert pending[0]["odno"] == "2"
