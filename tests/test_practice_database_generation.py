from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.engine import make_url

from trader.db.migrate import run_migrations
from trader.db.schema import schema_for_engine
from trader.db.practice_database_generation import (
    CRITICAL_STATE_TABLES,
    REQUIRED_CONTRACT_COLUMNS,
    _assert_required_trading_schema,
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
    """Current model baseline + actual 0020-and-later migrations, NOT legacy replay.

    The production SQLAlchemy model supplies the current TEXT-backed core tables.
    0001..0019 are explicitly recorded as a fixture baseline, not claimed to have
    run. Historical 0019 is incompatible with that model and universe ownership.
    Every later migration (including all US/Infinite/lifecycle DDL) must execute
    successfully before the production migrator records its version. No errors
    are caught or stamped as success by this fixture.
    """
    with engine.begin() as conn:
        conn.exec_driver_sql("DROP SCHEMA IF EXISTS public CASCADE")
        conn.exec_driver_sql("CREATE SCHEMA public")
    schema_for_engine(engine).metadata.create_all(engine)
    with engine.begin() as conn:
        # 0025 is a one-way TEXT -> JSONB migration, not an idempotent repair.
        # Restore its documented input type in this empty fixture so the real
        # migration runs, rather than skipping/stamping it as already applied.
        conn.exec_driver_sql("ALTER TABLE ledger_events ALTER COLUMN payload_json DROP DEFAULT")
        conn.exec_driver_sql(
            "ALTER TABLE ledger_events ALTER COLUMN payload_json TYPE TEXT USING payload_json::text"
        )
        conn.exec_driver_sql(
            "CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, "
            "applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        )
        baseline = sorted(p.name for p in Path("migrations").glob("*.sql") if p.name < "0020")
        assert len(baseline) == 19  # A reviewed, explicit baseline boundary.
        conn.execute(
            text("INSERT INTO schema_migrations(version) VALUES (:version)"),
            [{"version": version} for version in baseline],
        )
    run_migrations(engine)

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

        # Real PostgreSQL negative cases: matching migration stamps cannot hide
        # missing physical tables/columns. Roll back each DDL mutation so every
        # case starts from the independently constructed complete schema.
        for table_name in CRITICAL_STATE_TABLES:
            with target.connect() as conn:
                tx = conn.begin()
                try:
                    conn.exec_driver_sql(f'DROP TABLE "{table_name}" CASCADE')
                    with pytest.raises(PracticeDatabaseGenerationError, match="REQUIRED_TABLES_MISSING"):
                        _assert_required_trading_schema(conn, label="TARGET")
                finally:
                    tx.rollback()
        for table_name, columns in REQUIRED_CONTRACT_COLUMNS.items():
            for column in sorted(columns):
                with target.connect() as conn:
                    tx = conn.begin()
                    try:
                        conn.exec_driver_sql(f'ALTER TABLE "{table_name}" DROP COLUMN "{column}" CASCADE')
                        with pytest.raises(PracticeDatabaseGenerationError, match="REQUIRED_CONTRACT_COLUMNS_MISSING"):
                            _assert_required_trading_schema(conn, label="TARGET")
                    finally:
                        tx.rollback()

        # Also exercise the real top-level verifier with committed defects,
        # including identical source/target defects that schema parity misses.
        with target.begin() as conn:
            conn.exec_driver_sql("ALTER TABLE positions DROP COLUMN entry_meta_json")
        with pytest.raises(PracticeDatabaseGenerationError, match="TARGET_DATABASE_REQUIRED_CONTRACT_COLUMNS_MISSING"):
            verify_fresh_target_database(source_url=source_url, target_url=target_url)
        with source.begin() as conn:
            conn.exec_driver_sql("ALTER TABLE positions DROP COLUMN entry_meta_json")
        with pytest.raises(PracticeDatabaseGenerationError, match="SOURCE_DATABASE_REQUIRED_CONTRACT_COLUMNS_MISSING"):
            verify_fresh_target_database(source_url=source_url, target_url=target_url)
        for engine in (source, target):
            with engine.begin() as conn:
                conn.exec_driver_sql("DROP TABLE kr_infinite_state")
        with pytest.raises(PracticeDatabaseGenerationError, match="SOURCE_DATABASE_REQUIRED_TABLES_MISSING"):
            verify_fresh_target_database(source_url=source_url, target_url=target_url)
    finally:
        if "target" in locals():
            target.dispose()
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
