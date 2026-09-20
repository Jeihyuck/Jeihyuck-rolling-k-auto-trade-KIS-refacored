from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from sqlalchemy import inspect, text
from sqlalchemy.engine import make_url

from trader.db.engine import _connect_args_for_db_url


_DATABASE_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,62}$")

CRITICAL_STATE_TABLES = (
    "orders",
    "fills",
    "positions",
    "portfolio_epochs",
    "us_order_intents",
    "us_orders",
    "us_fills",
    "us_positions",
    "us_position_risk_state",
    "us_profit_capture_lifecycle",
    "us_tqqq_infinite_state",
    "kr_infinite_state",
    "kr_infinite_order_intents",
)

REQUIRED_TRADING_TABLES = {
    "orders",
    "fills",
    "positions",
    "us_order_intents",
    "us_orders",
    "us_fills",
    "us_positions",
}


class PracticeDatabaseGenerationError(RuntimeError):
    pass


def _driver_compatible_url(url: str) -> str:
    value = str(url or "").strip()
    if value.startswith("postgresql://"):
        return value.replace("postgresql://", "postgresql+psycopg://", 1)
    if value.startswith("postgres://"):
        return value.replace("postgres://", "postgresql+psycopg://", 1)
    return value


def _cli_postgres_url(url: str) -> str:
    parsed = make_url(_driver_compatible_url(url))
    if not str(parsed.drivername or "").startswith("postgres"):
        raise PracticeDatabaseGenerationError("POSTGRES_REQUIRED")
    return parsed.set(drivername="postgresql").render_as_string(hide_password=False)


def _engine_for_url(url: str) -> sa.Engine:
    compatible = _driver_compatible_url(url)
    return sa.create_engine(
        compatible,
        connect_args=_connect_args_for_db_url(compatible),
        pool_pre_ping=True,
        future=True,
    )


def _database_name(url: str) -> str:
    parsed = make_url(_driver_compatible_url(url))
    name = str(parsed.database or "").strip()
    if not name:
        raise PracticeDatabaseGenerationError("DATABASE_NAME_MISSING")
    return name


def _validate_database_name(name: str) -> str:
    value = str(name or "").strip()
    if not _DATABASE_NAME_RE.fullmatch(value):
        raise PracticeDatabaseGenerationError(
            "INVALID_NEW_DATABASE_NAME: use 1-63 chars, start with a letter, letters/digits/underscore only"
        )
    return value


def target_url_from_source(source_url: str, target_database_name: str) -> str:
    name = _validate_database_name(target_database_name)
    parsed = make_url(_driver_compatible_url(source_url))
    if not str(parsed.drivername or "").startswith("postgres"):
        raise PracticeDatabaseGenerationError("POSTGRES_REQUIRED")
    return parsed.set(database=name).render_as_string(hide_password=False)


def database_identity(url: str) -> dict[str, Any]:
    parsed = make_url(_driver_compatible_url(url))
    return {
        "driver": str(parsed.drivername or ""),
        "host": str(parsed.host or ""),
        "port": int(parsed.port) if parsed.port else None,
        "database": str(parsed.database or ""),
        "username_present": bool(parsed.username),
    }


def _same_database_endpoint(lhs_url: str, rhs_url: str) -> bool:
    lhs = make_url(_driver_compatible_url(lhs_url))
    rhs = make_url(_driver_compatible_url(rhs_url))
    return (
        str(lhs.host or "").lower(),
        int(lhs.port or 5432),
        str(lhs.database or ""),
    ) == (
        str(rhs.host or "").lower(),
        int(rhs.port or 5432),
        str(rhs.database or ""),
    )


def _public_tables(engine: sa.Engine) -> list[str]:
    return sorted(inspect(engine).get_table_names(schema="public"))


def state_row_counts(engine: sa.Engine) -> dict[str, int | None]:
    tables = set(_public_tables(engine))
    counts: dict[str, int | None] = {}
    with engine.connect() as conn:
        for table_name in CRITICAL_STATE_TABLES:
            if table_name not in tables:
                counts[table_name] = None
                continue
            counts[table_name] = int(
                conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar_one()
            )
    return counts


def migration_versions(engine: sa.Engine) -> list[str]:
    if "schema_migrations" not in set(_public_tables(engine)):
        return []
    with engine.connect() as conn:
        return sorted(
            str(row[0])
            for row in conn.execute(
                text("SELECT version FROM schema_migrations ORDER BY version")
            ).all()
        )


def repository_migration_versions(migrations_dir: str = "migrations") -> list[str]:
    return sorted(
        path.name
        for path in Path(migrations_dir).glob("*.sql")
        if path.is_file()
    )


def schema_signature(engine: sa.Engine) -> dict[str, dict[str, Any]]:
    """Stable public-schema signature proving structural source/target parity."""
    inspector = inspect(engine)
    signature: dict[str, dict[str, Any]] = {}
    for table_name in sorted(inspector.get_table_names(schema="public")):
        columns = [
            {
                "name": str(col.get("name") or ""),
                "type": str(col.get("type") or ""),
                "nullable": bool(col.get("nullable")),
            }
            for col in inspector.get_columns(table_name, schema="public")
        ]
        pk_raw = inspector.get_pk_constraint(table_name, schema="public") or {}
        pk = {
            "name": str(pk_raw.get("name") or ""),
            "columns": list(pk_raw.get("constrained_columns") or []),
        }
        uniques = sorted(
            (
                str(item.get("name") or ""),
                tuple(item.get("column_names") or []),
            )
            for item in (inspector.get_unique_constraints(table_name, schema="public") or [])
        )
        fks = sorted(
            (
                str(item.get("name") or ""),
                tuple(item.get("constrained_columns") or []),
                str(item.get("referred_schema") or "public"),
                str(item.get("referred_table") or ""),
                tuple(item.get("referred_columns") or []),
                str((item.get("options") or {}).get("ondelete") or ""),
            )
            for item in (inspector.get_foreign_keys(table_name, schema="public") or [])
        )
        indexes = sorted(
            (
                str(item.get("name") or ""),
                tuple(item.get("column_names") or []),
                bool(item.get("unique")),
            )
            for item in (inspector.get_indexes(table_name, schema="public") or [])
        )
        signature[table_name] = {
            "columns": columns,
            "primary_key": pk,
            "unique_constraints": uniques,
            "foreign_keys": fks,
            "indexes": indexes,
        }
    return signature


def source_archive_snapshot(engine: sa.Engine) -> dict[str, Any]:
    """Read-only evidence identifying the DB preserved as the historical archive."""
    with engine.connect() as conn:
        current_db = str(conn.execute(text("SELECT current_database()")).scalar_one())
    return {
        "database": current_db,
        "public_tables": _public_tables(engine),
        "state_row_counts": state_row_counts(engine),
        "migration_versions": migration_versions(engine),
        "schema_signature": schema_signature(engine),
    }


def _maintenance_url(source_url: str) -> str:
    parsed = make_url(_driver_compatible_url(source_url))
    host = str(parsed.host or "").lower()
    if "pooler.supabase.com" in host or int(parsed.port or 0) == 6543:
        raise PracticeDatabaseGenerationError(
            "DIRECT_CREATE_UNAVAILABLE_ON_POOLER: set PBCORE_NEW_DB_URL to a separately created empty PostgreSQL database"
        )
    return parsed.set(database="postgres").render_as_string(hide_password=False)


def create_empty_database_same_server(source_url: str, target_database_name: str) -> str:
    """Create a brand-new PostgreSQL DB. The source DB is never modified."""
    target_name = _validate_database_name(target_database_name)
    source_name = _database_name(source_url)
    if target_name == source_name:
        raise PracticeDatabaseGenerationError("TARGET_DATABASE_MUST_DIFFER_FROM_SOURCE")

    maintenance = _engine_for_url(_maintenance_url(source_url))
    try:
        with maintenance.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname=:name"),
                {"name": target_name},
            ).scalar()
            if exists:
                raise PracticeDatabaseGenerationError(
                    f"TARGET_DATABASE_ALREADY_EXISTS:{target_name}"
                )
            conn.exec_driver_sql(f'CREATE DATABASE "{target_name}" TEMPLATE template0')
    finally:
        maintenance.dispose()
    return target_url_from_source(source_url, target_name)


def _assert_target_fresh_before_clone(target_engine: sa.Engine) -> None:
    tables = _public_tables(target_engine)
    if tables:
        raise PracticeDatabaseGenerationError(
            "TARGET_DATABASE_NOT_EMPTY_BEFORE_SCHEMA_CLONE:" + ",".join(tables[:20])
        )


def _require_pg_client_tools() -> tuple[str, str]:
    pg_dump = shutil.which("pg_dump")
    psql = shutil.which("psql")
    if not pg_dump or not psql:
        raise PracticeDatabaseGenerationError(
            "POSTGRES_CLIENT_TOOLS_REQUIRED: install pg_dump and psql"
        )
    return pg_dump, psql


def clone_schema_only(*, source_url: str, target_url: str) -> None:
    """Copy current public schema only; never copy application data."""
    pg_dump, psql = _require_pg_client_tools()
    source_cli = _cli_postgres_url(source_url)
    target_cli = _cli_postgres_url(target_url)

    dump_cmd = [
        pg_dump,
        "--schema-only",
        "--no-owner",
        "--no-privileges",
        "--schema=public",
        source_cli,
    ]
    restore_cmd = [
        psql,
        "--no-psqlrc",
        "--set",
        "ON_ERROR_STOP=1",
        target_cli,
    ]

    dump = subprocess.Popen(
        dump_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
    )
    assert dump.stdout is not None
    restore = subprocess.run(
        restore_cmd,
        stdin=dump.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
        check=False,
    )
    dump.stdout.close()
    dump_stderr = dump.stderr.read() if dump.stderr is not None else b""
    dump_rc = dump.wait()

    if dump_rc != 0:
        raise PracticeDatabaseGenerationError(
            "PG_DUMP_SCHEMA_ONLY_FAILED:" + dump_stderr.decode("utf-8", errors="replace")[-2000:]
        )
    if restore.returncode != 0:
        raise PracticeDatabaseGenerationError(
            "PSQL_SCHEMA_RESTORE_FAILED:" + restore.stderr.decode("utf-8", errors="replace")[-2000:]
        )


def _copy_migration_stamps(source_engine: sa.Engine, target_engine: sa.Engine) -> list[str]:
    versions = migration_versions(source_engine)
    if not versions:
        raise PracticeDatabaseGenerationError("SOURCE_SCHEMA_MIGRATIONS_EMPTY")
    with target_engine.begin() as conn:
        conn.execute(text("DELETE FROM schema_migrations"))
        for version in versions:
            conn.execute(
                text("INSERT INTO schema_migrations(version) VALUES (:version)"),
                {"version": version},
            )
    return versions


def _assert_source_current(
    source_engine: sa.Engine,
    *,
    migrations_dir: str,
) -> list[str]:
    expected = repository_migration_versions(migrations_dir)
    actual = migration_versions(source_engine)
    if actual != expected:
        missing = sorted(set(expected) - set(actual))
        extra = sorted(set(actual) - set(expected))
        raise PracticeDatabaseGenerationError(
            f"SOURCE_MIGRATION_SET_MISMATCH missing={missing} extra={extra}"
        )
    return actual


def _assert_target_state_empty(target_engine: sa.Engine) -> dict[str, int | None]:
    counts = state_row_counts(target_engine)
    nonzero = {
        name: count
        for name, count in counts.items()
        if isinstance(count, int) and count != 0
    }
    if nonzero:
        raise PracticeDatabaseGenerationError(
            f"TARGET_DATABASE_STATE_NOT_EMPTY:{nonzero}"
        )
    missing = sorted(name for name in REQUIRED_TRADING_TABLES if counts.get(name) is None)
    if missing:
        raise PracticeDatabaseGenerationError(
            "TARGET_DATABASE_REQUIRED_TABLES_MISSING:" + ",".join(missing)
        )
    return counts


def _assert_schema_equivalent(
    source_engine: sa.Engine,
    target_engine: sa.Engine,
) -> None:
    source_sig = schema_signature(source_engine)
    target_sig = schema_signature(target_engine)
    if source_sig != target_sig:
        source_tables = set(source_sig)
        target_tables = set(target_sig)
        raise PracticeDatabaseGenerationError(
            "SOURCE_TARGET_SCHEMA_MISMATCH "
            f"missing_tables={sorted(source_tables-target_tables)} "
            f"extra_tables={sorted(target_tables-source_tables)}"
        )


def prepare_new_practice_database(
    *,
    source_url: str,
    target_database_name: str | None = None,
    target_url: str | None = None,
    migrations_dir: str = "migrations",
) -> dict[str, Any]:
    """Preserve the current DB and initialize a separate schema-identical empty DB."""
    if str(os.getenv("STRATEGY_ENV") or os.getenv("KIS_ENV") or "practice").strip().lower() != "practice":
        raise PracticeDatabaseGenerationError("PRACTICE_DATABASE_GENERATION_ONLY")
    if os.getenv("CREATE_NEW_PRACTICE_DB") != "1" or os.getenv("DB_GENERATION_CONFIRM") != "YES":
        raise PracticeDatabaseGenerationError("DATABASE_GENERATION_CONFIRMATION_REQUIRED")

    source_engine = _engine_for_url(source_url)
    target_engine: sa.Engine | None = None
    try:
        _assert_source_current(source_engine, migrations_dir=migrations_dir)
        source_before = source_archive_snapshot(source_engine)

        if target_url:
            resolved_target_url = str(target_url).strip()
            if not resolved_target_url:
                raise PracticeDatabaseGenerationError("PBCORE_NEW_DB_URL_EMPTY")
            if _same_database_endpoint(resolved_target_url, source_url):
                raise PracticeDatabaseGenerationError("TARGET_DATABASE_MUST_DIFFER_FROM_SOURCE")
        else:
            if not target_database_name:
                raise PracticeDatabaseGenerationError("NEW_PRACTICE_DB_NAME_REQUIRED")
            resolved_target_url = create_empty_database_same_server(
                source_url, target_database_name
            )

        target_engine = _engine_for_url(resolved_target_url)
        _assert_target_fresh_before_clone(target_engine)
        clone_schema_only(source_url=source_url, target_url=resolved_target_url)
        copied_versions = _copy_migration_stamps(source_engine, target_engine)
        _assert_schema_equivalent(source_engine, target_engine)
        target_counts = _assert_target_state_empty(target_engine)

        source_after = source_archive_snapshot(source_engine)
        immutable_keys = ("database", "public_tables", "migration_versions", "schema_signature")
        if any(source_before.get(key) != source_after.get(key) for key in immutable_keys):
            raise PracticeDatabaseGenerationError(
                "SOURCE_DATABASE_SCHEMA_CHANGED_DURING_SCHEMA_CLONE"
            )

        return {
            "status": "READY",
            "source_preserved": True,
            "history_copied_to_new_db": False,
            "schema_cloned_from_source": True,
            "source": database_identity(source_url),
            "target": database_identity(resolved_target_url),
            "source_archive_snapshot": source_before,
            "target_state_row_counts": target_counts,
            "target_migration_count": len(copied_versions),
            "target_latest_migration": copied_versions[-1] if copied_versions else None,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "cutover_required": True,
        }
    finally:
        if target_engine is not None:
            target_engine.dispose()
        source_engine.dispose()


def write_generation_manifest(result: dict[str, Any], directory: str | Path) -> Path:
    target_name = str((result.get("target") or {}).get("database") or "unknown")
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", target_name)
    output_dir = Path(directory)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"practice-db-generation-{safe_name}.json"
    path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return path


def verify_fresh_target_database(
    *,
    source_url: str,
    target_url: str,
    migrations_dir: str = "migrations",
) -> dict[str, Any]:
    if _same_database_endpoint(source_url, target_url):
        raise PracticeDatabaseGenerationError("TARGET_DATABASE_MUST_DIFFER_FROM_SOURCE")
    source_engine = _engine_for_url(source_url)
    target_engine = _engine_for_url(target_url)
    try:
        expected = _assert_source_current(source_engine, migrations_dir=migrations_dir)
        applied = migration_versions(target_engine)
        if applied != expected:
            missing = sorted(set(expected) - set(applied))
            extra = sorted(set(applied) - set(expected))
            raise PracticeDatabaseGenerationError(
                f"TARGET_MIGRATION_SET_MISMATCH missing={missing} extra={extra}"
            )
        _assert_schema_equivalent(source_engine, target_engine)
        counts = _assert_target_state_empty(target_engine)
        return {
            "status": "CUTOVER_DB_READY",
            "source": database_identity(source_url),
            "target": database_identity(target_url),
            "source_archive_snapshot": source_archive_snapshot(source_engine),
            "target_state_row_counts": counts,
            "migration_count": len(applied),
        }
    finally:
        target_engine.dispose()
        source_engine.dispose()
