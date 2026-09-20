from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from sqlalchemy import inspect, text
from sqlalchemy.engine import make_url

from trader.db.engine import _connect_args_for_db_url
from trader.db.migrate import _apply_pg_statement, _preflight_version, _strip_sql_comments, split_postgres_sql


_DATABASE_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,62}$")

FRESH_BOOTSTRAP_SUPERSEDED_MIGRATIONS = {
    # This historical migration tried to convert every public run_id column to
    # UUID and re-point every run_id FK to runs(run_id). That is incompatible
    # with universe_* ownership and with the current application TEXT ID
    # contract. Existing production databases keep their historical stamp;
    # physically fresh DBs preserve the canonical TEXT schema instead.
    "0019_run_id_uuid_migration.sql",
}

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


class PracticeDatabaseGenerationError(RuntimeError):
    pass


def _driver_compatible_url(url: str) -> str:
    value = str(url or "").strip()
    if value.startswith("postgresql://"):
        return value.replace("postgresql://", "postgresql+psycopg://", 1)
    if value.startswith("postgres://"):
        return value.replace("postgres://", "postgresql+psycopg://", 1)
    return value


def _engine_for_url(url: str) -> sa.Engine:
    # Preserve the caller's SSL policy. The global runtime normalizer adds
    # sslmode=require for production safety, but this DB-generation utility
    # must also support local/direct PostgreSQL endpoints that do not expose
    # SSL (including CI). Production URLs that already specify sslmode keep it.
    compatible = _driver_compatible_url(url)
    return sa.create_engine(
        compatible,
        connect_args=_connect_args_for_db_url(compatible),
        pool_pre_ping=True,
        future=True,
    )


def _database_name(url: str) -> str:
    parsed = make_url(url)
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
    parsed = make_url(source_url)
    if not str(parsed.drivername or "").startswith("postgres"):
        raise PracticeDatabaseGenerationError("POSTGRES_REQUIRED")
    return parsed.set(database=name).render_as_string(hide_password=False)


def database_identity(url: str) -> dict[str, Any]:
    parsed = make_url(url)
    return {
        "driver": str(parsed.drivername or ""),
        "host": str(parsed.host or ""),
        "port": int(parsed.port) if parsed.port else None,
        "database": str(parsed.database or ""),
        "username_present": bool(parsed.username),
    }


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
            for row in conn.execute(text("SELECT version FROM schema_migrations ORDER BY version")).all()
        )


def source_archive_snapshot(engine: sa.Engine) -> dict[str, Any]:
    """Read-only evidence that identifies the DB preserved as the archive."""
    with engine.connect() as conn:
        current_db = str(conn.execute(text("SELECT current_database()")).scalar_one())
    return {
        "database": current_db,
        "public_tables": _public_tables(engine),
        "state_row_counts": state_row_counts(engine),
        "migration_versions": migration_versions(engine),
    }


def _maintenance_url(source_url: str) -> str:
    parsed = make_url(source_url)
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


def _assert_target_fresh_before_migration(target_engine: sa.Engine) -> None:
    tables = _public_tables(target_engine)
    if tables:
        raise PracticeDatabaseGenerationError(
            "TARGET_DATABASE_NOT_EMPTY_BEFORE_MIGRATION:" + ",".join(tables[:20])
        )


def _is_outer_transaction_control(statement: str) -> bool:
    """Return True for historical migration wrappers owned by this bootstrapper."""
    normalized = " ".join(_strip_sql_comments(statement).strip().rstrip(";").upper().split())
    return normalized in {
        "BEGIN",
        "BEGIN TRANSACTION",
        "START TRANSACTION",
        "COMMIT",
        "END",
    }


def _escape_psycopg_percent_literals(statement: str) -> str:
    """Escape lone percent literals for psycopg's client-side placeholder parser.

    Existing doubled percents are already DBAPI-safe and are preserved. The
    migration history contains PL/pgSQL RAISE format strings with both escaped
    and legacy lone percent tokens. Doubling a lone token is only transport
    escaping: PostgreSQL receives the original single percent.
    """
    out: list[str] = []
    index = 0
    while index < len(statement):
        char = statement[index]
        if char != "%":
            out.append(char)
            index += 1
            continue
        next_char = statement[index + 1] if index + 1 < len(statement) else ""
        if next_char == "%":
            out.append("%%")
            index += 2
            continue
        if next_char in {"s", "b", "t"}:
            raise PracticeDatabaseGenerationError(
                "PARAMETER_PLACEHOLDER_FORBIDDEN_IN_STATIC_MIGRATION"
            )
        out.append("%%")
        index += 1
    return "".join(out)


def run_fresh_database_migrations(
    engine: sa.Engine,
    *,
    migrations_dir: str = "migrations",
) -> list[str]:
    """Replay the complete migration history into a physically empty DB.

    The normal runtime migrator uses nested savepoints and is optimized for
    upgrading an existing production DB. Some historical files (notably 0017)
    include their own BEGIN/COMMIT wrapper, which invalidates those savepoints.
    A fresh DB has no concurrent data to recover, so bootstrap each migration
    file in one SQLAlchemy-owned transaction and ignore only the file's outer
    transaction-control statements. Runtime migration behavior remains unchanged.
    """
    migration_files = sorted(
        path for path in Path(migrations_dir).glob("*.sql") if path.is_file()
    )
    if not migration_files:
        raise PracticeDatabaseGenerationError("NO_MIGRATIONS_FOUND")

    applied: list[str] = []
    for migration_path in migration_files:
        version = migration_path.name
        sql = migration_path.read_text(encoding="utf-8")
        statements = split_postgres_sql(sql)
        try:
            with engine.begin() as conn:
                conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS schema_migrations (
                        version TEXT PRIMARY KEY,
                        applied_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
                    )
                    """
                )
                existing = conn.execute(
                    text("SELECT 1 FROM schema_migrations WHERE version=:version"),
                    {"version": version},
                ).scalar()
                if existing:
                    raise PracticeDatabaseGenerationError(
                        f"FRESH_TARGET_MIGRATION_ALREADY_STAMPED:{version}"
                    )
                if version in FRESH_BOOTSTRAP_SUPERSEDED_MIGRATIONS:
                    # Stamp only: the current schema contract deliberately
                    # supersedes this historical transition.
                    conn.execute(
                        text("INSERT INTO schema_migrations(version) VALUES (:version)"),
                        {"version": version},
                    )
                    applied.append(version)
                    continue

                _preflight_version(conn, version)
                for statement in statements:
                    if _is_outer_transaction_control(statement):
                        continue
                    _apply_pg_statement(conn, _escape_psycopg_percent_literals(statement))
                conn.execute(
                    text("INSERT INTO schema_migrations(version) VALUES (:version)"),
                    {"version": version},
                )
            applied.append(version)
        except Exception as exc:
            raise PracticeDatabaseGenerationError(
                f"FRESH_DATABASE_MIGRATION_FAILED:{version}:{type(exc).__name__}:{exc}"
            ) from exc
    return applied


def _assert_current_id_contract(target_engine: sa.Engine) -> None:
    """Fresh DB must match the current application's TEXT-backed identity model."""
    expected_text_columns = (
        ("runs", "run_id"),
        ("universe_runs", "run_id"),
        ("universe_members", "run_id"),
        ("universe_current", "run_id"),
        ("orders", "run_id"),
        ("fills", "run_id"),
        ("ledger_events", "run_id"),
    )
    query = text(
        """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=:table_name AND column_name=:column_name
        """
    )
    mismatches: list[str] = []
    with target_engine.connect() as conn:
        for table_name, column_name in expected_text_columns:
            data_type = conn.execute(
                query,
                {"table_name": table_name, "column_name": column_name},
            ).scalar()
            if str(data_type or "").lower() != "text":
                mismatches.append(f"{table_name}.{column_name}={data_type}")
    if mismatches:
        raise PracticeDatabaseGenerationError(
            "TARGET_DATABASE_ID_CONTRACT_MISMATCH:" + ",".join(mismatches)
        )


def _assert_target_state_empty(target_engine: sa.Engine) -> dict[str, int | None]:
    counts = state_row_counts(target_engine)
    nonzero = {name: count for name, count in counts.items() if isinstance(count, int) and count != 0}
    if nonzero:
        raise PracticeDatabaseGenerationError(f"TARGET_DATABASE_STATE_NOT_EMPTY:{nonzero}")
    required = {"orders", "fills", "positions", "us_order_intents", "us_orders", "us_fills", "us_positions"}
    missing = sorted(name for name in required if counts.get(name) is None)
    if missing:
        raise PracticeDatabaseGenerationError(
            "TARGET_DATABASE_REQUIRED_TABLES_MISSING:" + ",".join(missing)
        )
    return counts


def prepare_new_practice_database(
    *,
    source_url: str,
    target_database_name: str | None = None,
    target_url: str | None = None,
    migrations_dir: str = "migrations",
) -> dict[str, Any]:
    """Preserve the current DB unchanged and initialize a separate fresh DB.

    If target_url is supplied, that database must already exist and be empty.
    Otherwise a new database is created on the same PostgreSQL server.
    """
    if str(os.getenv("STRATEGY_ENV") or os.getenv("KIS_ENV") or "practice").strip().lower() != "practice":
        raise PracticeDatabaseGenerationError("PRACTICE_DATABASE_GENERATION_ONLY")
    if os.getenv("CREATE_NEW_PRACTICE_DB") != "1" or os.getenv("DB_GENERATION_CONFIRM") != "YES":
        raise PracticeDatabaseGenerationError("DATABASE_GENERATION_CONFIRMATION_REQUIRED")

    source_engine = _engine_for_url(source_url)
    created_here = False
    new_engine: sa.Engine | None = None
    try:
        source_before = source_archive_snapshot(source_engine)

        if target_url:
            resolved_target_url = str(target_url).strip()
            if not resolved_target_url:
                raise PracticeDatabaseGenerationError("PBCORE_NEW_DB_URL_EMPTY")
            if _database_name(resolved_target_url) == _database_name(source_url):
                raise PracticeDatabaseGenerationError("TARGET_DATABASE_MUST_DIFFER_FROM_SOURCE")
        else:
            if not target_database_name:
                raise PracticeDatabaseGenerationError("NEW_PRACTICE_DB_NAME_REQUIRED")
            resolved_target_url = create_empty_database_same_server(
                source_url, target_database_name
            )
            created_here = True

        new_engine = _engine_for_url(resolved_target_url)
        _assert_target_fresh_before_migration(new_engine)
        target_versions = run_fresh_database_migrations(new_engine, migrations_dir=migrations_dir)
        _assert_current_id_contract(new_engine)
        target_counts = _assert_target_state_empty(new_engine)

        source_after = source_archive_snapshot(source_engine)
        if source_before["public_tables"] != source_after["public_tables"]:
            raise PracticeDatabaseGenerationError("SOURCE_DATABASE_SCHEMA_CHANGED_DURING_PREPARE")
        if source_before["migration_versions"] != source_after["migration_versions"]:
            raise PracticeDatabaseGenerationError("SOURCE_DATABASE_MIGRATIONS_CHANGED_DURING_PREPARE")

        return {
            "status": "READY",
            "source_preserved": True,
            "history_copied_to_new_db": False,
            "source": database_identity(source_url),
            "target": database_identity(resolved_target_url),
            "source_archive_snapshot": source_before,
            "target_state_row_counts": target_counts,
            "target_migration_count": len(target_versions),
            "target_latest_migration": target_versions[-1] if target_versions else None,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "cutover_required": True,
        }
    except Exception:
        # Never delete or mutate the source DB. A newly created target is left
        # in place for inspection instead of risking an accidental DROP.
        raise
    finally:
        if new_engine is not None:
            new_engine.dispose()
        source_engine.dispose()


def write_generation_manifest(result: dict[str, Any], directory: str | Path) -> Path:
    target_name = str((result.get("target") or {}).get("database") or "unknown")
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", target_name)
    output_dir = Path(directory)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"practice-db-generation-{safe_name}.json"
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return path


def verify_fresh_target_database(
    *,
    source_url: str,
    target_url: str,
    migrations_dir: str = "migrations",
) -> dict[str, Any]:
    if _database_name(source_url) == _database_name(target_url):
        raise PracticeDatabaseGenerationError("TARGET_DATABASE_MUST_DIFFER_FROM_SOURCE")
    source_engine = _engine_for_url(source_url)
    target_engine = _engine_for_url(target_url)
    try:
        current_files = sorted(path.name for path in Path(migrations_dir).glob("*.sql") if path.is_file())
        applied = migration_versions(target_engine)
        if applied != current_files:
            missing = sorted(set(current_files) - set(applied))
            extra = sorted(set(applied) - set(current_files))
            raise PracticeDatabaseGenerationError(
                f"TARGET_MIGRATION_SET_MISMATCH missing={missing} extra={extra}"
            )
        _assert_current_id_contract(target_engine)
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
