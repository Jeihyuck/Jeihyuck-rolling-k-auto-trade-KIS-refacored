import glob
import hashlib
import re
import logging
import os
import sqlite3
import stat
from pathlib import Path
from typing import Iterable

import sqlalchemy as sa
from sqlalchemy import Engine, text

from . import config
from .schema import schema_for_engine


logger = logging.getLogger(__name__)
MIGRATION_VERSION = "v1"
REQUIRED_TABLES = ("runs",)


def _sqlite_has_table(db_path: Path, table_name: str) -> bool:
    if not db_path.exists():
        return False
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table_name,),
            )
            row = cur.fetchone()
            return row is not None
        finally:
            conn.close()
    except Exception:
        return False


def _db_file_size(db_path: Path) -> int:
    try:
        return db_path.stat().st_size
    except Exception:
        return 0


def _must_bootstrap_sqlite(db_path: Path, required_tables: Iterable[str]) -> bool:
    if _db_file_size(db_path) == 0:
        return True
    for table in required_tables:
        if not _sqlite_has_table(db_path, table):
            return True
    return False


def _chmod_rw(path: Path) -> None:
    try:
        if path.is_dir():
            targets = [path] + list(path.rglob("*"))
        else:
            targets = [path]
        for target in targets:
            try:
                os.chmod(
                    target,
                    target.stat().st_mode | stat.S_IWUSR | stat.S_IRUSR,
                )
            except Exception:
                continue
    except Exception:
        pass


def _prepare_sqlite_path(db_path: Path) -> None:
    ensure_sqlite_writable(db_path)
    _chmod_rw(db_path)


def ensure_sqlite_writable(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(db_path.parent, 0o700)
    except Exception:
        logger.warning("[DB][PERM][DIR_CHMOD_FAIL] path=%s", db_path.parent, exc_info=True)

    if db_path.exists():
        try:
            os.chmod(db_path, 0o600)
        except Exception:
            logger.warning("[DB][PERM][FILE_CHMOD_FAIL] path=%s", db_path, exc_info=True)
    else:
        try:
            db_path.touch()
            os.chmod(db_path, 0o600)
        except Exception:
            logger.warning("[DB][PERM][TOUCH_FAIL] path=%s", db_path, exc_info=True)

    for suffix in ("-wal", "-shm"):
        sidecar = Path(f"{db_path}{suffix}")
        if sidecar.exists():
            try:
                sidecar.unlink()
            except Exception:
                logger.warning("[DB][PERM][SIDECAR_REMOVE_FAIL] path=%s", sidecar, exc_info=True)


def _schema_stamp_path() -> Path:
    cache_root = Path(os.getenv("TRADER_CACHE_ROOT", "bot_state/runtime"))
    return cache_root / "schema_version.txt"


def _compute_migration_version(migrations_dir: str) -> str:
    hasher = hashlib.sha256()
    hasher.update(MIGRATION_VERSION.encode("utf-8"))
    migration_files = sorted(Path(migrations_dir).glob("*.sql"))
    for path in migration_files:
        hasher.update(path.name.encode("utf-8"))
        try:
            hasher.update(path.read_bytes())
        except FileNotFoundError:
            continue
    return hasher.hexdigest()


def _should_skip_migrations(engine: Engine, migrations_dir: str) -> bool:
    mode = os.getenv("MIGRATE_MODE", "AUTO").upper()
    if mode == "OFF":
        logger.info("[DB][MIGRATE][SKIP] reason=disabled")
        return True
    if mode != "AUTO":
        return False
    stamp_path = _schema_stamp_path()
    if not stamp_path.exists():
        return False
    db_exists = True
    url = str(engine.url)
    if config.is_sqlite_url(url):
        db_path = Path(engine.url.database or "")
        db_exists = db_path.exists()
    if not db_exists:
        return False
    stamp_version = stamp_path.read_text(encoding="utf-8").strip()
    current_version = _compute_migration_version(migrations_dir)
    if stamp_version == current_version:
        logger.info("[DB][MIGRATE][SKIP] reason=up_to_date")
        return True
    return False


def _write_schema_stamp(migrations_dir: str) -> None:
    stamp_path = _schema_stamp_path()
    stamp_path.parent.mkdir(parents=True, exist_ok=True)
    stamp_path.write_text(_compute_migration_version(migrations_dir), encoding="utf-8")


def _ensure_schema_migrations_table(conn: sa.Connection) -> None:
    if conn.dialect.name == "sqlite":
        ddl = """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )
        """
    else:
        ddl = """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version text primary key,
            applied_at timestamptz default now()
        )
        """
    conn.exec_driver_sql(ddl)


def _list_applied_versions(conn: sa.Connection) -> set[str]:
    result = conn.execute(text("SELECT version FROM schema_migrations"))
    return {row[0] for row in result}


def _sqlite_column_names(conn: sa.Connection, table: str) -> set[str]:
    rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    return {row[1] for row in rows if len(row) > 1}


def _apply_sqlite_statement(conn: sa.Connection, statement: str) -> None:
    cleaned = statement.strip()
    if not cleaned:
        return
    match = re.match(r"ALTER\s+TABLE\s+(\w+)\s+ADD\s+COLUMN\s+(\w+)", cleaned, re.IGNORECASE)
    if match:
        table, column = match.group(1), match.group(2)
        existing = _sqlite_column_names(conn, table)
        if column in existing:
            logger.info("[DB][MIGRATE][SQLITE] skip existing column table=%s column=%s", table, column)
            return
    conn.execute(text(cleaned))


def run_migrations(engine: Engine, migrations_dir: str = "migrations") -> None:
    url = str(engine.url)
    db_path = None
    if config.is_sqlite_url(url):
        db_path = Path(engine.url.database or "")
        if db_path:
            _prepare_sqlite_path(db_path)
        if db_path and _must_bootstrap_sqlite(db_path, REQUIRED_TABLES):
            logger.warning(
                "[DB][MIGRATE][BOOTSTRAP] reason=missing_required_tables path=%s size=%s",
                db_path,
                _db_file_size(db_path),
            )
            schema_for_engine(engine).metadata.create_all(engine)
            _write_schema_stamp(migrations_dir)
            if db_path:
                logger.info(
                    "[DB][CHECK] path=%s exists=%s size=%s",
                    db_path,
                    int(os.path.exists(db_path)),
                    os.path.getsize(db_path) if os.path.exists(db_path) else -1,
                )
            return
    if _should_skip_migrations(engine, migrations_dir):
        return
    logger.info("[DB][MIGRATE][RUN] reason=stamp_miss_or_version_change")
    with engine.begin() as conn:
        url = str(engine.url)
        if config.is_sqlite_url(url):
            logger.info("[DB][MIGRATE] sqlite url=%s", url)
            _ensure_schema_migrations_table(conn)
            applied = _list_applied_versions(conn)
            migration_files = sorted(
                [
                    Path(path)
                    for path in glob.glob(os.path.join(migrations_dir, "*.sql"))
                    if Path(path).is_file()
                ]
            )
            for path in migration_files:
                version = path.name
                if version in applied:
                    continue
                sql = path.read_text()
                logger.info("[DB][MIGRATE] applying %s", version)
                try:
                    statements = [stmt for stmt in sql.split(";") if stmt.strip()]
                    for statement in statements:
                        _apply_sqlite_statement(conn, statement)
                    conn.execute(
                        text("INSERT INTO schema_migrations(version) VALUES (:version)"),
                        {"version": version},
                    )
                except Exception as exc:
                    snippet = " ".join(sql.split())[:2000]
                    logger.error("[DB][MIGRATE][FAIL] version=%s err=%s sql_snippet=%s", version, exc, snippet)
                    raise
            _write_schema_stamp(migrations_dir)
            if db_path:
                logger.info(
                    "[DB][CHECK] path=%s exists=%s size=%s",
                    db_path,
                    int(os.path.exists(db_path)),
                    os.path.getsize(db_path) if os.path.exists(db_path) else -1,
                )
            return
        logger.info("[DB][MIGRATE] external url=%s", url)
        # Ensure pgcrypto exists before any migration that uses gen_random_uuid().
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto;"))
        _ensure_schema_migrations_table(conn)
        applied = _list_applied_versions(conn)

        migration_files = sorted(
            [
                Path(path)
                for path in glob.glob(os.path.join(migrations_dir, "*.sql"))
                if Path(path).is_file()
            ]
        )
        for path in migration_files:
            version = path.name
            if version in applied:
                continue
            sql = path.read_text()
            logger.info("[DB][MIGRATE] applying %s", version)
            try:
                conn.execute(text(sql))
                conn.execute(
                    text("INSERT INTO schema_migrations(version) VALUES (:version)"),
                    {"version": version},
                )
            except Exception as exc:
                snippet = " ".join(sql.split())[:2000]
                logger.error("[DB][MIGRATE][FAIL] version=%s err=%s sql_snippet=%s", version, exc, snippet)
                raise
    _write_schema_stamp(migrations_dir)


if __name__ == "__main__":
    from .engine import make_engine

    run_migrations(make_engine())
