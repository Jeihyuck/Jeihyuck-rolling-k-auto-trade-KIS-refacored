import glob
import hashlib
import logging
import os
import stat
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy import Engine, text

from . import config
from .schema import schema_for_engine


logger = logging.getLogger(__name__)
MIGRATION_VERSION = "v1"


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
    db_path.parent.mkdir(parents=True, exist_ok=True)
    _chmod_rw(db_path.parent)
    if not db_path.exists():
        db_path.touch()
    _chmod_rw(db_path)


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
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version text primary key,
                applied_at timestamptz default now()
            )
            """
        )
    )


def _list_applied_versions(conn: sa.Connection) -> set[str]:
    result = conn.execute(text("SELECT version FROM schema_migrations"))
    return {row[0] for row in result}


def run_migrations(engine: Engine, migrations_dir: str = "migrations") -> None:
    url = str(engine.url)
    if config.is_sqlite_url(url):
        db_path = Path(engine.url.database or "")
        if db_path:
            _prepare_sqlite_path(db_path)
    if _should_skip_migrations(engine, migrations_dir):
        return
    logger.info("[DB][MIGRATE][RUN] reason=stamp_miss_or_version_change")
    with engine.begin() as conn:
        url = str(engine.url)
        if config.is_sqlite_url(url):
            logger.info("[DB][MIGRATE] sqlite url=%s", url)
            schema_for_engine(engine).metadata.create_all(engine)
            _write_schema_stamp(migrations_dir)
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
