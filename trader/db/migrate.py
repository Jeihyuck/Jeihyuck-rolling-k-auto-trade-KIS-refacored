import glob
import hashlib
import re
import logging
import os
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy import Engine, text



logger = logging.getLogger(__name__)
MIGRATION_VERSION = "v1"


def split_postgres_sql(sql: str) -> list[str]:
    statements: list[str] = []
    buffer: list[str] = []
    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    in_block_comment = False
    dollar_tag: str | None = None
    i = 0
    length = len(sql)

    def flush_statement() -> None:
        statement = "".join(buffer).strip()
        if statement:
            statements.append(statement)
        buffer.clear()

    while i < length:
        char = sql[i]
        next_char = sql[i + 1] if i + 1 < length else ""

        if in_line_comment:
            buffer.append(char)
            if char == "\n":
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            buffer.append(char)
            if char == "*" and next_char == "/":
                buffer.append(next_char)
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        if dollar_tag is not None:
            if sql.startswith(dollar_tag, i):
                buffer.append(dollar_tag)
                i += len(dollar_tag)
                dollar_tag = None
            else:
                buffer.append(char)
                i += 1
            continue

        if in_single_quote:
            buffer.append(char)
            if char == "'" and next_char == "'":
                buffer.append(next_char)
                i += 2
                continue
            if char == "'":
                in_single_quote = False
            i += 1
            continue

        if in_double_quote:
            buffer.append(char)
            if char == '"' and next_char == '"':
                buffer.append(next_char)
                i += 2
                continue
            if char == '"':
                in_double_quote = False
            i += 1
            continue

        if char == "-" and next_char == "-":
            buffer.append(char)
            buffer.append(next_char)
            in_line_comment = True
            i += 2
            continue

        if char == "/" and next_char == "*":
            buffer.append(char)
            buffer.append(next_char)
            in_block_comment = True
            i += 2
            continue

        if char == "'":
            buffer.append(char)
            in_single_quote = True
            i += 1
            continue

        if char == '"':
            buffer.append(char)
            in_double_quote = True
            i += 1
            continue

        if char == "$":
            end = sql.find("$", i + 1)
            if end != -1:
                tag = sql[i + 1 : end]
                if tag == "" or all(ch.isalnum() or ch == "_" for ch in tag):
                    delimiter = f"${tag}$"
                    buffer.append(delimiter)
                    dollar_tag = delimiter
                    i = end + 1
                    continue
            buffer.append(char)
            i += 1
            continue

        if char == ";":
            flush_statement()
            i += 1
            continue

        buffer.append(char)
        i += 1

    flush_statement()
    return statements


def _strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
    out_lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("--"):
            continue
        if "--" in line:
            line = line.split("--", 1)[0]
        out_lines.append(line)
    return "\n".join(out_lines).strip()


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
    ddl = """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version TEXT PRIMARY KEY,
        applied_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
    )
    """
    conn.exec_driver_sql(ddl)


def _list_applied_versions(conn: sa.Connection) -> set[str]:
    result = conn.execute(text("SELECT version FROM schema_migrations"))
    return {row[0] for row in result}


def _apply_pg_statement(conn: sa.Connection, statement: str) -> None:
    cleaned = statement.strip()
    if not cleaned:
        return
    try:
        conn.exec_driver_sql(cleaned)
    except Exception as exc:
        if _should_ignore_pg_error(exc):
            logger.info("[DB][MIGRATE][PG] ignore error=%s statement=%s", exc, cleaned)
            return
        raise


def _should_ignore_pg_error(exc: Exception) -> bool:
    message = str(exc).lower()
    if "column" in message and ("already exists" in message or "does not exist" in message):
        return True
    if "relation" in message and "already exists" in message:
        return True
    if "duplicate_table" in message or "duplicate_column" in message:
        return True
    return False


def run_migrations(engine: Engine, migrations_dir: str = "migrations") -> None:
    if _should_skip_migrations(engine, migrations_dir):
        return
    logger.info("[DB][MIGRATE][RUN] reason=stamp_miss_or_version_change")
    with engine.begin() as conn:
        url = str(engine.url)
        logger.info("[DB][MIGRATE] external url=%s", url)
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
            sql = path.read_text(encoding="utf-8")
            logger.info("[DB][MIGRATE] applying %s", version)
            try:
                statements = split_postgres_sql(sql)
                for statement in statements:
                    _apply_pg_statement(conn, statement)
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
