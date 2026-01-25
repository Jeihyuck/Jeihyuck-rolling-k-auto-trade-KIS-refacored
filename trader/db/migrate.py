import glob
import re
import logging
import os

import sqlalchemy as sa
from sqlalchemy import Engine, text

from pathlib import Path


logger = logging.getLogger(__name__)

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


def _should_skip_migrations(engine: Engine, migrations_dir: str) -> bool:
    mode = os.getenv("MIGRATE_MODE", "AUTO").upper()
    if mode == "OFF":
        logger.info("[DB][MIGRATE][SKIP] reason=disabled")
        return True
    return False


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
    if str(engine.url).startswith("sqlite"):
        raise RuntimeError("sqlite is forbidden. Use Postgres (PBCORE_DB_URL).")
    logger.info("[DB][MIGRATE][RUN] reason=stamp_miss_or_version_change")
    migration_files = sorted(
        [
            Path(path)
            for path in glob.glob(os.path.join(migrations_dir, "*.sql"))
            if Path(path).is_file()
        ]
    )
    with engine.begin() as conn:
        url = str(engine.url)
        logger.info("[DB][MIGRATE] external url=%s", url)
        _ensure_schema_migrations_table(conn)
        applied = _list_applied_versions(conn)

        for path in migration_files:
            version = path.name
            if version in applied:
                continue
            sql = path.read_text(encoding="utf-8")
            logger.info("[DB][MIGRATE][APPLY] version=%s", version)
            try:
                statements = split_postgres_sql(sql)
                for statement in statements:
                    try:
                        _apply_pg_statement(conn, statement)
                    except Exception as exc:
                        logger.exception(
                            "[DB][MIGRATE][FAIL] version=%s statement=%s err=%s",
                            version,
                            statement,
                            exc,
                        )
                        try:
                            conn.rollback()
                            logger.info("[DB][MIGRATE][ROLLBACK] version=%s", version)
                        except Exception:
                            logger.exception("[DB][MIGRATE][ROLLBACK_FAIL] version=%s", version)
                        raise
                conn.execute(
                    text("INSERT INTO schema_migrations(version) VALUES (:version)"),
                    {"version": version},
                )
            except Exception as exc:
                snippet = " ".join(sql.split())[:2000]
                logger.exception(
                    "[DB][MIGRATE][FAIL] version=%s err=%s sql_snippet=%s",
                    version,
                    exc,
                    snippet,
                )
                raise
    logger.info("[DB][MIGRATE][MIGRATE OK] count=%s", len(migration_files))


if __name__ == "__main__":
    from .engine import make_engine

    run_migrations(make_engine())
