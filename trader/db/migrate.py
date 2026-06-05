import glob
import re
import logging
import os
from typing import Any

import sqlalchemy as sa
from sqlalchemy import Engine, text

from pathlib import Path

# Backward-compatible alias for get_engine
# (다른 코드가 migrate.get_engine을 사용할 수 있어서 호환성 유지)
from trader.db.engine import get_engine  # noqa: F401

logger = logging.getLogger(__name__)

_MIGRATION_GUARD: set[str] = set()

_US_FILLS_FIX_VERSION = "0043_us_fills_idempotency_and_order_reconcile_fix.sql"
_US_FILLS_IDEMPOTENT_INDEX = "uq_us_fills_idempotent"

_US_FILLS_DUPLICATE_COUNT_SQL = text(
    """
    SELECT COUNT(*)
    FROM (
        SELECT 1
        FROM us_fills
        GROUP BY
            trade_date,
            symbol,
            side,
            COALESCE(order_no, ''),
            COALESCE(client_order_key, ''),
            qty,
            price_usd
        HAVING COUNT(*) > 1
    ) dup_keys
    """
)

_US_FILLS_DUPLICATE_KEYS_SQL = text(
    """
    SELECT
        trade_date,
        symbol,
        side,
        COALESCE(order_no, '') AS order_no,
        COALESCE(client_order_key, '') AS client_order_key,
        qty,
        price_usd,
        COUNT(*) AS duplicate_rows
    FROM us_fills
    GROUP BY
        trade_date,
        symbol,
        side,
        COALESCE(order_no, ''),
        COALESCE(client_order_key, ''),
        qty,
        price_usd
    HAVING COUNT(*) > 1
    ORDER BY trade_date, symbol, side, order_no, client_order_key
    """
)

_US_FILLS_DEDUP_SQL = text(
    """
    WITH ranked AS (
        SELECT
            ctid,
            ROW_NUMBER() OVER (
                PARTITION BY
                    trade_date,
                    symbol,
                    side,
                    COALESCE(order_no, ''),
                    COALESCE(client_order_key, ''),
                    qty,
                    price_usd
                ORDER BY
                    COALESCE(updated_at, created_at, NOW()) DESC,
                    ctid DESC
            ) AS rn
        FROM us_fills
    )
    DELETE FROM us_fills f
    USING ranked r
    WHERE f.ctid = r.ctid
      AND r.rn > 1
    """
)

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


def _read_runs_column_types(engine: Engine) -> dict[str, str | None]:
    query = text(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'runs'
          AND column_name IN ('run_id', 'takeover_from_run_id')
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query).all()
    return {str(row.column_name): row.data_type for row in rows}


def _log_migration_failure_context(engine: Engine, version: str, exc: Exception) -> None:
    if version != "0036_runs_session_guard_metadata.sql":
        return
    try:
        column_types = _read_runs_column_types(engine)
        run_id_type = column_types.get("run_id")
        takeover_type = column_types.get("takeover_from_run_id")
        action = "alter_to_text" if run_id_type == "text" and takeover_type == "uuid" else "inspect"
        logger.error(
            "[DB][MIGRATE][TYPE_MISMATCH] version=0036 run_id_type=%s takeover_from_run_id_type=%s action=%s err=%s",
            run_id_type,
            takeover_type,
            action,
            exc,
        )
    except Exception as lookup_exc:
        logger.exception(
            "[DB][MIGRATE][TYPE_MISMATCH][LOOKUP_FAIL] version=0036 err=%s lookup_err=%s",
            exc,
            lookup_exc,
        )


def _apply_pg_statement(conn: sa.Connection, statement: str) -> None:
    cleaned = statement.strip()
    if not cleaned:
        return
    
    # Guard: psycopg placeholder trap
    # psycopg3 only allows %s/%b/%t placeholders; any %I/%L etc will explode.
    bad = ["%I", "%L", "%Q", "%R"]
    if any(x in cleaned for x in bad):
        raise RuntimeError(
            f"[MIGRATE] Forbidden percent-format token found in SQL (psycopg placeholder trap): {bad}"
        )

    index = 0
    while index < len(cleaned):
        if cleaned[index] != "%":
            index += 1
            continue
        next_char = cleaned[index + 1] if index + 1 < len(cleaned) else ""
        if next_char in {"%", "s", "b", "t"}:
            index += 2
            continue
        raise RuntimeError(
            "[MIGRATE] Unescaped percent token found in SQL. Use %% inside PL/pgSQL RAISE strings when executing via exec_driver_sql."
        )
    
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


def _statement_mentions_index(statement: str, index_name: str) -> bool:
    return index_name.lower() in statement.lower()


def _is_us_fills_index_create_failure(message: str) -> bool:
    lowered = message.lower()
    return "uniqueviolation" in lowered or "could not create unique index" in lowered


def _is_unique_violation(exc: Exception) -> bool:
    if isinstance(exc, sa.exc.IntegrityError):
        return True

    current: Any = exc
    while current is not None:
        class_name = current.__class__.__name__
        if class_name == "UniqueViolation":
            return True
        current = getattr(current, "orig", None) or getattr(current, "__cause__", None)
    return False


def _log_us_fills_duplicate_keys(conn: sa.Connection) -> None:
    rows = conn.execute(_US_FILLS_DUPLICATE_KEYS_SQL).fetchall()
    for row in rows:
        data = dict(row._mapping)
        logger.warning(
            "[DB][MIGRATE][DEDUP][DUPLICATE_KEY] trade_date=%s symbol=%s side=%s order_no=%s client_order_key=%s qty=%s price_usd=%s",
            data.get("trade_date"),
            data.get("symbol"),
            data.get("side"),
            data.get("order_no", ""),
            data.get("client_order_key", ""),
            data.get("qty"),
            data.get("price_usd"),
        )


def _dedup_us_fills_for_idempotent_index(conn: sa.Connection, *, log_keys: bool = False) -> tuple[int, int]:
    logger.info(
        "[DB][MIGRATE][DEDUP][START] table=us_fills index=%s",
        _US_FILLS_IDEMPOTENT_INDEX,
    )
    duplicate_count = int(conn.execute(_US_FILLS_DUPLICATE_COUNT_SQL).scalar() or 0)
    logger.info("[DB][MIGRATE][DEDUP][DUPLICATES] count=%d", duplicate_count)
    if log_keys and duplicate_count > 0:
        _log_us_fills_duplicate_keys(conn)
    deleted_count = int(conn.execute(_US_FILLS_DEDUP_SQL).rowcount or 0)
    logger.info("[DB][MIGRATE][DEDUP][DELETE] deleted=%d", deleted_count)
    logger.info("[DB][MIGRATE][DEDUP][DONE] status=OK")
    return duplicate_count, deleted_count


def _preflight_version(conn: sa.Connection, version: str) -> None:
    if version == _US_FILLS_FIX_VERSION:
        _dedup_us_fills_for_idempotent_index(conn)


def _try_recover_us_fills_unique_violation(
    conn: sa.Connection,
    *,
    version: str,
    statement: str,
    exc: Exception,
) -> bool:
    message = str(exc)
    if version != _US_FILLS_FIX_VERSION:
        return False
    if not _is_unique_violation(exc) and not _is_us_fills_index_create_failure(message):
        return False
    if _US_FILLS_IDEMPOTENT_INDEX not in message and not _statement_mentions_index(statement, _US_FILLS_IDEMPOTENT_INDEX):
        return False

    logger.warning(
        "[DB][MIGRATE][FAIL_RECOVERABLE] version=%s index=%s reason=duplicate_us_fills",
        version,
        _US_FILLS_IDEMPOTENT_INDEX,
    )
    _dedup_us_fills_for_idempotent_index(conn, log_keys=True)
    logger.info(
        "[DB][MIGRATE][DEDUP][RETRY_INDEX] index=%s",
        _US_FILLS_IDEMPOTENT_INDEX,
    )
    logger.info(
        "[DB][MIGRATE][INDEX][CREATE] index=%s",
        _US_FILLS_IDEMPOTENT_INDEX,
    )
    with conn.begin_nested():
        _apply_pg_statement(conn, statement)
    logger.info(
        "[DB][MIGRATE][INDEX][OK] index=%s",
        _US_FILLS_IDEMPOTENT_INDEX,
    )
    logger.info("[DB][MIGRATE][RECOVERED] version=%s", version)
    return True


def run_migrations(engine: Engine, migrations_dir: str = "migrations") -> None:
    if _should_skip_migrations(engine, migrations_dir):
        return
    if str(engine.url).startswith("sqlite"):
        raise RuntimeError("sqlite is forbidden. Use Postgres (PBCORE_DB_URL).")
    guard_key = f"{engine.url!s}|{Path(migrations_dir).resolve()}"
    if guard_key in _MIGRATION_GUARD:
        return
    _MIGRATION_GUARD.add(guard_key)
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
                _preflight_version(conn, version)
                statements = split_postgres_sql(sql)
                for statement in statements:
                    try:
                        if _statement_mentions_index(statement, _US_FILLS_IDEMPOTENT_INDEX):
                            logger.info(
                                "[DB][MIGRATE][INDEX][CREATE] index=%s",
                                _US_FILLS_IDEMPOTENT_INDEX,
                            )
                        with conn.begin_nested():
                            _apply_pg_statement(conn, statement)
                        if _statement_mentions_index(statement, _US_FILLS_IDEMPOTENT_INDEX):
                            logger.info(
                                "[DB][MIGRATE][INDEX][OK] index=%s",
                                _US_FILLS_IDEMPOTENT_INDEX,
                            )
                    except Exception as exc:
                        if _try_recover_us_fills_unique_violation(
                            conn,
                            version=version,
                            statement=statement,
                            exc=exc,
                        ):
                            continue
                        logger.exception(
                            "[DB][MIGRATE][FAIL] version=%s statement=%s err=%s",
                            version,
                            statement,
                            exc,
                        )
                        raise
                conn.execute(
                    text("INSERT INTO schema_migrations(version) VALUES (:version)"),
                    {"version": version},
                )
            except Exception as exc:
                _log_migration_failure_context(engine, version, exc)
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
