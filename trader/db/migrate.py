import glob
import logging
import os
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy import Engine, text

from . import config
from .schema import schema_for_engine


logger = logging.getLogger(__name__)


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
    with engine.begin() as conn:
        url = str(engine.url)
        if config.is_sqlite_url(url):
            logger.info("[DB][MIGRATE] sqlite url=%s", url)
            schema_for_engine(engine).metadata.create_all(engine)
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


if __name__ == "__main__":
    from .engine import make_engine

    run_migrations(make_engine())
