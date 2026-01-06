from __future__ import annotations

import json
import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from trader.db import config as db_config

logger = logging.getLogger(__name__)


def _default_db_path() -> Path:
    env_path = os.getenv("UNIVERSE_SQLITE_PATH")
    if env_path:
        return Path(env_path)
    return db_config.DEFAULT_SQLITE_PATH


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS universe_cache (
            env TEXT NOT NULL,
            strategy TEXT NOT NULL,
            as_of TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(env, strategy, as_of)
        )
        """
    )
    conn.commit()


@dataclass
class SQLiteCacheProvider:
    db_path: Path | None = None

    def __post_init__(self) -> None:
        self.db_path = self.db_path or _default_db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        _ensure_table(conn)
        return conn

    def save_universe_cache(self, env: str, strategy: str, as_of: str, payload: dict) -> None:
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO universe_cache(env, strategy, as_of, payload_json, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (env, strategy, as_of, json.dumps(payload, ensure_ascii=False), datetime.utcnow().isoformat()),
                )
            logger.info("[UNIVERSE][SQLITE_CACHE][SAVE] env=%s strategy=%s as_of=%s path=%s", env, strategy, as_of, self.db_path)
        except Exception:
            logger.exception("[UNIVERSE][SQLITE_CACHE][SAVE_FAIL] env=%s strategy=%s as_of=%s path=%s", env, strategy, as_of, self.db_path)

    def load_latest_universe_cache(self, env: str, strategy: str) -> Optional[dict]:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT payload_json, as_of, created_at
                    FROM universe_cache
                    WHERE env=? AND strategy=?
                    ORDER BY as_of DESC, created_at DESC
                    LIMIT 1
                    """,
                    (env, strategy),
                ).fetchone()
            if not row:
                logger.info("[UNIVERSE][SQLITE_CACHE][MISS] env=%s strategy=%s path=%s", env, strategy, self.db_path)
                return None
            payload = json.loads(row[0])
            logger.info("[UNIVERSE][SQLITE_CACHE][HIT] env=%s strategy=%s as_of=%s path=%s", env, strategy, row[1], self.db_path)
            return payload
        except Exception:
            logger.exception("[UNIVERSE][SQLITE_CACHE][LOAD_FAIL] env=%s strategy=%s path=%s", env, strategy, self.db_path)
            return None

