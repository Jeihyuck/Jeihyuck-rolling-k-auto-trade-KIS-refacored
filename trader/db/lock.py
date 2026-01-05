from __future__ import annotations

import logging

import sqlalchemy as sa
from sqlalchemy import Engine, text

from .config import is_sqlite_url

logger = logging.getLogger(__name__)


def try_acquire_lock(engine: Engine, key: str) -> bool:
    """
    Attempt to acquire a database-level advisory lock.
    Falls back to a no-op lock on non-Postgres engines to keep local testing simple.
    """
    url = str(engine.url)
    if is_sqlite_url(url):
        return True

    try:
        with engine.begin() as conn:
            res = conn.execute(text("SELECT pg_try_advisory_lock(hashtext(:key))"), {"key": key})
            row = res.scalar()
            return bool(row)
    except Exception:
        logger.exception("Failed to acquire advisory lock key=%s", key)
        return False


def release_lock(engine: Engine, key: str) -> None:
    url = str(engine.url)
    if is_sqlite_url(url):
        return
    try:
        with engine.begin() as conn:
            conn.execute(text("SELECT pg_advisory_unlock(hashtext(:key))"), {"key": key})
    except Exception:
        logger.exception("Failed to release advisory lock key=%s", key)
