from __future__ import annotations

import logging

import sqlalchemy as sa
from sqlalchemy import Engine, text

logger = logging.getLogger(__name__)


def try_acquire_lock(engine: Engine, key: str) -> bool:
    """
    Attempt to acquire a database-level advisory lock.
    """
    if engine.dialect.name != "postgresql":
        raise RuntimeError("Postgres is required for advisory locks.")

    try:
        with engine.begin() as conn:
            res = conn.execute(text("SELECT pg_try_advisory_lock(hashtext(:key))"), {"key": key})
            row = res.scalar()
            return bool(row)
    except Exception:
        logger.exception("Failed to acquire advisory lock key=%s", key)
        return False


def release_lock(engine: Engine, key: str) -> None:
    if engine.dialect.name != "postgresql":
        return
    try:
        with engine.begin() as conn:
            conn.execute(text("SELECT pg_advisory_unlock(hashtext(:key))"), {"key": key})
    except Exception:
        logger.exception("Failed to release advisory lock key=%s", key)
