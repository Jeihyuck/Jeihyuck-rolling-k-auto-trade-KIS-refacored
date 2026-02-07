import logging
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError, DBAPIError

logger = logging.getLogger(__name__)

LOCK_KEY = 912345678


def acquire_advisory_lock(conn, key: int = LOCK_KEY) -> bool:
    return bool(conn.execute(sa.text("SELECT pg_try_advisory_lock(:k)"), {"k": key}).scalar())


def release_advisory_lock(conn, key: int = LOCK_KEY) -> None:
    """Release advisory lock (best-effort). Network errors are logged but not raised."""

    def _unlock(active_conn) -> None:
        active_conn.execute(sa.text("SELECT pg_advisory_unlock(:k)"), {"k": key})

    try:
        _unlock(conn)
    except (OperationalError, DBAPIError) as exc:
        logger.warning("[LOCK][RELEASE] failed, retry once with fresh conn: %s", exc)
        try:
            engine = getattr(conn, "engine", None)
            if engine is not None:
                engine.dispose()
                with engine.connect() as fresh_conn:
                    _unlock(fresh_conn)
            else:
                _unlock(conn)
        except Exception as exc2:
            logger.warning("[LOCK][RELEASE] best-effort release failed (ignoring): %s", exc2)
    except Exception as exc:
        logger.warning("[LOCK][RELEASE] unexpected error during release (ignoring): %s", exc)
