import logging
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError, DBAPIError

logger = logging.getLogger(__name__)

LOCK_KEY = 912345678


def acquire_advisory_lock(conn, key: int = LOCK_KEY) -> bool:
    return bool(conn.execute(sa.text("SELECT pg_try_advisory_lock(:k)"), {"k": key}).scalar())


def release_advisory_lock(conn, key: int = LOCK_KEY) -> None:
    """Release advisory lock (best-effort). Network errors are logged but not raised."""
    try:
        conn.execute(sa.text("SELECT pg_advisory_unlock(:k)"), {"k": key})
    except (OperationalError, DBAPIError) as exc:
        # SSL errors, connection drops during shutdown are expected
        logger.warning("[LOCK][RELEASE] best-effort release failed (ignoring): %s", exc)
    except Exception as exc:
        # Unexpected errors still logged but not raised (best-effort)
        logger.warning("[LOCK][RELEASE] unexpected error during release (ignoring): %s", exc)
