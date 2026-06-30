import logging
import os
import time
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError, DBAPIError

logger = logging.getLogger(__name__)

LOCK_KEY = 912345678


def _advisory_key_parts(key: int) -> tuple:
    """
    pg_try_advisory_lock(bigint) key를 pg_locks의 classid/objid와 비교하기 위한 helper.
    positive bigint 기준.
    """
    key = int(key)
    classid = (key >> 32) & 0xFFFFFFFF
    objid = key & 0xFFFFFFFF
    return classid, objid


def log_advisory_lock_holders(conn, *, key: int = LOCK_KEY, context: str = "", limit: int = 10) -> None:
    """
    advisory lock 획득 실패 시 현재 advisory lock holder를 진단 로그로 출력한다.
    """
    classid, objid = _advisory_key_parts(key)

    sql = sa.text("""
        SELECT
            l.pid,
            l.locktype,
            l.classid,
            l.objid,
            l.objsubid,
            l.granted,
            a.application_name,
            a.usename,
            a.client_addr,
            a.state,
            a.backend_start,
            a.xact_start,
            a.query_start,
            a.wait_event_type,
            a.wait_event,
            LEFT(REGEXP_REPLACE(COALESCE(a.query, ''), '\\s+', ' ', 'g'), 700) AS query
        FROM pg_locks l
        LEFT JOIN pg_stat_activity a ON a.pid = l.pid
        WHERE l.locktype = 'advisory'
        ORDER BY l.granted DESC, a.backend_start NULLS LAST
        LIMIT :limit
    """)

    try:
        rows = conn.execute(sql, {"limit": limit}).mappings().all()
    except Exception as exc:
        logger.warning(
            "[LOCK][HOLDER][QUERY_FAIL] key=%s context=%s err_type=%s err=%s",
            key,
            context,
            type(exc).__name__,
            exc,
        )
        return

    matching = [
        r for r in rows
        if int(r.get("classid") or 0) == classid and int(r.get("objid") or 0) == objid
    ]

    logger.warning(
        "[LOCK][UNAVAILABLE][SUMMARY] key=%s classid=%s objid=%s context=%s advisory_locks_total=%s matching=%s",
        key,
        classid,
        objid,
        context,
        len(rows),
        len(matching),
    )

    display_rows = matching if matching else rows

    for i, r in enumerate(display_rows):
        logger.warning(
            "[LOCK][HOLDER] idx=%s key=%s match=%s pid=%s granted=%s app=%s user=%s client=%s state=%s backend_start=%s xact_start=%s query_start=%s wait=%s/%s query=%s",
            i,
            key,
            int(r in matching),
            r.get("pid"),
            r.get("granted"),
            r.get("application_name"),
            r.get("usename"),
            r.get("client_addr"),
            r.get("state"),
            r.get("backend_start"),
            r.get("xact_start"),
            r.get("query_start"),
            r.get("wait_event_type"),
            r.get("wait_event"),
            r.get("query"),
        )


def acquire_advisory_lock(
    conn,
    key: int = LOCK_KEY,
    *,
    context: str = "",
    log_owner_on_fail: bool | None = None,
) -> bool:
    retries = int(os.getenv("LOCK_ACQUIRE_RETRIES", "3"))
    sleep_sec = float(os.getenv("LOCK_ACQUIRE_SLEEP_SEC", "0.5"))

    if log_owner_on_fail is None:
        log_owner_on_fail = os.getenv("PB1_LOCK_LOG_OWNER_ON_FAIL", "1").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

    for attempt in range(1, retries + 1):
        try:
            ok = bool(conn.execute(sa.text("SELECT pg_try_advisory_lock(:k)"), {"k": key}).scalar())
        except Exception as exc:
            logger.warning(
                "[LOCK][ACQUIRE][FAIL] key=%s context=%s attempt=%s/%s err_type=%s err=%s",
                key,
                context,
                attempt,
                retries,
                type(exc).__name__,
                exc,
            )
            ok = False

        if ok:
            logger.info(
                "[LOCK][ACQUIRE][OK] key=%s context=%s attempt=%s/%s",
                key,
                context,
                attempt,
                retries,
            )
            return True

        logger.warning(
            "[LOCK][ACQUIRE][BUSY] key=%s context=%s attempt=%s/%s sleep_sec=%.2f",
            key,
            context,
            attempt,
            retries,
            sleep_sec if attempt < retries else 0.0,
        )

        if attempt < retries:
            time.sleep(sleep_sec)

    if log_owner_on_fail:
        log_advisory_lock_holders(conn, key=key, context=context)

    return False


def release_advisory_lock(conn, key: int = LOCK_KEY) -> None:
    """Release the advisory lock held by the provided session connection."""
    try:
        unlocked = bool(conn.execute(sa.text("SELECT pg_advisory_unlock(:k)"), {"k": key}).scalar())
        logger.info("[LOCK][RELEASE][OK] key=%s source=provided_conn unlocked=%s", key, int(unlocked))
        if not unlocked:
            logger.warning("[LOCK][RELEASE][NOT_HELD] key=%s source=provided_conn unlocked=0", key)
        return
    except (OperationalError, DBAPIError) as exc:
        logger.warning("[LOCK][RELEASE][PROVIDED_CONN_FAIL] key=%s err=%s action=fresh_conn_fallback", key, exc)
    except Exception as exc:
        logger.warning("[LOCK][RELEASE][PROVIDED_CONN_FAIL] key=%s err=%s action=fresh_conn_fallback", key, exc)

    engine = getattr(conn, "engine", None)
    if engine is None:
        logger.warning("[LOCK][RELEASE][FRESH_CONN_SKIP] key=%s reason=no_engine", key)
        return
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as fresh_conn:
            unlocked = bool(fresh_conn.execute(sa.text("SELECT pg_advisory_unlock(:k)"), {"k": key}).scalar())
        if unlocked:
            logger.info("[LOCK][RELEASE][FRESH_CONN_UNLOCKED] key=%s unlocked=1", key)
        else:
            logger.warning("[LOCK][RELEASE][FRESH_CONN_NOT_HELD] key=%s unlocked=0 reason=advisory_locks_are_session_scoped", key)
    except Exception as exc:
        logger.warning("[LOCK][RELEASE][FRESH_CONN_FAIL] key=%s err=%s", key, exc)
