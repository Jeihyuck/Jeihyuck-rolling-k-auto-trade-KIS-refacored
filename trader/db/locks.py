import logging
import os
import time
from pathlib import Path
import subprocess
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError, DBAPIError

logger = logging.getLogger(__name__)

LOCK_KEY = 912345678


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _timeouts() -> tuple[int, int, int]:
    return (
        _int_env("DB_LOCK_CONN_LOCK_TIMEOUT_MS", _int_env("DB_LOCK_TIMEOUT_MS", 5000)),
        _int_env("DB_LOCK_CONN_STATEMENT_TIMEOUT_MS", 15000),
        _int_env("DB_LOCK_CONN_IDLE_IN_TX_SESSION_TIMEOUT_MS", 30000),
    )


def _xact_lock_idle_timeout_ms() -> int:
    """Return the timeout for the dedicated transaction-lock connection.

    PB1 deliberately keeps this connection idle while trading persistence uses
    other connections.  It must not inherit the general lock connection's
    idle-in-transaction timeout or PostgreSQL could release the xact lock
    during an active PB1 run.
    """
    return _int_env("DB_XACT_LOCK_IDLE_IN_TX_SESSION_TIMEOUT_MS", 0)


def _stale_holder(row: dict) -> tuple[bool, str | None]:
    """Return whether a holder is safe to consider for explicit termination."""
    threshold = _int_env("KR_LOCK_STALE_XACT_SEC", 300)
    state = str(row.get("state") or "").lower()
    query = str(row.get("query") or "").lower()
    xact_age = float(row.get("xact_age_seconds") or 0)
    advisory_query = "pg_try_advisory_lock" in query or "pg_try_advisory_xact_lock" in query
    is_stale = state == "idle in transaction" and advisory_query and xact_age >= threshold
    return is_stale, "idle_in_transaction_advisory_lock" if is_stale else None


def _termination_allowed() -> bool:
    enabled = os.getenv("KR_LOCK_TERMINATE_STALE_HOLDER", "0").strip().lower() in {"1", "true", "yes", "on"}
    env = os.getenv("STRATEGY_ENV", os.getenv("KIS_ENV", "practice")).strip().lower()
    return enabled and env in {"practice", "local", "dev", "development"}


def _terminate_stale_holder_if_allowed(conn, *, key: int, row: dict, current_pid: int | None) -> bool:
    """Terminate only a verified stale *other* backend in a non-live environment."""
    stale, _ = _stale_holder(row)
    pid = int(row.get("pid") or 0)
    if not (stale and _termination_allowed() and pid and pid != current_pid):
        return False
    logger.warning("[LOCK][STALE][TERMINATE_ATTEMPT] key=%s pid=%s xact_age_sec=%s", key, pid, row.get("xact_age_seconds"))
    try:
        terminated = bool(conn.execute(sa.text("SELECT pg_terminate_backend(:pid)"), {"pid": pid}).scalar())
        logger.warning("[LOCK][STALE][TERMINATE_DONE] key=%s pid=%s terminated=%s", key, pid, int(terminated))
        return terminated
    except Exception as exc:
        logger.warning("[LOCK][STALE][TERMINATE_FAIL] key=%s pid=%s err=%s", key, pid, exc)
        return False


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
            EXTRACT(EPOCH FROM (clock_timestamp() - a.backend_start)) AS age_seconds,
            EXTRACT(EPOCH FROM (clock_timestamp() - a.xact_start)) AS xact_age_seconds,
            EXTRACT(EPOCH FROM (clock_timestamp() - a.query_start)) AS query_age_seconds,
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

    current_pid = None
    try:
        current_pid = int(conn.execute(sa.text("SELECT pg_backend_pid()")).scalar())
    except Exception:
        pass
    for i, r in enumerate(display_rows):
        stale, stale_reason = _stale_holder(dict(r))
        logger.warning(
            "[LOCK][HOLDER]%s idx=%s key=%s match=%s pid=%s granted=%s application_name=%s user=%s client_addr=%s state=%s backend_start=%s xact_start=%s query_start=%s age_seconds=%s xact_age_seconds=%s query_age_seconds=%s wait_event_type=%s wait_event=%s is_stale_candidate=%s stale_reason=%s query=%s",
            "[STALE_CANDIDATE]" if stale else "",
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
            r.get("age_seconds"),
            r.get("xact_age_seconds"),
            r.get("query_age_seconds"),
            r.get("wait_event_type"),
            r.get("wait_event"),
            int(stale),
            stale_reason,
            r.get("query"),
        )
        _terminate_stale_holder_if_allowed(conn, key=key, row=dict(r), current_pid=current_pid)


def log_kr_lock_diagnostics(conn, *, key: int, context: str) -> None:
    """Emit local/process/DB diagnostics together when a KR execution lock is busy."""
    lock_files = sorted(str(path) for path in Path("runtime/locks").glob("*kr*"))
    logger.warning("[KR_LOCK_DIAG][LOCAL_LOCKS] exists=%s paths=%s", int(bool(lock_files)), lock_files)
    try:
        processes = subprocess.run(["ps", "-eo", "pid=,stat=,args="], text=True, capture_output=True, timeout=3, check=False).stdout
        kr_processes = [line.strip() for line in processes.splitlines() if "kr" in line.lower() or "pb1" in line.lower()][:10]
    except Exception as exc:
        kr_processes = [f"process_query_failed:{type(exc).__name__}"]
    logger.warning("[KR_LOCK_DIAG][PROCESS] matches=%s", kr_processes)
    log_advisory_lock_holders(conn, key=key, context=context)
    logger.warning("[KR_LOCK_DIAG][ACTION_HINT] stale DB session detected; set KR_LOCK_TERMINATE_STALE_HOLDER=1 in practice or terminate the reported pid manually")


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
            lock_timeout_ms, statement_timeout_ms, idle_timeout_ms = _timeouts()
            conn.execute(sa.text(f"SET lock_timeout = '{lock_timeout_ms}ms'"))
            conn.execute(sa.text(f"SET statement_timeout = {statement_timeout_ms}"))
            conn.execute(sa.text(f"SET idle_in_transaction_session_timeout = {idle_timeout_ms}"))
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
            held = True
            logger.info(
                "[LOCK][ACQUIRE][OK] key=%s context=%s attempt=%s/%s idle_timeout=%s statement_timeout=%s",
                key,
                context,
                attempt,
                retries,
                idle_timeout_ms,
                statement_timeout_ms,
            )
            logger.info("[LOCK][HELD][CHECK] key=%s held=%s", key, int(bool(held)))
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
        if "market=KR" in context or "strategy_env=" in context:
            log_kr_lock_diagnostics(conn, key=key, context=context)
        else:
            log_advisory_lock_holders(conn, key=key, context=context)

    return False


def acquire_advisory_xact_lock(conn, key: int = LOCK_KEY, *, context: str = "", log_owner_on_fail: bool | None = None) -> bool:
    """Acquire a transaction-scoped PostgreSQL advisory lock on *conn*.

    The caller must retain the transaction for the protected work.  Rollback,
    commit, or closing the connection releases the lock automatically.  This
    must be a dedicated lock-scope connection: never use it for order, fill,
    or report persistence, because release rolls back this transaction.
    """
    retries = _int_env("LOCK_ACQUIRE_RETRIES", 3)
    sleep_sec = float(os.getenv("LOCK_ACQUIRE_SLEEP_SEC", "0.5"))
    if log_owner_on_fail is None:
        log_owner_on_fail = os.getenv("PB1_LOCK_LOG_OWNER_ON_FAIL", "1").strip().lower() in {"1", "true", "yes", "on"}
    lock_timeout_ms, statement_timeout_ms, _ = _timeouts()
    idle_timeout_ms = _xact_lock_idle_timeout_ms()
    for attempt in range(1, retries + 1):
        try:
            conn.execute(sa.text(f"SET LOCAL lock_timeout = '{lock_timeout_ms}ms'"))
            conn.execute(sa.text(f"SET LOCAL statement_timeout = {statement_timeout_ms}"))
            conn.execute(sa.text(f"SET LOCAL idle_in_transaction_session_timeout = {idle_timeout_ms}"))
            ok = bool(conn.execute(sa.text("SELECT pg_try_advisory_xact_lock(:k)"), {"k": key}).scalar())
        except Exception as exc:
            logger.warning("[LOCK][ACQUIRE][FAIL] key=%s context=%s attempt=%s/%s err=%s", key, context, attempt, retries, exc)
            ok = False
        if ok:
            logger.info("[LOCK][ACQUIRE][OK] key=%s context=%s attempt=%s/%s scope=xact idle_timeout=%s statement_timeout=%s", key, context, attempt, retries, idle_timeout_ms, statement_timeout_ms)
            return True
        logger.warning("[LOCK][ACQUIRE][BUSY] key=%s context=%s attempt=%s/%s sleep_sec=%.2f", key, context, attempt, retries, sleep_sec if attempt < retries else 0)
        if attempt < retries:
            time.sleep(sleep_sec)
    if log_owner_on_fail:
        log_kr_lock_diagnostics(conn, key=key, context=context)
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


def release_advisory_xact_lock(conn, key: int = LOCK_KEY, *, context: str = "") -> None:
    """End a dedicated lock-only transaction; xact locks cannot be unlocked separately.

    Do not use ``conn`` for trading writes.  Its rollback releases only the
    advisory-lock transaction and must not be allowed to roll back persistence.
    """
    try:
        if getattr(conn, "in_transaction", lambda: False)():
            conn.rollback()
        logger.info("[LOCK][RELEASE][OK] key=%s scope=xact action=rollback context=%s", key, context)
    except Exception as exc:
        logger.error("[LOCK][RELEASE][MISSING_OR_FAILED] key=%s scope=xact context=%s err=%s", key, context, exc)
        raise
