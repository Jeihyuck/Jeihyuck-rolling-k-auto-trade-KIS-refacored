from __future__ import annotations

import logging
import os
from typing import Optional, Tuple

import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, OperationalError, ProgrammingError, StatementError, TimeoutError as SATimeoutError

logger = logging.getLogger(__name__)

POSTGRES_PREFIXES = ("postgres://", "postgresql://", "postgresql+", "postgres+")
DB_URL_KEYS = (
    "PBCORE_DB_URL",
    "DATABASE_URL",
)
DB_LOCK_TIMEOUT_MS_DEFAULT = 5000
DB_STATEMENT_TIMEOUT_MS_DEFAULT = 15000
DB_IDLE_IN_TX_SESSION_TIMEOUT_MS_DEFAULT = 15000


def _redact_url(url: str) -> str:
    # user:pass@host 형태에서 pass 노출 방지
    if "@" in url:
        left = url.split("@", 1)[0]
        return f"{left}@***"
    return (url[:24] + "***") if len(url) > 24 else (url + "***")


def _pick_db_url() -> Tuple[str, str]:
    for key in DB_URL_KEYS:
        v = (os.getenv(key) or "").strip()
        if v:
            return v, key
    return "", ""


def _describe_db_url(url: str) -> Tuple[str, str]:
    try:
        parsed = make_url(url)
    except Exception:
        return "", ""
    drivername = parsed.drivername or ""
    base = drivername.split("+", 1)[0] if drivername else ""
    return drivername, base


def _normalize_db_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return url

    # SQLAlchemy 기본 드라이버(psycopg2) 경로를 피하고 psycopg3로 강제
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    elif url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg://", 1)

    # Supabase pooler/pgBouncer 대비: sslmode=require 기본 보장
    if url.startswith("postgresql+psycopg://") and "sslmode=" not in url.lower():
        url += ("&" if "?" in url else "?") + "sslmode=require"

    return url


def get_db_url() -> str:
    raw = (os.getenv("PBCORE_DB_URL") or os.getenv("DATABASE_URL") or "").strip()
    url = raw
    if not url:
        raise RuntimeError(
            "Postgres DB URL missing. "
            "Set PBCORE_DB_URL (preferred) or DATABASE_URL."
        )
    if not url.startswith(POSTGRES_PREFIXES):
        raise RuntimeError(f"postgres-only: invalid scheme for DB URL { _redact_url(url)}")
    return url


def _connect_args_for_db_url(db_url: str) -> dict:
    """
    Supabase pooler(6543, PgBouncer) 환경에서 psycopg3 prepared statement 충돌 방지.
    - psycopg3 문서: PgBouncer/풀러 사용 시 prepared statements 비활성화 권고
      -> prepare_threshold=None
    """
    connect_args: dict = {
        "connect_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", "15")),
        "keepalives": int(os.getenv("DB_KEEPALIVES", "1")),
        "keepalives_idle": int(os.getenv("DB_KEEPALIVES_IDLE", "30")),
        "keepalives_interval": int(os.getenv("DB_KEEPALIVES_INTERVAL", "10")),
        "keepalives_count": int(os.getenv("DB_KEEPALIVES_COUNT", "5")),
    }
    drivername, base_driver = _describe_db_url(db_url)
    if base_driver == "postgresql" or drivername.startswith("postgres"):
        lock_timeout_ms = int(os.getenv("DB_LOCK_TIMEOUT_MS", str(DB_LOCK_TIMEOUT_MS_DEFAULT)))
        statement_timeout_ms = int(os.getenv("DB_STATEMENT_TIMEOUT_MS", str(DB_STATEMENT_TIMEOUT_MS_DEFAULT)))
        idle_in_tx_timeout_ms = int(
            os.getenv(
                "DB_IDLE_IN_TX_SESSION_TIMEOUT_MS",
                str(DB_IDLE_IN_TX_SESSION_TIMEOUT_MS_DEFAULT),
            )
        )

        pg_options = [
            f"-c lock_timeout={lock_timeout_ms}",
            f"-c statement_timeout={statement_timeout_ms}",
            f"-c idle_in_transaction_session_timeout={idle_in_tx_timeout_ms}",
        ]

        existing_options = str(connect_args.get("options", "") or "").strip()
        connect_args["options"] = " ".join([opt for opt in [existing_options, *pg_options] if opt]).strip()
        logger.info(
            "[DB][CONNECT_ARGS][TIMEOUTS] connect_timeout=%s lock_timeout_ms=%s statement_timeout_ms=%s idle_in_tx_timeout_ms=%s",
            connect_args.get("connect_timeout"),
            lock_timeout_ms,
            statement_timeout_ms,
            idle_in_tx_timeout_ms,
        )

    app_name = (
        os.getenv("DB_APPLICATION_NAME")
        or os.getenv("PGAPPNAME")
        or f"pb1-{os.getenv('GITHUB_WORKFLOW', 'local')}-{os.getenv('GITHUB_JOB', 'job')}-{os.getenv('GITHUB_RUN_ID', 'no_run')}-{os.getenv('GITHUB_RUN_ATTEMPT', '0')}"
    )
    connect_args["application_name"] = app_name
    logger.info(
        "[DB][CONNECT_ARGS][APP] application_name=%s",
        app_name,
    )

    # 강제 플래그가 있으면 최우선
    if os.getenv("DB_DISABLE_PREPARED_STATEMENTS", "0") in {"1", "true", "TRUE"}:
        connect_args["prepare_threshold"] = None
        logger.info("[DB][CONNECT_ARGS][PREPARED] prepared_disabled=%s", int(connect_args.get("prepare_threshold") is None))
        return connect_args

    # URL 기반 자동 감지
    if "pooler.supabase.com" in (db_url or "") or ":6543" in (db_url or ""):
        connect_args["prepare_threshold"] = None
    logger.info("[DB][CONNECT_ARGS][PREPARED] prepared_disabled=%s", int(connect_args.get("prepare_threshold") is None))
    return connect_args


def make_engine() -> sa.Engine:
    url = _normalize_db_url(get_db_url())
    try:
        # SQLAlchemy 엔진 생성 (psycopg v3 지원)
        connect_args = _connect_args_for_db_url(url)
        return sa.create_engine(
            url,
            connect_args=connect_args,
            execution_options={"compiled_cache": None},
            pool_pre_ping=True,
            pool_reset_on_return="rollback",
            pool_recycle=int(os.getenv("DB_POOL_RECYCLE", "1800")),
            pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
            max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "10")),
            pool_timeout=int(os.getenv("DB_POOL_TIMEOUT", "10")),
            future=True,
        )
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Postgres driver missing. Install psycopg[binary]. "
            "Ensure psycopg is installed."
        ) from exc


def connection_has_active_transaction(conn) -> bool:
    try:
        in_transaction = getattr(conn, "in_transaction", None)
        if callable(in_transaction):
            return bool(in_transaction())
    except Exception:
        return False
    return False


def _is_connection_poison_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return any(
        token in msg
        for token in (
            "connection in transaction status active",
            "can't change 'autocommit'",
            "can't change autocommit",
            "server closed the connection",
            "connection already closed",
            "terminating connection",
            "statement timeout",
            "lock timeout",
            "pool timeout",
        )
    )


def dispose_engine_safely(engine: "sa.Engine | None" = None, *, reason: str = "") -> None:
    target = engine
    if target is None:
        try:
            target = get_engine()
        except Exception:
            target = None

    if target is None:
        return

    try:
        logger.warning("[DB][ENGINE][DISPOSE] reason=%s", reason or "unknown")
        target.dispose()
    except Exception as exc:
        logger.warning(
            "[DB][ENGINE][DISPOSE][FAIL] reason=%s err_type=%s err=%s",
            reason or "unknown",
            type(exc).__name__,
            exc,
        )


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _is_krx_context() -> bool:
    """현재 실행 컨텍스트가 한국장(KRX) 인지 판정."""
    values = [
        os.getenv("PB1_MARKET_SCOPE"),
        os.getenv("MARKET"),
        os.getenv("EXCHANGE"),
        os.getenv("TRADE_MARKET"),
    ]
    joined = " ".join(str(v or "").strip().lower() for v in values)
    workflow = str(os.getenv("GITHUB_WORKFLOW") or "").strip().lower()

    if any(token in joined for token in ["krx", "korea", "domestic"]):
        return True
    # "kr" 단독 매칭은 단어 경계에서만 허용
    if " kr " in f" {joined} ":
        return True
    if "trade am" in workflow or "trade pm" in workflow or "trade close" in workflow or "afternoon" in workflow:
        return True

    return False


def _is_runner_tick_timeout(exc: BaseException) -> bool:
    """TickTimeoutError 성격의 예외인지 판정."""
    name = exc.__class__.__name__
    msg = str(exc)
    return (
        name == "TickTimeoutError"
        or "tick_hard_timeout" in msg
        or ("timeout_sec=" in msg and "last_stage=" in msg)
    )


def _krx_db_fail_open_enabled(default: bool = False) -> bool:
    """KRX 컨텍스트에서 DB read fail-open 활성화 여부."""
    if not _is_krx_context():
        return False
    return _env_flag("KRX_DB_READ_FAIL_OPEN", default=default)


def safe_read_mappings(
    engine: sa.Engine,
    stmt,
    *,
    op_name: str,
    fail_open: bool = False,
) -> tuple[list[dict], bool]:
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            logger.info(
                "[DB][READ][PATH] op=%s active_tx=%s safe_mode=connect_only",
                op_name,
                int(connection_has_active_transaction(conn)),
            )
            # read query 직전에 statement_timeout / lock_timeout 명시적 재설정
            # (Supabase/PgBouncer pool connection에서 session options가 안 먹는 경우 방어)
            read_timeout_ms = int(
                os.getenv("DB_READ_STATEMENT_TIMEOUT_MS", os.getenv("DB_STATEMENT_TIMEOUT_MS", "15000"))
            )
            lock_timeout_ms = int(
                os.getenv("DB_READ_LOCK_TIMEOUT_MS", os.getenv("DB_LOCK_TIMEOUT_MS", "5000"))
            )
            try:
                conn.exec_driver_sql(f"SET statement_timeout = {read_timeout_ms}")
                conn.exec_driver_sql(f"SET lock_timeout = {lock_timeout_ms}")
                logger.info(
                    "[DB][READ][TIMEOUT_SET] op=%s statement_timeout_ms=%s lock_timeout_ms=%s krx=%s",
                    op_name,
                    read_timeout_ms,
                    lock_timeout_ms,
                    int(_is_krx_context()),
                )
            except Exception as _tset_exc:
                logger.warning(
                    "[DB][READ][TIMEOUT_SET][SKIP] op=%s err_type=%s err=%s",
                    op_name,
                    type(_tset_exc).__name__,
                    _tset_exc,
                )

            try:
                rows = conn.execute(stmt).mappings().all()
            finally:
                try:
                    conn.exec_driver_sql("RESET statement_timeout")
                    conn.exec_driver_sql("RESET lock_timeout")
                except Exception:
                    pass

            logger.info(
                "[DB][READ][OK] op=%s rows=%s fail_open=%s",
                op_name,
                len(rows),
                int(bool(fail_open)),
            )
            return [dict(row) for row in rows], False

    except (
        OperationalError,
        ProgrammingError,
        DBAPIError,
        SATimeoutError,
        StatementError,
    ) as exc:
        poison = _is_connection_poison_error(exc)
        logger.exception(
            "[DB][READ][FAIL] op=%s fail_open=%s poison=%s err_type=%s err=%s",
            op_name,
            int(bool(fail_open)),
            int(bool(poison)),
            type(exc).__name__,
            exc,
        )

        if poison:
            logger.warning(
                "[DB][READ][POISON] op=%s action=dispose_engine err_type=%s",
                op_name,
                type(exc).__name__,
            )
            dispose_engine_safely(engine, reason=f"safe_read_mappings:{op_name}:{type(exc).__name__}")

        if fail_open:
            logger.warning("[DB][READ][FAIL_OPEN] op=%s -> returning []", op_name)
            return [], True

        raise

    except Exception as exc:
        # TickTimeoutError(SIGALRM 기반) 처리 — KRX 한국장 한정 fail-open
        # BaseException / KeyboardInterrupt / SystemExit은 여기서 잡지 않음
        if _is_runner_tick_timeout(exc):
            krx_fail_open = bool(fail_open) or _krx_db_fail_open_enabled(default=False)
            # traceback_seen 방지: logger.exception() 대신 logger.error() 사용
            logger.error(
                "[DB][READ][TICK_TIMEOUT] op=%s fail_open=%s krx=%s err_type=%s err=%s",
                op_name,
                int(bool(krx_fail_open)),
                int(_is_krx_context()),
                type(exc).__name__,
                exc,
            )
            dispose_engine_safely(engine, reason=f"safe_read_mappings:{op_name}:TickTimeoutError")
            if krx_fail_open:
                logger.warning("[DB][READ][FAIL_OPEN] op=%s tick_timeout -> returning []", op_name)
                return [], True
        raise


# 싱글톤 엔진 인스턴스
_engine_instance: Optional[sa.Engine] = None


def get_engine() -> sa.Engine:
    """DB 엔진 싱글톤 getter - 모든 코드에서 통일해서 사용."""
    global _engine_instance
    if _engine_instance is None:
        _engine_instance = make_engine()
    return _engine_instance


# ✅ 호환성 레이어: 일부 스크립트에서 SessionLocal/get_db를 기대할 수 있음
from sqlalchemy.orm import sessionmaker, Session
from typing import Generator
from contextlib import contextmanager

_SessionLocal = None


def _get_session_local():
    """SessionLocal 지연 초기화."""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(bind=get_engine(), autocommit=False, autoflush=False)
    return _SessionLocal


def SessionLocal():
    """SessionLocal factory (호환성용)."""
    return _get_session_local()()


def get_db() -> Generator[Session, None, None]:
    """FastAPI 스타일 DB 세션 generator (호환성용)."""
    session_factory = _get_session_local()
    db = session_factory()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    """Context manager 스타일 DB 세션 (호환성용)."""
    session_factory = _get_session_local()
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
