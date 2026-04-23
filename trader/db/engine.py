from __future__ import annotations

import logging
import os
from typing import Optional, Tuple

import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, OperationalError, StatementError, TimeoutError as SATimeoutError

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
            pool_timeout=int(os.getenv("DB_POOL_TIMEOUT", "30")),
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


def safe_read_mappings(
    engine: sa.Engine,
    stmt,
    *,
    op_name: str,
    fail_open: bool = False,
) -> tuple[list[dict], bool]:
    try:
        with engine.connect() as conn:
            logger.info(
                "[DB][READ][PATH] op=%s active_tx=%s safe_mode=connect_only",
                op_name,
                int(connection_has_active_transaction(conn)),
            )
            rows = conn.execute(stmt).mappings().all()
            logger.info("[DB][READ][OK] op=%s rows=%s fail_open=%s", op_name, len(rows), int(bool(fail_open)))
            return [dict(row) for row in rows], False
    except (OperationalError, DBAPIError, SATimeoutError, StatementError) as exc:
        logger.exception(
            "[DB][READ][FAIL] op=%s fail_open=%s err_type=%s err=%s",
            op_name,
            int(bool(fail_open)),
            type(exc).__name__,
            exc,
        )
        if fail_open:
            logger.warning("[FAIL_OPEN][DB][READ] op=%s -> returning []", op_name)
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
