from __future__ import annotations

import os
from typing import Optional, Tuple

import sqlalchemy as sa
from sqlalchemy.engine import make_url

POSTGRES_PREFIXES = ("postgres://", "postgresql://", "postgresql+", "postgres+")
DB_URL_KEYS = (
    "PBCORE_DB_URL",
    "DATABASE_URL",
)


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


def get_db_url() -> str:
    url = (os.getenv("PBCORE_DB_URL") or os.getenv("DATABASE_URL") or "").strip()
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
    connect_args: dict = {}
    # 강제 플래그가 있으면 최우선
    if os.getenv("DB_DISABLE_PREPARED_STATEMENTS", "0") in {"1", "true", "TRUE"}:
        connect_args["prepare_threshold"] = None
        return connect_args

    # URL 기반 자동 감지
    if "pooler.supabase.com" in (db_url or "") or ":6543" in (db_url or ""):
        connect_args["prepare_threshold"] = None
    return connect_args


def make_engine() -> sa.Engine:
    url = get_db_url()
    try:
        # SQLAlchemy 엔진 생성 (psycopg v3 지원)
        connect_args = _connect_args_for_db_url(url)
        return sa.create_engine(
            url,
            connect_args=connect_args,
            execution_options={"compiled_cache": None},
            pool_pre_ping=True,
            pool_recycle=300,
        )
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Postgres driver missing. Install psycopg[binary]. "
            "Ensure psycopg is installed."
        ) from exc


# 싱글톤 엔진 인스턴스
_engine_instance: Optional[sa.Engine] = None


def get_engine() -> sa.Engine:
    """DB 엔진 싱글톤 getter - 모든 코드에서 통일해서 사용."""
    global _engine_instance
    if _engine_instance is None:
        _engine_instance = make_engine()
    return _engine_instance
