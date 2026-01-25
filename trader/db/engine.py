from __future__ import annotations

import os
from typing import Tuple

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


def make_engine() -> sa.Engine:
    url = get_db_url()
    try:
        # SQLAlchemy 엔진 생성 (psycopg v3 지원)
        return sa.create_engine(url, pool_pre_ping=True)
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Postgres driver missing. Install psycopg[binary]. "
            "Ensure psycopg is installed."
        ) from exc
