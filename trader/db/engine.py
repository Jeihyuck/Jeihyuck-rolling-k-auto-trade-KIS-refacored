from __future__ import annotations

import os
from typing import Tuple

import sqlalchemy as sa

# Postgres-only 정책: 허용할 URL prefix들
ALLOWED_PG_PREFIXES = (
    "postgres://",
    "postgresql://",
)

# DB URL을 읽을 env 우선순위
DB_URL_KEYS = (
    "PBCORE_DB_URL",            # ✅ primary
    "DATABASE_URL",             # common
    "TRADER_DB_URL",
    "DB_URL",
    "SQLALCHEMY_DATABASE_URL",
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


def get_db_url() -> str:
    url, src = _pick_db_url()

    if not url:
        raise RuntimeError(
            "Postgres DB URL missing. "
            "Set PBCORE_DB_URL (preferred) or DATABASE_URL."
        )

    u = url.lower()

    # SQLite 명시 금지
    if u.startswith("sqlite:"):
        raise RuntimeError(
            f"SQLite is forbidden. Use Postgres only. "
            f"Got {src}=sqlite:***"
        )

    # Postgres 스킴 허용
    if u.startswith(ALLOWED_PG_PREFIXES):
        return url

    # 나머지 스킴은 모두 불허 (mysql 등)
    raise RuntimeError(
        "DB URL scheme not allowed (Postgres only). "
        f"Got {src}={_redact_url(url)}"
    )


def make_engine() -> sa.Engine:
    url = get_db_url()
    # SQLAlchemy 엔진 생성 (psycopg v3 지원)
    return sa.create_engine(url, pool_pre_ping=True)
