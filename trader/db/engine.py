from __future__ import annotations

import os
from typing import Tuple

import sqlalchemy as sa
from sqlalchemy.engine import make_url

ALLOWED_PG_SCHEMES = {"postgres", "postgresql"}
PG_PSYCO_PG_DRIVER = "postgresql+psycopg"

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


def _describe_db_url(url: str) -> Tuple[str, str]:
    try:
        parsed = make_url(url)
    except Exception:
        return "", ""
    drivername = parsed.drivername or ""
    base = drivername.split("+", 1)[0] if drivername else ""
    return drivername, base


def get_db_url() -> str:
    url, src = _pick_db_url()

    if not url:
        raise RuntimeError(
            "Postgres DB URL missing. "
            "Set PBCORE_DB_URL (preferred) or DATABASE_URL."
        )

    try:
        parsed = make_url(url)
    except Exception as exc:
        raise RuntimeError(
            "Invalid DB URL. "
            f"Got {src}={_redact_url(url)}"
        ) from exc

    drivername = parsed.drivername
    base = drivername.split("+", 1)[0] if drivername else ""

    if base not in ALLOWED_PG_SCHEMES:
        raise RuntimeError(
            "DB URL scheme not allowed (Postgres only). "
            f"Got {src}={_redact_url(url)} "
            f"(drivername={drivername}, base={base})"
        )

    normalized = parsed.set(drivername=PG_PSYCO_PG_DRIVER).render_as_string(
        hide_password=False
    )
    return normalized


def make_engine() -> sa.Engine:
    url = get_db_url()
    drivername = ""
    try:
        drivername = make_url(url).drivername or ""
        # SQLAlchemy 엔진 생성 (psycopg v3 지원)
        return sa.create_engine(url, pool_pre_ping=True)
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Postgres driver missing. Install psycopg[binary]. "
            f"Current URL driver={drivername}."
        ) from exc
