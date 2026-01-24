import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

def _db_echo() -> bool:
    return os.getenv("DB_ECHO", "false").lower() in {"1", "true", "yes", "on"}


def _pick_db_url() -> str:
    # ✅ 하나로 통일: PBCORE_DB_URL을 1순위로 강제
    for key in ("PBCORE_DB_URL", "DATABASE_URL", "TRADER_DB_URL", "DB_URL", "SQLALCHEMY_DATABASE_URL"):
        v = (os.getenv(key) or "").strip()
        if v:
            return v
    return ""


def get_db_url() -> str:
    url = _pick_db_url()
    if not url:
        raise RuntimeError(
            "Postgres DB URL missing. Set one of: PBCORE_DB_URL (preferred), DATABASE_URL, TRADER_DB_URL, DB_URL."
        )

    u = url.lower()
    if u.startswith("postgresql://") or u.startswith("postgres://"):
        return url

    # sqlite거나 다른 스킴이면 금지
    raise RuntimeError(f"SQLite is forbidden. Use Postgres only. Got url={url.split('@')[0]}***")


def make_engine() -> Engine:
    url = get_db_url()
    return create_engine(
        url,
        echo=_db_echo(),
        pool_pre_ping=True,
        pool_recycle=1800,
    )
