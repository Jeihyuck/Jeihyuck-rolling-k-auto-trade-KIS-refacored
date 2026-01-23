import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def _db_echo() -> bool:
    return os.getenv("DB_ECHO", "false").lower() in {"1", "true", "yes", "on"}


def get_db_url() -> str:
    url = os.getenv("PBCORE_DB_URL", "").strip()
    if not url:
        raise RuntimeError(
            "PBCORE_DB_URL is required. Set GitHub Actions secret 'PBCORE_DB_URL' "
            "and inject it as env in the workflow."
        )
    if url.startswith("sqlite:"):
        raise RuntimeError("SQLite is forbidden. Use Postgres only.")
    return url


def make_engine() -> Engine:
    url = get_db_url()
    return create_engine(
        url,
        echo=_db_echo(),
        pool_pre_ping=True,
        pool_recycle=1800,
    )
