import os


def get_database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL environment variable must be set")
    return url


def get_db_echo() -> bool:
    return os.getenv("DB_ECHO", "false").lower() in ("1", "true", "yes", "on")


def is_sqlite_url(url: str) -> bool:
    return url.startswith("sqlite:")
