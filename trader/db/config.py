import os
from pathlib import Path

from sqlalchemy.engine.url import make_url


DEFAULT_SQLITE_PATH = Path(os.getenv("UNIVERSE_SQLITE_PATH") or Path(__file__).resolve().parents[1] / "state" / "pbcore.sqlite3")


def using_external_db() -> bool:
    return os.getenv("USING_EXTERNAL_DB", "0").lower() in {"1", "true", "yes", "on"}


def _ensure_sqlite_parent(url: str) -> None:
    try:
        if not is_sqlite_url(url):
            return
        db_path = make_url(url).database
        if not db_path:
            return
        Path(db_path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        # Best-effort only; filesystem permissions may vary.
        return


def get_database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if using_external_db():
        if not url:
            raise RuntimeError("DATABASE_URL must be set when USING_EXTERNAL_DB=1")
        return url

    if url and is_sqlite_url(url):
        _ensure_sqlite_parent(url)
        return url

    default_url = f"sqlite:///{DEFAULT_SQLITE_PATH}"
    _ensure_sqlite_parent(default_url)
    return default_url


def get_db_echo() -> bool:
    return os.getenv("DB_ECHO", "false").lower() in ("1", "true", "yes", "on")


def is_sqlite_url(url: str) -> bool:
    return url.startswith("sqlite:")
