"""
Database utilities for the PB-Core v2 backend.
"""

from .config import get_database_url, get_db_echo, is_sqlite_url
from .engine import make_engine
from .schema import (
    FILLS,
    LEDGER_EVENTS,
    ORDERS,
    POSITIONS,
    RUNS,
    UNIVERSE_CURRENT,
    UNIVERSE_MEMBERS,
    UNIVERSE_RUNS,
    METADATA,
)


def run_migrations(engine, migrations_dir: str = "migrations"):
    from .migrate import run_migrations as _rm

    return _rm(engine, migrations_dir=migrations_dir)


__all__ = [
    "get_database_url",
    "get_db_echo",
    "is_sqlite_url",
    "make_engine",
    "run_migrations",
    "FILLS",
    "LEDGER_EVENTS",
    "ORDERS",
    "POSITIONS",
    "RUNS",
    "UNIVERSE_CURRENT",
    "UNIVERSE_MEMBERS",
    "UNIVERSE_RUNS",
    "METADATA",
]
