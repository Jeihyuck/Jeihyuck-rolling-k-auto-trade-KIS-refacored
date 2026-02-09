"""
Database utilities for the PB-Core v2 backend.
"""

from .engine import get_db_url, make_engine
from .schema import (
    FILLS,
    DERIVED_MINERVINI,
    LEDGER_EVENTS,
    ORDERS,
    POSITIONS,
    RUNS,
    UNIVERSE_CURRENT,
    UNIVERSE_MEMBERS,
    UNIVERSE_RUNS,
    METADATA,
    JOB_CHECKPOINTS,
)


def run_migrations(engine, migrations_dir: str = "migrations"):
    from .migrate import run_migrations as _rm

    return _rm(engine, migrations_dir=migrations_dir)


__all__ = [
    "get_db_url",
    "make_engine",
    "run_migrations",
    "FILLS",
    "DERIVED_MINERVINI",
    "LEDGER_EVENTS",
    "ORDERS",
    "POSITIONS",
    "RUNS",
    "UNIVERSE_CURRENT",
    "UNIVERSE_MEMBERS",
    "UNIVERSE_RUNS",
    "METADATA",
    "JOB_CHECKPOINTS",
]
