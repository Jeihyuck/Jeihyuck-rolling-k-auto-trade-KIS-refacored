"""
Database utilities for the PB-Core v2 backend.
"""

from .engine import get_db_url, make_engine
from .schema import (
    FILLS,
    DERIVED_FLOW,
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


# SQLite is used by the KR contract suite.  Its CURRENT_TIMESTAMP is UTC-naive,
# while KR order/session windows are KST wall-clock values.  Install a
# dialect-only expression compatibility guard so midnight KST does not hide
# durable orders in CI/local SQLite.  PostgreSQL/live behaviour is untouched.
from .sqlite_kst_window_compat import install_sqlite_kst_order_window_compat

install_sqlite_kst_order_window_compat()


__all__ = [
    "get_db_url",
    "make_engine",
    "run_migrations",
    "FILLS",
    "DERIVED_FLOW",
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
