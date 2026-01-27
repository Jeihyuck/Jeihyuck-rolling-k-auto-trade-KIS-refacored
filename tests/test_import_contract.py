"""Test import contract for critical modules to prevent regression."""
import pytest


def test_pb1_runner_import():
    """Ensure pb1_runner can be imported without ImportError."""
    import trader.pb1_runner  # noqa: F401


def test_positions_repo_import():
    """Ensure PositionsRepo can be imported from repos."""
    from trader.db.repos import PositionsRepo  # noqa: F401


def test_all_repos_import():
    """Ensure all public repo classes can be imported."""
    from trader.db.repos import (  # noqa: F401
        RunsRepo,
        UniverseRepo,
        OrdersRepo,
        FillsRepo,
        PositionsRepo,
        LedgerEventsRepo,
        ReconcileLogRepo,
    )