"""
Test watchlist bundle save/recover logic.

Tests that the 4-stage bundle (universe_scored, pool120, top50, final30)
is properly saved and recovered from DB.
"""
from __future__ import annotations

import os
from datetime import date

import pytest

from trader.db.engine import get_engine
from trader.db.repos import WatchlistRepo
from trader.watchlist_builder import (
    WatchlistBundle,
    save_bundle,
    recover_bundle_from_db,
)


@pytest.fixture
def test_engine():
    """Use test database."""
    os.environ["DB_URL"] = os.getenv("DB_URL", "postgresql://user:password@localhost:5432/trader_test")
    engine = get_engine()
    yield engine
    engine.dispose()


@pytest.fixture
def sample_bundle():
    """Create a sample bundle for testing."""
    return WatchlistBundle(
        as_of="2026-02-24",
        env="test",
        strategy="pb1_watchlist",
        universe_scored=[{"code": f"00{i:04d}", "score": 80 + i} for i in range(1, 101)],
        pool120=[{"code": f"00{i:04d}", "score": 85 + i} for i in range(1, 121)],
        top50=[{"code": f"00{i:04d}", "score": 90 + i} for i in range(1, 51)],
        final30=[{"code": f"00{i:04d}", "score": 95 + i} for i in range(1, 31)],
        meta={
            "source": "test",
            "weights": {"tech_weight": 0.7, "flow_weight": 0.3},
        },
    )


def test_save_bundle_all_4_stages(test_engine, sample_bundle):
    """Test that save_bundle saves all 4 stages to DB."""
    as_of = date(2026, 2, 24)
    
    # Save bundle
    save_bundle(
        engine=test_engine,
        env="test",
        as_of=as_of,
        bundle=sample_bundle,
    )
    
    # Verify all 4 stages are saved
    repo = WatchlistRepo(test_engine)
    
    universe_rows, _ = repo.load_watchlist(
        env="test",
        strategy="pb1_universe_scored",
        as_of=as_of,
        allow_latest_fallback=False,
    )
    assert universe_rows is not None
    assert len(universe_rows) == 100, f"Expected 100 universe_scored rows, got {len(universe_rows)}"
    
    pool120_rows, _ = repo.load_watchlist(
        env="test",
        strategy="pb1_pool120",
        as_of=as_of,
        allow_latest_fallback=False,
    )
    assert pool120_rows is not None
    assert len(pool120_rows) == 120, f"Expected 120 pool120 rows, got {len(pool120_rows)}"
    
    top50_rows, _ = repo.load_watchlist(
        env="test",
        strategy="pb1_top50",
        as_of=as_of,
        allow_latest_fallback=False,
    )
    assert top50_rows is not None
    assert len(top50_rows) == 50, f"Expected 50 top50 rows, got {len(top50_rows)}"
    
    final30_rows, _ = repo.load_watchlist(
        env="test",
        strategy="pb1_watchlist_final",
        as_of=as_of,
        allow_latest_fallback=False,
    )
    assert final30_rows is not None
    assert len(final30_rows) == 30, f"Expected 30 final30 rows, got {len(final30_rows)}"


def test_recover_bundle_from_db_success(test_engine, sample_bundle):
    """Test successful bundle recovery from DB."""
    as_of = date(2026, 2, 24)
    
    # Save bundle first
    save_bundle(
        engine=test_engine,
        env="test",
        as_of=as_of,
        bundle=sample_bundle,
    )
    
    # Recover bundle
    recovered = recover_bundle_from_db(
        engine=test_engine,
        env="test",
        as_of=as_of,
        min_pool=40,
        exact_top50=50,
        exact_final30=30,
    )
    
    assert recovered is not None, "Bundle recovery should succeed"
    assert recovered.is_complete(min_pool=40, exact_top50=50, exact_final30=30)
    assert len(recovered.universe_scored) == 100
    assert len(recovered.pool120) == 120
    assert len(recovered.top50) == 50
    assert len(recovered.final30) == 30


def test_recover_bundle_from_db_missing_stages(test_engine):
    """Test bundle recovery failure when stages are missing."""
    as_of = date(2026, 2, 24)
    repo = WatchlistRepo(test_engine)
    
    # Save only final30 (missing other 3 stages)
    repo.save_watchlist(
        env="test",
        strategy="pb1_watchlist_final",
        as_of=as_of,
        members=[{"code": f"00{i:04d}", "score": 95 + i} for i in range(1, 31)],
    )
    
    # Recovery should fail due to missing stages
    recovered = recover_bundle_from_db(
        engine=test_engine,
        env="test",
        as_of=as_of,
        min_pool=40,
        exact_top50=50,
        exact_final30=30,
    )
    
    assert recovered is None, "Bundle recovery should fail when intermediate stages are missing"


def test_bundle_is_complete_validation():
    """Test WatchlistBundle.is_complete() validation logic."""
    # Complete bundle
    complete = WatchlistBundle(
        as_of="2026-02-24",
        env="test",
        strategy="pb1_watchlist",
        universe_scored=[{"code": f"00{i:04d}"} for i in range(1, 101)],
        pool120=[{"code": f"00{i:04d}"} for i in range(1, 121)],
        top50=[{"code": f"00{i:04d}"} for i in range(1, 51)],
        final30=[{"code": f"00{i:04d}"} for i in range(1, 31)],
        meta={},
    )
    assert complete.is_complete(min_pool=40, exact_top50=50, exact_final30=30)
    
    # Incomplete bundle - pool120 too small
    incomplete_pool = WatchlistBundle(
        as_of="2026-02-24",
        env="test",
        strategy="pb1_watchlist",
        universe_scored=[{"code": f"00{i:04d}"} for i in range(1, 101)],
        pool120=[{"code": f"00{i:04d}"} for i in range(1, 21)],  # Only 20
        top50=[{"code": f"00{i:04d}"} for i in range(1, 51)],
        final30=[{"code": f"00{i:04d}"} for i in range(1, 31)],
        meta={},
    )
    assert not incomplete_pool.is_complete(min_pool=40, exact_top50=50, exact_final30=30)
    
    # Incomplete bundle - universe_scored empty
    incomplete_universe = WatchlistBundle(
        as_of="2026-02-24",
        env="test",
        strategy="pb1_watchlist",
        universe_scored=[],  # Empty
        pool120=[{"code": f"00{i:04d}"} for i in range(1, 121)],
        top50=[{"code": f"00{i:04d}"} for i in range(1, 51)],
        final30=[{"code": f"00{i:04d}"} for i in range(1, 31)],
        meta={},
    )
    assert not incomplete_universe.is_complete(min_pool=40, exact_top50=50, exact_final30=30)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
