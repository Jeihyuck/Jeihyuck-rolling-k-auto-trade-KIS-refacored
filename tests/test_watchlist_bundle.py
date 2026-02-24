"""Tests for WatchlistBundle data structure and validation."""
from __future__ import annotations

from datetime import date

import pytest

from trader.watchlist_builder import WatchlistBundle


def test_watchlist_bundle_complete():
    """Test WatchlistBundle.is_complete() with valid data."""
    bundle = WatchlistBundle(
        as_of="2026-02-24",
        env="practice",
        strategy="pb1_watchlist",
        universe_scored=[{"code": f"{i:06d}", "score": i} for i in range(1, 201)],
        pool120=[{"code": f"{i:06d}", "score": i} for i in range(1, 121)],
        top50=[{"code": f"{i:06d}", "score": i} for i in range(1, 51)],
        final30=[{"code": f"{i:06d}", "score": i} for i in range(1, 31)],
        meta={"source": "test"},
    )
    
    assert bundle.is_complete() is True


def test_watchlist_bundle_incomplete_universe():
    """Test bundle fails validation when universe_scored is empty."""
    bundle = WatchlistBundle(
        as_of="2026-02-24",
        env="practice",
        strategy="pb1_watchlist",
        universe_scored=[],  # Empty!
        pool120=[{"code": f"{i:06d}"} for i in range(1, 121)],
        top50=[{"code": f"{i:06d}"} for i in range(1, 51)],
        final30=[{"code": f"{i:06d}"} for i in range(1, 31)],
        meta={},
    )
    
    assert bundle.is_complete() is False


def test_watchlist_bundle_incomplete_pool120():
    """Test bundle fails validation when pool120 < 40."""
    bundle = WatchlistBundle(
        as_of="2026-02-24",
        env="practice",
        strategy="pb1_watchlist",
        universe_scored=[{"code": f"{i:06d}"} for i in range(1, 201)],
        pool120=[{"code": f"{i:06d}"} for i in range(1, 21)],  # Only 20!
        top50=[{"code": f"{i:06d}"} for i in range(1, 51)],
        final30=[{"code": f"{i:06d}"} for i in range(1, 31)],
        meta={},
    )
    
    assert bundle.is_complete() is False


def test_watchlist_bundle_incomplete_top50():
    """Test bundle fails validation when top50 < 50."""
    bundle = WatchlistBundle(
        as_of="2026-02-24",
        env="practice",
        strategy="pb1_watchlist",
        universe_scored=[{"code": f"{i:06d}"} for i in range(1, 201)],
        pool120=[{"code": f"{i:06d}"} for i in range(1, 121)],
        top50=[{"code": f"{i:06d}"} for i in range(1, 40)],  # Only 39!
        final30=[{"code": f"{i:06d}"} for i in range(1, 31)],
        meta={},
    )
    
    assert bundle.is_complete() is False


def test_watchlist_bundle_incomplete_final30():
    """Test bundle fails validation when final30 < 30."""
    bundle = WatchlistBundle(
        as_of="2026-02-24",
        env="practice",
        strategy="pb1_watchlist",
        universe_scored=[{"code": f"{i:06d}"} for i in range(1, 201)],
        pool120=[{"code": f"{i:06d}"} for i in range(1, 121)],
        top50=[{"code": f"{i:06d}"} for i in range(1, 51)],
        final30=[{"code": f"{i:06d}"} for i in range(1, 20)],  # Only 19!
        meta={},
    )
    
    assert bundle.is_complete() is False


def test_watchlist_bundle_to_dict():
    """Test WatchlistBundle.to_dict() conversion."""
    bundle = WatchlistBundle(
        as_of="2026-02-24",
        env="practice",
        strategy="pb1_watchlist",
        universe_scored=[{"code": "000001"}],
        pool120=[{"code": "000001"}] * 120,
        top50=[{"code": "000001"}] * 50,
        final30=[{"code": "000001"}] * 30,
        meta={"test_key": "test_value"},
    )
    
    result = bundle.to_dict()
    
    assert result["as_of"] == "2026-02-24"
    assert result["env"] == "practice"
    assert result["strategy"] == "pb1_watchlist"
    assert len(result["universe_scored"]) == 1
    assert len(result["pool120"]) == 120
    assert len(result["top50"]) == 50
    assert len(result["final30"]) == 30
    assert result["test_key"] == "test_value"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
