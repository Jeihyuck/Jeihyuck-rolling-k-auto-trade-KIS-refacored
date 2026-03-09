"""Test that bundle save logs clearly indicate universe_scored source."""
from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest


def test_bundle_save_logs_universe_source():
    """Test that save_bundle logs pb1_universe_scored source clearly."""
    from trader.watchlist_builder import WatchlistBundle, save_bundle
    
    # Create mock bundle with candidate_pool-based universe_scored
    bundle = WatchlistBundle(
        as_of="2026-03-08",
        env="practice",
        strategy="pb1_watchlist",
        universe_scored=[{"code": f"{i:06d}", "name": f"Stock{i}"} for i in range(120)],
        pool120=[{"code": f"{i:06d}", "name": f"Stock{i}"} for i in range(120)],
        top50=[{"code": f"{i:06d}", "name": f"Stock{i}"} for i in range(50)],
        final30=[{"code": f"{i:06d}", "name": f"Stock{i}"} for i in range(30)],
        meta={},
    )
    
    # Mock engine and repo
    with patch("trader.watchlist_builder.WatchlistRepo") as MockRepo:
        mock_repo_instance = MagicMock()
        MockRepo.return_value = mock_repo_instance
        
        # Capture logs
        import io
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.INFO)
        logger = logging.getLogger("trader.watchlist_builder")
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        
        try:
            from datetime import date
            mock_engine = MagicMock()
            
            save_bundle(
                engine=mock_engine,
                env="practice",
                as_of=date(2026, 3, 8),
                bundle=bundle,
            )
            
            log_output = log_capture.getvalue()
            
            # Verify source logging
            assert "[WATCHLIST][SAVE_SCOPE]" in log_output
            assert "pb1_universe_scored_source=universe_filtered" in log_output
            assert "rows=120" in log_output
            
            # Verify bundle DataFrame field logs
            assert "[BUNDLE][DATAFRAME][FIELDS]" in log_output
            assert "[BUNDLE][DATAFRAME][SCORES]" in log_output
            
        finally:
            logger.removeHandler(handler)


def test_build_watchlist_logs_upstream_universe():
    """Test that build_and_save_watchlist logs upstream universe count."""
    from trader.watchlist_builder import build_and_save_watchlist
    from datetime import date
    
    # Create test members (simulating 196 upstream universe)
    upstream_members = [
        {"code": f"{i:06d}", "name": f"Stock{i}"}
        for i in range(196)
    ]
    
    # Mock dependencies
    with (
        patch("trader.watchlist_builder.WatchlistRepo") as MockRepo,
        patch("trader.candidate_pool_builder.load_candidate_pool") as mock_load_pool,
    ):
        # Mock candidate pool to return 120 items
        mock_load_pool.return_value = (
            [f"{i:06d}" for i in range(120)],
            date(2026, 3, 8),
            "cached",
        )
        
        mock_repo_instance = MagicMock()
        mock_repo_instance.load_watchlist.return_value = ([], None)
        MockRepo.return_value = mock_repo_instance
        
        # Mock ohlcv_provider
        mock_ohlcv = MagicMock()
        mock_ohlcv.return_value = (None, {})
        
        # Capture logs
        import io
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.INFO)
        logger = logging.getLogger("trader.watchlist_builder")
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        
        try:
            mock_engine = MagicMock()
            
            # Note: This will fail due to missing dependencies, but we're testing logging
            try:
                build_and_save_watchlist(
                    engine=mock_engine,
                    env="practice",
                    strategy="pb1_watchlist",
                    as_of=date(2026, 3, 8),
                    members=upstream_members,
                    ohlcv_provider=mock_ohlcv,
                    minervini_config={},
                    force_rebuild=True,
                    use_cache=False,
                )
            except Exception:
                # Expected to fail - we're only testing logging
                pass
            
            log_output = log_capture.getvalue()
            
            # Verify upstream universe logging
            assert "[WATCHLIST][UNIVERSE][UPSTREAM]" in log_output
            assert "count=196" in log_output
            
            # Verify candidate pool source logging
            assert "upstream_universe=196" in log_output or "source=candidate_pool" in log_output
            
        finally:
            logger.removeHandler(handler)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
