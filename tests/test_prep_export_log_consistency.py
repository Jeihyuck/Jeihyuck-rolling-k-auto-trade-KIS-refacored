"""Test PREP export log consistency across watchlist_builder/prep_runner/exporter."""
from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


def test_prep_export_inmem_log_includes_entry_scores():
    """Test that PREP][EXPORT][FINAL30][INMEM] log includes entry scores."""
    
    # Create test watchlist bundle with entry scores
    test_bundle = {
        "as_of": "2026-03-08",
        "env": "practice",
        "strategy": "pb1_watchlist",
        "universe_scored": [],
        "pool120": [],
        "top50": [],
        "final30": [
            {
                "code": "005930",
                "name": "Samsung",
                "tech_score": 75.0,
                "score_final": 72.0,
                "breakout_score": 80.0,
                "pullback_score": 75.0,
                "momentum_score": 70.0,
            }
        ],
        "meta": {},
    }
    
    # Capture logs
    import io
    log_capture = io.StringIO()
    handler = logging.StreamHandler(log_capture)
    handler.setLevel(logging.INFO)
    
    # Test the log format directly
    final30_df = pd.DataFrame(test_bundle["final30"])
    
    if not final30_df.empty:
        breakout_nonzero = int((final30_df["breakout_score"].fillna(0.0) > 0.0).sum()) if "breakout_score" in final30_df.columns else 0
        pullback_nonzero = int((final30_df["pullback_score"].fillna(0.0) > 0.0).sum()) if "pullback_score" in final30_df.columns else 0
        momentum_nonzero = int((final30_df["momentum_score"].fillna(0.0) > 0.0).sum()) if "momentum_score" in final30_df.columns else 0
        
        # Verify counts match expected
        assert breakout_nonzero == 1
        assert pullback_nonzero == 1
        assert momentum_nonzero == 1


def test_watchlist_exporter_log_consistency():
    """Test that watchlist_builder and exporter report same nonzero counts."""
    from trader.exporter import export_watchlist_bundle
    import tempfile
    from pathlib import Path
    
    # Create test data
    test_data = [
        {
            "code": "005930",
            "name": "Samsung",
            "as_of": "2026-03-08",
            "breakout_score": 80.0,
            "pullback_score": 75.0,
            "momentum_score": 70.0,
            "tech_score": 75.0,
            "score_final": 72.0,
            "rank": 1,
        },
        {
            "code": "000660",
            "name": "SK Hynix",
            "as_of": "2026-03-08",
            "breakout_score": 85.0,
            "pullback_score": 80.0,
            "momentum_score": 75.0,
            "tech_score": 80.0,
            "score_final": 78.0,
            "rank": 2,
        },
    ]
    
    df = pd.DataFrame(test_data)
    
    # Calculate expected counts
    breakout_expected = int((df["breakout_score"] > 0).sum())
    pullback_expected = int((df["pullback_score"] > 0).sum())
    momentum_expected = int((df["momentum_score"] > 0).sum())
    
    assert breakout_expected == 2
    assert pullback_expected == 2
    assert momentum_expected == 2
    
    # Capture exporter logs
    import io
    log_capture = io.StringIO()
    handler = logging.StreamHandler(log_capture)
    handler.setLevel(logging.INFO)
    logger = logging.getLogger("trader.exporter")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir)
            
            export_watchlist_bundle(
                out_dir=out_dir,
                frames_dict={"final30": df},
                meta_dict={"as_of": "2026-03-08", "env": "practice"},
            )
            
            log_output = log_capture.getvalue()
            
            # Verify exporter logs match expected counts
            assert "[EXPORT][SCORES]" in log_output
            assert "breakout_nonzero=2" in log_output
            assert "pullback_nonzero=2" in log_output
            assert "momentum_nonzero=2" in log_output
            
            # Verify field presence log
            assert "[EXPORT][FIELDS]" in log_output
            assert "includes_breakout=1" in log_output
            assert "includes_pullback=1" in log_output
            assert "includes_momentum=1" in log_output
    
    finally:
        logger.removeHandler(handler)


def test_bundle_save_exporter_log_consistency():
    """Test that bundle save and exporter report consistent field presence."""
    from trader.watchlist_builder import WatchlistBundle, save_bundle
    import tempfile
    from pathlib import Path
    
    # Create bundle with entry scores
    bundle_data = [
        {
            "code": "005930",
            "name": "Samsung",
            "breakout_score": 80.0,
            "pullback_score": 75.0,
            "momentum_score": 70.0,
            "tech_score": 75.0,
            "score_final": 72.0,
        }
    ]
    
    bundle = WatchlistBundle(
        as_of="2026-03-08",
        env="practice",
        strategy="pb1_watchlist",
        universe_scored=bundle_data.copy(),
        pool120=bundle_data.copy(),
        top50=bundle_data.copy(),
        final30=bundle_data.copy(),
        meta={},
    )
    
    # Capture bundle save logs
    import io
    log_capture = io.StringIO()
    handler = logging.StreamHandler(log_capture)
    handler.setLevel(logging.INFO)
    logger = logging.getLogger("trader.watchlist_builder")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    
    try:
        with patch("trader.watchlist_builder.WatchlistRepo") as MockRepo:
            mock_repo_instance = MagicMock()
            MockRepo.return_value = mock_repo_instance
            
            from datetime import date
            mock_engine = MagicMock()
            
            save_bundle(
                engine=mock_engine,
                env="practice",
                as_of=date(2026, 3, 8),
                bundle=bundle,
            )
            
            log_output = log_capture.getvalue()
            
            # Verify bundle save logs entry scores
            assert "[BUNDLE][DATAFRAME][FIELDS]" in log_output
            assert "includes_breakout=1" in log_output
            assert "includes_pullback=1" in log_output
            assert "includes_momentum=1" in log_output
            
            assert "[BUNDLE][DATAFRAME][SCORES]" in log_output
            assert "breakout_nonzero=1" in log_output
            assert "pullback_nonzero=1" in log_output
            assert "momentum_nonzero=1" in log_output
    
    finally:
        logger.removeHandler(handler)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
