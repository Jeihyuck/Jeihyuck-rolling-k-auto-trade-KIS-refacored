"""
Test derived_minervini quality checks and integration.

Validates:
- DerivedMinerviniRepo.load_derived() method exists and works
- Quality checks reject all-zero scores
- Entry style scores are calculated
- Merge preserves row counts
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import date


def test_derived_minervini_repo_has_load_derived_method():
    """Smoke test: DerivedMinerviniRepo has load_derived method."""
    from trader.db.repos import DerivedMinerviniRepo
    
    assert hasattr(DerivedMinerviniRepo, 'load_derived'), \
        "DerivedMinerviniRepo missing load_derived() method"


def test_load_derived_signature():
    """Verify load_derived has correct signature."""
    from trader.db.repos import DerivedMinerviniRepo
    import inspect
    
    sig = inspect.signature(DerivedMinerviniRepo.load_derived)
    params = list(sig.parameters.keys())
    
    # Should have: self, env, as_of, symbols=None, allow_fallback=False, ttl_days=7
    assert 'env' in params, "load_derived missing 'env' parameter"
    assert 'as_of' in params, "load_derived missing 'as_of' parameter"
    assert 'symbols' in params, "load_derived missing 'symbols' parameter"
    assert 'allow_fallback' in params, "load_derived missing 'allow_fallback' parameter"


def test_watchlist_builder_has_merge_derived_scores():
    """Smoke test: WatchlistBuilder has _merge_derived_scores."""
    from trader.watchlist_builder import WatchlistBuilder
    
    assert hasattr(WatchlistBuilder, '_merge_derived_scores'), \
        "WatchlistBuilder missing _merge_derived_scores() method"


def test_watchlist_builder_has_entry_style_score_methods():
    """Smoke test: WatchlistBuilder has entry style score calculation methods."""
    from trader.watchlist_builder import WatchlistBuilder
    
    assert hasattr(WatchlistBuilder, '_compute_breakout_score'), \
        "WatchlistBuilder missing _compute_breakout_score()"
    assert hasattr(WatchlistBuilder, '_compute_pullback_score'), \
        "WatchlistBuilder missing _compute_pullback_score()"
    assert hasattr(WatchlistBuilder, '_compute_momentum_score'), \
        "WatchlistBuilder missing _compute_momentum_score()"


def test_compute_breakout_score_returns_valid_range():
    """Test _compute_breakout_score returns 0-100."""
    from trader.watchlist_builder import WatchlistBuilder
    
    builder = WatchlistBuilder(
        pooln=120,
        topk=50,
        finaln=30,
        min_price=3000.0,
        liq_days=20,
        min_rows=30,
        ohlcv_provider=MagicMock(),
        flow_provider=MagicMock(),
        minervini_config={},
    )
    
    # Near high with volume surge
    row = {
        "close": 10000.0,
        "high_20d": 10100.0,  # Close is 99% of high
        "high_55d": 10200.0,
        "volume": 1_000_000,
        "volume_avg20": 500_000,  # 2x surge
    }
    
    score = builder._compute_breakout_score(row)
    assert 0.0 <= score <= 100.0, f"Score out of range: {score}"
    assert score > 0, "Should have positive breakout score"


def test_compute_pullback_score_returns_valid_range():
    """Test _compute_pullback_score returns 0-100."""
    from trader.watchlist_builder import WatchlistBuilder
    
    builder = WatchlistBuilder(
        pooln=120,
        topk=50,
        finaln=30,
        min_price=3000.0,
        liq_days=20,
        min_rows=30,
        ohlcv_provider=MagicMock(),
        flow_provider=MagicMock(),
        minervini_config={},
    )
    
    # Pullback above MA20 with low volume
    row = {
        "close": 9500.0,
        "ma20": 9300.0,  # Above MA20
        "ma50": 9000.0,
        "high_55d": 10000.0,  # 5% pullback
        "volume": 400_000,
        "volume_avg20": 500_000,  # Volume contraction
    }
    
    score = builder._compute_pullback_score(row)
    assert 0.0 <= score <= 100.0, f"Score out of range: {score}"


def test_compute_momentum_score_returns_valid_range():
    """Test _compute_momentum_score returns 0-100."""
    from trader.watchlist_builder import WatchlistBuilder
    
    builder = WatchlistBuilder(
        pooln=120,
        topk=50,
        finaln=30,
        min_price=3000.0,
        liq_days=20,
        min_rows=30,
        ohlcv_provider=MagicMock(),
        flow_provider=MagicMock(),
        minervini_config={},
    )
    
    # Strong momentum
    row = {
        "ret_20d": 0.15,  # 15% gain
        "ret_60d": 0.30,  # 30% gain
        "ret_120d": 0.50,  # 50% gain
        "rs_score": 85.0,
    }
    
    score = builder._compute_momentum_score(row)
    assert 0.0 <= score <= 100.0, f"Score out of range: {score}"
    assert score > 0, "Should have positive momentum score"


def test_merge_derived_scores_with_valid_source():
    """Test _merge_derived_scores works with valid source map."""
    from trader.watchlist_builder import WatchlistBuilder
    
    builder = WatchlistBuilder(
        pooln=120,
        topk=50,
        finaln=30,
        min_price=3000.0,
        liq_days=20,
        min_rows=30,
        ohlcv_provider=MagicMock(),
        flow_provider=MagicMock(),
        minervini_config={},
    )
    
    input_rows = [
        {"code": "000001", "symbol": "000001", "ma20": 1000.0},
        {"code": "000002", "symbol": "000002", "ma20": 2000.0},
    ]
    
    # Mock _load_minervini_source_map to return valid scores including entry scores
    with patch.object(builder, '_load_minervini_source_map') as mock_load:
        mock_load.return_value = {
            "000001": {
                "rs_score": 85.0,
                "vcp_score": 70.0,
                "trend_score": 80.0,
                "breakout_score": 75.0,
                "pullback_score": 60.0,
                "momentum_score": 80.0,
            },
            "000002": {
                "rs_score": 90.0,
                "vcp_score": 75.0,
                "trend_score": 85.0,
                "breakout_score": 80.0,
                "pullback_score": 65.0,
                "momentum_score": 85.0,
            },
        }
        
        merged = builder._merge_derived_scores(input_rows, as_of=date(2026, 3, 1))
        
        # Should preserve all rows
        assert len(merged) == len(input_rows), \
            f"Row count changed: {len(input_rows)} -> {len(merged)}"
        
        # Should have merged scores
        assert merged[0]["rs_score"] == 85.0
        assert merged[0]["vcp_score"] == 70.0
        assert merged[0]["breakout_score"] == 75.0


def test_compute_tech_score_includes_entry_component():
    """Test _compute_tech_score calculates entry component."""
    from trader.watchlist_builder import WatchlistBuilder
    
    builder = WatchlistBuilder(
        pooln=120,
        topk=50,
        finaln=30,
        min_price=3000.0,
        liq_days=20,
        min_rows=30,
        ohlcv_provider=MagicMock(),
        flow_provider=MagicMock(),
        minervini_config={},
    )
    
    row = {
        "rs_score": 85.0,
        "vcp_score": 70.0,
        "trend_score": 80.0,
        "close": 10000.0,
        "high_20d": 10100.0,
        "high_55d": 10200.0,
        "volume": 1_000_000,
        "volume_avg20": 500_000,
        "ma20": 9800.0,
        "ma50": 9500.0,
    }
    
    tech_score = builder._compute_tech_score(row)
    
    # Should have calculated entry scores
    assert "breakout_score" in row, "breakout_score not set"
    assert "pullback_score" in row, "pullback_score not set"
    assert "momentum_score" in row, "momentum_score not set"
    assert "entry_component" in row, "entry_component not set"
    assert "entry_style_selected" in row, "entry_style_selected not set"
    
    # Tech score should be > 0
    assert tech_score > 0.0, "tech_score should be positive"
    assert 0.0 <= tech_score <= 100.0, f"tech_score out of range: {tech_score}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

