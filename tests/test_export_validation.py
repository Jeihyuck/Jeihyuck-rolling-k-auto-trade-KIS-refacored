"""Tests for export validation and 0-row detection."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from trader.exporter import export_watchlist_bundle


def test_export_validation_empty_universe_scored():
    """Test export validation detects empty universe_scored."""
    with tempfile.TemporaryDirectory() as tmpdir:
        out_dir = Path(tmpdir)
        frames = {
            "universe_scored": pd.DataFrame(),  # Empty!
            "pool120": pd.DataFrame([{"code": f"{i:06d}"} for i in range(120)]),
            "top50": pd.DataFrame([{"code": f"{i:06d}"} for i in range(50)]),
            "final30": pd.DataFrame([{"code": f"{i:06d}"} for i in range(30)]),
        }
        meta = {}
        
        result = export_watchlist_bundle(out_dir=out_dir, frames_dict=frames, meta_dict=meta)
        
        # Export should still happen but with validation failures in meta
        assert (out_dir / "meta.json").exists()
        with (out_dir / "meta.json").open() as f:
            saved_meta = json.load(f)
        
        assert "export_validation_failures" in saved_meta
        assert any("universe_scored:empty" in f for f in saved_meta["export_validation_failures"])


def test_export_validation_pool120_too_small():
    """Test export validation detects pool120 < 40."""
    with tempfile.TemporaryDirectory() as tmpdir:
        out_dir = Path(tmpdir)
        frames = {
            "universe_scored": pd.DataFrame([{"code": f"{i:06d}"} for i in range(100)]),
            "pool120": pd.DataFrame([{"code": f"{i:06d}"} for i in range(20)]),  # Only 20!
            "top50": pd.DataFrame([{"code": f"{i:06d}"} for i in range(50)]),
            "final30": pd.DataFrame([{"code": f"{i:06d}"} for i in range(30)]),
        }
        meta = {}
        
        result = export_watchlist_bundle(out_dir=out_dir, frames_dict=frames, meta_dict=meta)
        
        with (out_dir / "meta.json").open() as f:
            saved_meta = json.load(f)
        
        assert "export_validation_failures" in saved_meta
        failures = saved_meta["export_validation_failures"]
        assert any("pool120:too_small" in f for f in failures)


def test_export_validation_top50_too_small():
    """Test export validation detects top50 < 50."""
    with tempfile.TemporaryDirectory() as tmpdir:
        out_dir = Path(tmpdir)
        frames = {
            "universe_scored": pd.DataFrame([{"code": f"{i:06d}"} for i in range(100)]),
            "pool120": pd.DataFrame([{"code": f"{i:06d}"} for i in range(120)]),
            "top50": pd.DataFrame([{"code": f"{i:06d}"} for i in range(30)]),  # Only 30!
            "final30": pd.DataFrame([{"code": f"{i:06d}"} for i in range(30)]),
        }
        meta = {}
        
        result = export_watchlist_bundle(out_dir=out_dir, frames_dict=frames, meta_dict=meta)
        
        with (out_dir / "meta.json").open() as f:
            saved_meta = json.load(f)
        
        assert "export_validation_failures" in saved_meta
        failures = saved_meta["export_validation_failures"]
        assert any("top50:too_small" in f for f in failures)


def test_export_validation_final30_too_small():
    """Test export validation detects final30 < 30."""
    with tempfile.TemporaryDirectory() as tmpdir:
        out_dir = Path(tmpdir)
        frames = {
            "universe_scored": pd.DataFrame([{"code": f"{i:06d}"} for i in range(100)]),
            "pool120": pd.DataFrame([{"code": f"{i:06d}"} for i in range(120)]),
            "top50": pd.DataFrame([{"code": f"{i:06d}"} for i in range(50)]),
            "final30": pd.DataFrame([{"code": f"{i:06d}"} for i in range(15)]),  # Only 15!
        }
        meta = {}
        
        result = export_watchlist_bundle(out_dir=out_dir, frames_dict=frames, meta_dict=meta)
        
        with (out_dir / "meta.json").open() as f:
            saved_meta = json.load(f)
        
        assert "export_validation_failures" in saved_meta
        failures = saved_meta["export_validation_failures"]
        assert any("final30:too_small" in f for f in failures)


def test_export_validation_all_valid():
    """Test export validation passes with all valid data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        out_dir = Path(tmpdir)
        frames = {
            "universe_scored": pd.DataFrame([{"code": f"{i:06d}", "score": i} for i in range(200)]),
            "pool120": pd.DataFrame([{"code": f"{i:06d}", "score": i} for i in range(120)]),
            "top50": pd.DataFrame([{"code": f"{i:06d}", "score": i} for i in range(50)]),
            "final30": pd.DataFrame([{"code": f"{i:06d}", "score": i} for i in range(30)]),
        }
        meta = {"test": "meta"}
        
        result = export_watchlist_bundle(out_dir=out_dir, frames_dict=frames, meta_dict=meta)
        
        # All files should be created
        assert (out_dir / "universe_scored.csv").exists()
        assert (out_dir / "pool120.csv").exists()
        assert (out_dir / "top50.csv").exists()
        assert (out_dir / "final30.csv").exists()
        assert (out_dir / "meta.json").exists()
        
        # Meta should NOT have validation failures
        with (out_dir / "meta.json").open() as f:
            saved_meta = json.load(f)
        
        assert "export_validation_failures" not in saved_meta or len(saved_meta.get("export_validation_failures", [])) == 0


def test_export_csv_rows_match():
    """Test exported CSV row counts match input DataFrames."""
    with tempfile.TemporaryDirectory() as tmpdir:
        out_dir = Path(tmpdir)
        frames = {
            "universe_scored": pd.DataFrame([{"code": f"{i:06d}"} for i in range(196)]),
            "pool120": pd.DataFrame([{"code": f"{i:06d}"} for i in range(120)]),
            "top50": pd.DataFrame([{"code": f"{i:06d}"} for i in range(50)]),
            "final30": pd.DataFrame([{"code": f"{i:06d}"} for i in range(30)]),
        }
        meta = {}
        
        result = export_watchlist_bundle(out_dir=out_dir, frames_dict=frames, meta_dict=meta)
        
        # Read back and verify row counts
        universe_csv = pd.read_csv(out_dir / "universe_scored.csv")
        pool120_csv = pd.read_csv(out_dir / "pool120.csv")
        top50_csv = pd.read_csv(out_dir / "top50.csv")
        final30_csv = pd.read_csv(out_dir / "final30.csv")
        
        assert len(universe_csv) == 196
        assert len(pool120_csv) == 120
        assert len(top50_csv) == 50
        assert len(final30_csv) == 30


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
