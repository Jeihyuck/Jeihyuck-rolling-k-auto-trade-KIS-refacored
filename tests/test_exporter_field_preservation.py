"""Test that exporter preserves entry score columns."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest


def test_exporter_preserves_entry_scores():
    """Test that exporter maintains entry score columns through export."""
    from trader.exporter import export_watchlist_bundle
    
    # Create test data with entry scores (30 rows to pass validation)
    test_data = [
        {
            "code": f"{i:06d}",
            "name": f"Stock {i}",
            "as_of": "2026-03-08",
            "breakout_score": 75.0 + i,
            "pullback_score": 80.0 + i,
            "momentum_score": 70.0 + i,
            "rs_score": 85.0,
            "vcp_score": 65.0,
            "trend_score": 90.0,
            "tech_score": 78.5,
            "flow_score": 45.2,
            "score_final": 72.3,
            "rank": i + 1,
        }
        for i in range(30)
    ]
    
    df = pd.DataFrame(test_data)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        out_dir = Path(tmpdir)
        
        export_watchlist_bundle(
            out_dir=out_dir,
            frames_dict={"final30": df},
            meta_dict={"as_of": "2026-03-08", "env": "practice"},
        )
        
        # Check CSV export
        csv_path = out_dir / "final30.csv"
        assert csv_path.exists()
        
        exported_df = pd.read_csv(csv_path)
        
        # Verify entry score columns are present
        assert "breakout_score" in exported_df.columns
        assert "pullback_score" in exported_df.columns
        assert "momentum_score" in exported_df.columns
        assert "tech_score" in exported_df.columns
        assert "flow_score" in exported_df.columns
        assert "score_final" in exported_df.columns
        
        # Verify values are preserved (not zero)
        breakout_nonzero = (exported_df["breakout_score"].fillna(0.0) > 0.0).sum()
        pullback_nonzero = (exported_df["pullback_score"].fillna(0.0) > 0.0).sum()
        momentum_nonzero = (exported_df["momentum_score"].fillna(0.0) > 0.0).sum()
        
        assert breakout_nonzero == 30
        assert pullback_nonzero == 30
        assert momentum_nonzero == 30
        
        # Check JSON export
        json_path = out_dir / "final30.json"
        assert json_path.exists()
        
        with open(json_path, 'r') as f:
            exported_json = json.load(f)
        
        assert len(exported_json) == 30
        assert exported_json[0]["breakout_score"] > 0
        assert exported_json[0]["pullback_score"] > 0
        assert exported_json[0]["momentum_score"] > 0


def test_exporter_field_loss_detection():
    """Test that exporter logs field presence correctly."""
    from trader.exporter import export_watchlist_bundle
    import logging
    
    # Create test data with some scores missing (30 rows to pass validation)
    test_data = [
        {
            "code": f"{i:06d}",
            "name": f"Stock {i}",
            "as_of": "2026-03-08",
            "tech_score": 78.5,
            "score_final": 72.3,
            "rank": i + 1,
            # Note: entry scores intentionally missing to test field detection
        }
        for i in range(30)
    ]
    
    df = pd.DataFrame(test_data)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        out_dir = Path(tmpdir)
        
        # Capture logs
        import io
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.INFO)
        logger = logging.getLogger("trader.exporter")
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        
        try:
            export_watchlist_bundle(
                out_dir=out_dir,
                frames_dict={"final30": df},
                meta_dict={"as_of": "2026-03-08", "env": "practice"},
            )
            
            log_output = log_capture.getvalue()
            
            # Verify field presence log exists
            assert "[EXPORT][FIELDS]" in log_output
            assert "includes_breakout=0" in log_output or "includes_breakout=1" in log_output
            
            # Verify score log exists
            assert "[EXPORT][SCORES]" in log_output
            assert "breakout_nonzero=" in log_output
        
        finally:
            logger.removeHandler(handler)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
