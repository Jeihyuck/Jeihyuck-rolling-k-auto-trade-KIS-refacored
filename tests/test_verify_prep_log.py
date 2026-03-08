"""Test verify_prep_log.py script functionality."""
from __future__ import annotations

import tempfile
from pathlib import Path

import pytest


def test_verify_prep_done_success():
    """Test that PREP_DONE event is detected."""
    log_content = """
[PREP][START] as_of=2026-03-08
[PREP][DERIVED][MINERVINI] upserted=196
[DERIVED][LOAD] rows=196
event_type=PREP_DONE
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        f.write(log_content)
        log_path = Path(f.name)
    
    try:
        from scripts.verify_prep_log import parse_log_file
        
        results = parse_log_file(log_path)
        assert results.prep_done is True
        assert results.derived_count == 196
    finally:
        log_path.unlink()


def test_verify_contract_recovery_success():
    """Test that contract failure + recovery is treated as success."""
    log_content = """
[PREP][START] as_of=2026-03-08
event_type=PREP_DONE
[PREP][DERIVED][MINERVINI] upserted=120
contract_universe_too_small:120<150
[PREP][WATCHLIST][RECOVERY][DB_SUCCESS]
[PREP][WATCHLIST][FINAL][SAVE] n=30
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        f.write(log_content)
        log_path = Path(f.name)
    
    try:
        from scripts.verify_prep_log import parse_log_file
        
        results = parse_log_file(log_path)
        assert results.prep_done is True
        assert "contract_universe_too_small" in results.contract_failures
        assert "DB_SUCCESS" in results.contract_recoveries
        assert not results.has_critical_failure()
    finally:
        log_path.unlink()


def test_verify_contract_unrecovered_failure():
    """Test that unrecovered contract failure is detected."""
    log_content = """
[PREP][START] as_of=2026-03-08
event_type=PREP_DONE
[PREP][DERIVED][MINERVINI] upserted=120
contract_universe_too_small:120<150
[PREP][WATCHLIST][FINAL][SAVE] n=30
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        f.write(log_content)
        log_path = Path(f.name)
    
    try:
        from scripts.verify_prep_log import parse_log_file
        
        results = parse_log_file(log_path)
        assert results.prep_done is True
        assert "contract_universe_too_small" in results.contract_failures
        assert len(results.contract_recoveries) == 0
        assert results.has_critical_failure()
    finally:
        log_path.unlink()


def test_verify_entry_scores_present():
    """Test that entry score detection works."""
    log_content = """
[PREP][START] as_of=2026-03-08
event_type=PREP_DONE
[PREP][DERIVED][MINERVINI] upserted=196
breakout_nonzero=30
pullback_nonzero=30
momentum_nonzero=30
[PREP][WATCHLIST][FINAL][SAVE] n=30
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        f.write(log_content)
        log_path = Path(f.name)
    
    try:
        from scripts.verify_prep_log import parse_log_file
        
        results = parse_log_file(log_path)
        assert results.breakout_nonzero == 30
        assert results.pullback_nonzero == 30
        assert results.momentum_nonzero == 30
        assert not results.has_warnings()
    finally:
        log_path.unlink()


def test_verify_derived_count_patterns():
    """Test that derived count is extracted from multiple log patterns."""
    # Test pattern 1: [PREP][DERIVED][MINERVINI] upserted=196
    log1 = """
[PREP][DERIVED][MINERVINI] upserted=196
event_type=PREP_DONE
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        f.write(log1)
        log_path = Path(f.name)
    
    try:
        from scripts.verify_prep_log import parse_log_file
        
        results = parse_log_file(log_path)
        assert results.derived_count == 196
    finally:
        log_path.unlink()
    
    # Test pattern 2: [DERIVED][LOAD] rows=196
    log2 = """
[DERIVED][LOAD] rows=196
event_type=PREP_DONE
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        f.write(log2)
        log_path = Path(f.name)
    
    try:
        from scripts.verify_prep_log import parse_log_file
        
        results = parse_log_file(log_path)
        assert results.derived_count == 196
    finally:
        log_path.unlink()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
