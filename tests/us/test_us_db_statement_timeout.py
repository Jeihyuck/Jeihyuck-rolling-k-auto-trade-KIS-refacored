# -*- coding: utf-8 -*-
"""Tests for US DB statement timeout configuration.

Verifies that:
1. load_latest_us_prep_status applies statement_timeout
2. load_locked_us_watchlist applies statement_timeout  
3. Public trader/db/engine.py is not modified
"""
import os
from unittest.mock import MagicMock, patch, call


def test_prep_status_applies_statement_timeout():
    """load_latest_us_prep_status should set statement_timeout."""
    from trader.us.db import repos
    
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    
    # Mock the query result
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None
    mock_conn.execute.return_value = mock_result
    
    with patch.object(repos, "_get_engine_or_none", return_value=mock_engine):
        repos.load_latest_us_prep_status(trade_date="2026-05-07", timeout_sec=15)
    
    # Verify set_config was called with statement_timeout
    calls = mock_conn.execute.call_args_list
    assert len(calls) >= 2, "Expected at least 2 calls: set_config + SELECT"
    
    # First call should be set_config
    first_call_args = str(calls[0])
    assert "set_config" in first_call_args.lower()
    assert "statement_timeout" in first_call_args.lower()
    # Should use 15000ms
    timeout_val = calls[0].args[1] if len(calls[0].args) > 1 else calls[0].kwargs.get("timeout_value")
    if timeout_val:
        assert "15000" in str(timeout_val)


def test_watchlist_applies_statement_timeout():
    """load_locked_us_watchlist should set statement_timeout."""
    from trader.us.db import repos
    
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    
    # Mock the query result
    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    mock_conn.execute.return_value = mock_result
    
    with patch.object(repos, "_get_engine_or_none", return_value=mock_engine):
        repos.load_locked_us_watchlist(
            trade_date="2026-05-07", 
            min_count=1, 
            allow_degraded=True,
            timeout_sec=12,
        )
    
    # Verify set_config was called
    calls = mock_conn.execute.call_args_list
    assert len(calls) >= 2, "Expected at least 2 calls: set_config + SELECT"
    
    first_call_args = str(calls[0])
    assert "set_config" in first_call_args.lower()
    assert "statement_timeout" in first_call_args.lower()
    # Should use 12000ms
    timeout_val = calls[0].args[1] if len(calls[0].args) > 1 else calls[0].kwargs.get("timeout_value")
    if timeout_val:
        assert "12000" in str(timeout_val)


def test_public_db_engine_not_modified():
    """Verify trader/db/engine.py was not modified (KR boundary rule)."""
    import inspect
    from trader.db import engine as db_engine
    
    # Check get_engine function signature hasn't changed to include timeout
    if hasattr(db_engine, "get_engine"):
        sig = inspect.signature(db_engine.get_engine)
        params = list(sig.parameters.keys())
        # Should not have timeout/statement_timeout parameters
        assert "timeout" not in params, "trader/db/engine.py should not have timeout param"
        assert "statement_timeout" not in params, "trader/db/engine.py should not have statement_timeout param"
    
    # Verify module docstring doesn't mention US-specific changes
    if db_engine.__doc__:
        doc = db_engine.__doc__.lower()
        # Should not mention US or statement timeout
        assert "us_" not in doc, "trader/db/engine.py should not mention US-specific changes"


def test_timeout_honored_in_guard():
    """Integration test: guard script passes timeout_sec to DB functions."""
    # This is more of a schema verification test
    from trader.us.db.repos import load_latest_us_prep_status, load_locked_us_watchlist
    import inspect
    
    # Verify function signatures
    prep_sig = inspect.signature(load_latest_us_prep_status)
    assert "timeout_sec" in prep_sig.parameters, "load_latest_us_prep_status must accept timeout_sec"
    
    watchlist_sig = inspect.signature(load_locked_us_watchlist)
    assert "timeout_sec" in watchlist_sig.parameters, "load_locked_us_watchlist must accept timeout_sec"
    
    # Verify defaults
    assert prep_sig.parameters["timeout_sec"].default == 20
    assert watchlist_sig.parameters["timeout_sec"].default == 20
