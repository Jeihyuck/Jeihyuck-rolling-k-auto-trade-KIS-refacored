"""Tests for RunContext run_id field compatibility."""
from __future__ import annotations

from datetime import datetime

import pytest

from trader.run_context import RunContext


def test_run_context_with_run_id():
    """Test RunContext accepts run_id parameter without TypeError."""
    ctx = RunContext(
        account_env="practice",
        exec_mode="DIAG",
        strategy="pb1_watchlist",
        started_at=datetime.now(),
        run_id="test-run-123",
    )
    
    assert ctx.run_id == "test-run-123"
    assert ctx.account_env == "practice"
    assert ctx.exec_mode == "DIAG"


def test_run_context_without_run_id_fallback_gh_run():
    """Test RunContext auto-generates run_id from gh_run_number."""
    ctx = RunContext(
        account_env="practice",
        exec_mode="DIAG",
        strategy="pb1_watchlist",
        started_at=datetime.now(),
        gh_run_number=12345,
    )
    
    assert ctx.run_id == "12345"


def test_run_context_without_run_id_fallback_gh_and_sha():
    """Test RunContext auto-generates run_id from gh_run_number and git_sha."""
    ctx = RunContext(
        account_env="practice",
        exec_mode="DIAG",
        strategy="pb1_watchlist",
        started_at=datetime.now(),
        gh_run_number=12345,
        git_sha="abcdef1234567890",
    )
    
    assert ctx.run_id == "12345-abcdef12"


def test_run_context_without_run_id_fallback_local():
    """Test RunContext auto-generates run_id as 'local' when no inputs."""
    ctx = RunContext(
        account_env="practice",
        exec_mode="DIAG",
        strategy="pb1_watchlist",
        started_at=datetime.now(),
    )
    
    assert ctx.run_id == "local"


def test_run_context_backward_compat_new_factory():
    """Test RunContext.new() factory method still works."""
    ctx = RunContext.new(
        account_env="practice",
        exec_mode="LIVE",
        strategy="pb1_watchlist",
        gh_run_number=999,
        git_sha="test123",
    )
    
    assert ctx.account_env == "practice"
    assert ctx.exec_mode == "LIVE"
    assert ctx.run_id in ["999-test123", "999"]  # Either is acceptable


def test_run_context_no_typeerror_on_kwargs():
    """Test that passing run_id as kwarg does not raise TypeError."""
    # This is the critical test - previously this would raise:
    # TypeError: RunContext.__init__() got an unexpected keyword argument 'run_id'
    try:
        ctx = RunContext(
            account_env="practice",
            exec_mode="DIAG",
            strategy="test",
            started_at=datetime.now(),
            run_id="explicit-id",
            gh_run_number=100,
        )
        assert ctx.run_id == "explicit-id"
    except TypeError as exc:
        pytest.fail(f"RunContext raised TypeError with run_id kwarg: {exc}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
