"""
Test PB1 exit stage signature fixes.

This test ensures that:
1. reconcile_today has the correct signature with RunContext
2. close_stale_positions is properly imported and callable
3. Both functions exist and are not NameError
"""

from __future__ import annotations

import inspect
from datetime import datetime

import pytest

from trader.reconcile_kis import reconcile_today
from trader.reconcile_db import close_stale_positions
from trader.run_context import RunContext


def test_reconcile_today_signature():
    """Verify reconcile_today accepts ctx: RunContext parameter."""
    sig = inspect.signature(reconcile_today)
    params = sig.parameters
    
    # Must have keyword-only parameters
    assert "engine" in params
    assert "kis" in params
    assert "ctx" in params
    
    # ctx should be annotated as RunContext
    ctx_param = params["ctx"]
    assert ctx_param.annotation == RunContext or "RunContext" in str(ctx_param.annotation)
    
    # Should NOT have env, run_id, strategy as separate parameters
    assert "env" not in params
    assert "run_id" not in params
    assert "strategy" not in params


def test_close_stale_positions_signature():
    """Verify close_stale_positions has the expected signature."""
    sig = inspect.signature(close_stale_positions)
    params = sig.parameters
    
    # Required parameters
    assert "engine" in params
    assert "env" in params
    assert "strategy" in params
    assert "reason" in params
    assert "ts" in params
    
    # ts should be datetime
    ts_param = params["ts"]
    assert ts_param.annotation == datetime or "datetime" in str(ts_param.annotation)


def test_reconcile_today_exists_and_callable():
    """Verify reconcile_today exists and is callable (not NameError)."""
    assert callable(reconcile_today)
    assert reconcile_today.__name__ == "reconcile_today"


def test_close_stale_positions_exists_and_callable():
    """Verify close_stale_positions exists and is callable (not NameError)."""
    assert callable(close_stale_positions)
    assert close_stale_positions.__name__ == "close_stale_positions"


def test_pb1_runner_imports():
    """Verify pb1_runner can import both functions without NameError."""
    # This will fail if imports are missing
    from trader.pb1_runner import reconcile_today as rt_imported
    from trader.pb1_runner import close_stale_positions as csp_imported
    
    assert callable(rt_imported)
    assert callable(csp_imported)


def test_run_context_construction():
    """Verify RunContext can be constructed with required fields."""
    ctx = RunContext(
        run_id="test-run",
        env="practice",
        strategy="test_strategy",
        started_at=datetime.now(),
        dry_run=False,
    )
    
    assert ctx.run_id == "test-run"
    assert ctx.env == "practice"
    assert ctx.strategy == "test_strategy"
    assert not ctx.dry_run
