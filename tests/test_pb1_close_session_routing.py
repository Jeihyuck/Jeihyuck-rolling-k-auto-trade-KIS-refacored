"""
Test close session routing and normalization.

Critical requirements:
- PB1_SESSION_KIND=close must never become "afternoon"
- FORCE_MARKET_WINDOW=close must stay "close"
- PB1_FORCE_TRADE_SESSION=close must stay "close"
- _resolve_session_end_dt() must use PB1_CLOSE_SESSION_END for close sessions
- close phase must use TRADE_CLOSE log prefix, never TRADE_AM or TRADE_PM
"""

import os
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
import pytest

# Import after setting up env to avoid side effects
def test_close_session_never_becomes_afternoon():
    """Verify close session is never converted to afternoon."""
    from trader.pb1_runner import normalize_session_kind, _resolve_session_kind
    
    # Direct normalization
    assert normalize_session_kind("close") == "close"
    assert normalize_session_kind("trade-close") == "close"
    
    # Via _resolve_session_kind with explicit env
    os.environ["PB1_SESSION_KIND"] = "close"
    os.environ.pop("FORCE_MARKET_WINDOW", None)
    os.environ.pop("PB1_FORCE_TRADE_SESSION", None)
    
    result = _resolve_session_kind()
    assert result == "close", f"Expected 'close' but got '{result}'"


def test_close_via_force_market_window():
    """Verify FORCE_MARKET_WINDOW=close results in close session."""
    from trader.pb1_runner import _resolve_session_kind
    
    os.environ["PB1_SESSION_KIND"] = "afternoon"
    os.environ["FORCE_MARKET_WINDOW"] = "close"
    os.environ.pop("PB1_FORCE_TRADE_SESSION", None)
    
    result = _resolve_session_kind()
    assert result == "close", f"FORCE_MARKET_WINDOW=close should override, got '{result}'"


def test_close_via_force_trade_session():
    """Verify PB1_FORCE_TRADE_SESSION=close results in close session."""
    from trader.pb1_runner import _resolve_session_kind
    
    os.environ["PB1_SESSION_KIND"] = "afternoon"
    os.environ.pop("FORCE_MARKET_WINDOW", None)
    os.environ["PB1_FORCE_TRADE_SESSION"] = "close"
    
    result = _resolve_session_kind()
    assert result == "close", f"PB1_FORCE_TRADE_SESSION=close should result in close, got '{result}'"


def test_close_session_end_uses_correct_env():
    """Verify _resolve_session_end_dt uses PB1_CLOSE_SESSION_END for close sessions."""
    from trader.pb1_runner import _resolve_session_end_dt, _resolve_session_kind
    
    KST = ZoneInfo("Asia/Seoul")
    now = datetime(2026, 5, 1, 15, 20, 0, tzinfo=KST)
    
    os.environ["PB1_SESSION_KIND"] = "close"
    os.environ["PB1_CLOSE_SESSION_END"] = "15:30"
    os.environ["PB1_PM_SESSION_END"] = "15:10"
    os.environ.pop("FORCE_MARKET_WINDOW", None)
    
    session_kind = _resolve_session_kind()
    assert session_kind == "close"
    
    session_end_dt = _resolve_session_end_dt(now)
    assert session_end_dt.hour == 15
    assert session_end_dt.minute == 30, f"Expected minute=30 for close session, got {session_end_dt.minute}"


def test_pm_session_does_not_use_close_end():
    """Verify PM session uses PM_SESSION_END, not CLOSE_SESSION_END."""
    from trader.pb1_runner import _resolve_session_end_dt, _resolve_session_kind
    
    KST = ZoneInfo("Asia/Seoul")
    now = datetime(2026, 5, 1, 13, 30, 0, tzinfo=KST)
    
    # Clean env to ensure no cross-test pollution
    os.environ.pop("FORCE_MARKET_WINDOW", None)
    os.environ.pop("PB1_FORCE_TRADE_SESSION", None)
    
    os.environ["PB1_SESSION_KIND"] = "afternoon"
    os.environ["PB1_PM_SESSION_END"] = "15:10"
    os.environ["PB1_CLOSE_SESSION_END"] = "15:30"
    os.environ["PB1_AM_SESSION_END"] = "13:00"
    
    session_kind = _resolve_session_kind(now)
    assert session_kind == "afternoon", f"Expected 'afternoon' but got '{session_kind}'"
    
    session_end_dt = _resolve_session_end_dt(now)
    assert session_end_dt.hour == 15
    assert session_end_dt.minute == 10,  f"Expected minute=10 for PM session, got {session_end_dt.minute}"


def test_normalize_window_preserves_close():
    """Verify normalize_window keeps close as close."""
    from trader.pb1_runner import normalize_window
    
    result = normalize_window(session_kind="close", input_window="close")
    assert result == "close", f"normalize_window should preserve close, got '{result}'"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
