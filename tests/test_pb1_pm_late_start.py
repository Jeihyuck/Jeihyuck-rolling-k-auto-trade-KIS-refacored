"""
Test PM late start and stale skip logic.

Critical requirements:
- PM session starting at 13:31 (after START_ALLOW_UNTIL=13:30) should still run if before PM_SESSION_END=15:10
- Stale skip should only trigger if now >= PM_SESSION_END, not just > START_ALLOW_UNTIL
- Late start should log warning but not hard skip
- Close session starting at 15:15 should run even if after normal PM window
"""

import os
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
import pytest


def test_pm_late_start_before_session_end_runs():
    """PM started at 13:31 (after START_ALLOW_UNTIL) should still run if before SESSION_END."""
    from trader.trade_tick import _apply_prewarm_guard
    
    os.environ["PB1_PREWARM_ENABLED"] = "1"
    os.environ["PB1_SESSION_KIND"] = "afternoon"
    os.environ["PB1_TARGET_START_TIME"] = "13:00"
    os.environ["PB1_START_ALLOW_UNTIL"] = "13:30"
    os.environ["PB1_PM_SESSION_END"] = "15:10"
    
    KST = ZoneInfo("Asia/Seoul")
    # Thursday 13:31 - late start but before session end
    now = datetime(2026, 5, 1, 13, 31, 0, tzinfo=KST)  # Thursday
    
    result = _apply_prewarm_guard(now_override=now)
    # Should return None (proceed) or set warning flag, not 0 (skip)
    assert result is None, f"PM at 13:31 should run with late warning, got {result}"
    assert os.environ.get("PB1_LATE_START_WARNING") == "1", "Should set late start warning flag"


def test_pm_after_session_end_skips():
    """PM started at 15:11 (after PM_SESSION_END=15:10) should skip."""
    from trader.trade_tick import _apply_prewarm_guard
    
    os.environ["PB1_PREWARM_ENABLED"] = "1"
    os.environ["PB1_SESSION_KIND"] = "afternoon"
    os.environ["PB1_TARGET_START_TIME"] = "13:00"
    os.environ["PB1_START_ALLOW_UNTIL"] = "13:30"
    os.environ["PB1_PM_SESSION_END"] = "15:10"
    
    KST = ZoneInfo("Asia/Seoul")
    # Thursday 15:11 - after session end
    now = datetime(2026, 5, 1, 15, 11, 0, tzinfo=KST)
    
    result = _apply_prewarm_guard(now_override=now)
    assert result == 0, f"PM at 15:11 (after session_end) should skip, got {result}"


def test_close_at_1515_runs():
    """Close session at 15:15 should run (session_end=15:30)."""
    from trader.trade_tick import _apply_prewarm_guard
    
    os.environ["PB1_PREWARM_ENABLED"] = "1"
    os.environ["PB1_SESSION_KIND"] = "close"
    os.environ["PB1_TARGET_START_TIME"] = "15:15"
    os.environ["PB1_START_ALLOW_UNTIL"] = "15:35"
    os.environ["PB1_CLOSE_SESSION_END"] = "15:30"
    
    KST = ZoneInfo("Asia/Seoul")
    # Thursday 15:15 - normal close start
    now = datetime(2026, 5, 1, 15, 15, 0, tzinfo=KST)
    
    result = _apply_prewarm_guard(now_override=now)
    assert result is None, f"Close at 15:15 should run, got {result}"


def test_close_at_1520_runs():
    """Close session at 15:20 (before session_end=15:30) should run with warning."""
    from trader.trade_tick import _apply_prewarm_guard
    
    os.environ["PB1_PREWARM_ENABLED"] = "1"
    os.environ["PB1_SESSION_KIND"] = "close"
    os.environ["PB1_TARGET_START_TIME"] = "15:15"
    os.environ["PB1_START_ALLOW_UNTIL"] = "15:25"
    os.environ["PB1_CLOSE_SESSION_END"] = "15:30"
    
    KST = ZoneInfo("Asia/Seoul")
    # Thursday 15:20 - past target, before session end
    now = datetime(2026, 5, 1, 15, 20, 0, tzinfo=KST)
    
    result = _apply_prewarm_guard(now_override=now)
    assert result is None, f"Close at 15:20 (before session_end) should run, got {result}"


def test_close_at_1531_skips():
    """Close session at 15:31 (after session_end=15:30) should skip."""
    from trader.trade_tick import _apply_prewarm_guard
    
    os.environ["PB1_PREWARM_ENABLED"] = "1"
    os.environ["PB1_SESSION_KIND"] = "close"
    os.environ["PB1_TARGET_START_TIME"] = "15:15"
    os.environ["PB1_START_ALLOW_UNTIL"] = "15:35"
    os.environ["PB1_CLOSE_SESSION_END"] = "15:30"
    
    KST = ZoneInfo("Asia/Seoul")
    # Thursday 15:31 - after session end
    now = datetime(2026, 5, 1, 15, 31, 0, tzinfo=KST)
    
    result = _apply_prewarm_guard(now_override=now)
    assert result == 0, f"Close at 15:31 (after session_end) should skip, got {result}"


def test_weekend_skip():
    """Weekend should skip (non-trading day)."""
    from trader.trade_tick import _apply_prewarm_guard
    
    os.environ["PB1_PREWARM_ENABLED"] = "1"
    os.environ["PB1_SESSION_KIND"] = "afternoon"
    os.environ["PB1_TARGET_START_TIME"] = "13:00"
    os.environ["PB1_START_ALLOW_UNTIL"] = "13:30"
    os.environ["PB1_COMPUTE_ONLY"] = "0"
    
    KST = ZoneInfo("Asia/Seoul")
    # Saturday
    now = datetime(2026, 5, 2, 13, 0, 0, tzinfo=KST)
    
    result = _apply_prewarm_guard(now_override=now)
    assert result == 0, f"Saturday should skip, got {result}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
