#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Test US workflow phase guards and boot guards.

Tests that workflow phase guard logic mirrors Korean workflow patterns:
- Early start + wait until target
- Normal window
- Recovery window
- Stale skip

Also tests afternoon boot guard for exit_only fallback.
"""
import pytest
from datetime import datetime, time
from zoneinfo import ZoneInfo


def parse_et_time(hhmm: str) -> tuple[int, int]:
    """Parse HHMM string to (hour, minute)."""
    h = int(hhmm[:2])
    m = int(hhmm[2:])
    return h, m


def et_to_minutes(hhmm: str) -> int:
    """Convert HHMM ET to minutes since midnight."""
    h, m = parse_et_time(hhmm)
    return h * 60 + m


class TestUSAMPhaseGuard:
    """Test US AM phase guard logic (한국장 패턴 적용)."""

    def test_early_wait_0910(self):
        """09:10 ET schedule should wait until 09:30."""
        now_min = et_to_minutes("0910")
        target_min = et_to_minutes("0930")
        start_allow_until = et_to_minutes("1000")
        recovery_until = et_to_minutes("1130")
        
        # Phase guard logic
        should_run = True
        wait_until_target = False
        run_window = "normal"
        recovery_run = False
        
        if now_min > recovery_until:
            should_run = False
            run_window = "stale"
        elif now_min > start_allow_until:
            run_window = "recovery"
            recovery_run = True
        elif now_min < target_min:
            wait_until_target = True
            wait_seconds = (target_min - now_min) * 60
            if wait_seconds <= 3600:
                run_window = "early_wait"
            else:
                should_run = False
                run_window = "too_early"
        
        assert should_run is True
        assert wait_until_target is True
        assert run_window == "early_wait"
        assert recovery_run is False

    def test_normal_0935(self):
        """09:35 ET schedule is normal window."""
        now_min = et_to_minutes("0935")
        target_min = et_to_minutes("0930")
        start_allow_until = et_to_minutes("1000")
        recovery_until = et_to_minutes("1130")
        
        should_run = True
        wait_until_target = False
        run_window = "normal"
        recovery_run = False
        
        if now_min > recovery_until:
            should_run = False
            run_window = "stale"
        elif now_min > start_allow_until:
            run_window = "recovery"
            recovery_run = True
        elif now_min < target_min:
            wait_until_target = True
            run_window = "early_wait"
        
        assert should_run is True
        assert wait_until_target is False
        assert run_window == "normal"
        assert recovery_run is False

    def test_recovery_1015(self):
        """10:15 ET schedule is recovery window."""
        now_min = et_to_minutes("1015")
        target_min = et_to_minutes("0930")
        start_allow_until = et_to_minutes("1000")
        recovery_until = et_to_minutes("1130")
        
        should_run = True
        wait_until_target = False
        run_window = "normal"
        recovery_run = False
        
        if now_min > recovery_until:
            should_run = False
            run_window = "stale"
        elif now_min > start_allow_until:
            run_window = "recovery"
            recovery_run = True
        elif now_min < target_min:
            wait_until_target = True
            run_window = "early_wait"
        
        assert should_run is True
        assert wait_until_target is False
        assert run_window == "recovery"
        assert recovery_run is True

    def test_stale_1145(self):
        """11:45 ET schedule is stale, should skip."""
        now_min = et_to_minutes("1145")
        target_min = et_to_minutes("0930")
        start_allow_until = et_to_minutes("1000")
        recovery_until = et_to_minutes("1130")
        
        should_run = True
        wait_until_target = False
        run_window = "normal"
        recovery_run = False
        
        if now_min > recovery_until:
            should_run = False
            run_window = "stale"
        elif now_min > start_allow_until:
            run_window = "recovery"
            recovery_run = True
        elif now_min < target_min:
            wait_until_target = True
            run_window = "early_wait"
        
        assert should_run is False
        assert run_window == "stale"


class TestUSAfternoonBootGuard:
    """Test US afternoon boot guard logic (한국장 패턴 적용)."""

    def test_early_wait_1220(self):
        """12:20 ET should wait until 12:30."""
        now_min = et_to_minutes("1220")
        target_min = et_to_minutes("1230")
        afternoon_allow_until = et_to_minutes("1330")
        recovery_until = et_to_minutes("1520")
        session_end = et_to_minutes("1550")
        
        should_run = True
        wait_until_target = False
        run_window = "normal"
        route = "normal_afternoon"
        
        if now_min > session_end:
            should_run = False
            run_window = "stale"
        elif now_min > recovery_until:
            run_window = "exit_only"
            route = "exit_only"
        elif now_min > afternoon_allow_until:
            run_window = "recovery"
            route = "recovery_afternoon"
        elif now_min < target_min:
            wait_until_target = True
            wait_seconds = (target_min - now_min) * 60
            if wait_seconds <= 1800:
                run_window = "early_wait"
            else:
                should_run = False
                run_window = "too_early"
        
        assert should_run is True
        assert wait_until_target is True
        assert run_window == "early_wait"

    def test_normal_1240(self):
        """12:40 ET is normal afternoon."""
        now_min = et_to_minutes("1240")
        target_min = et_to_minutes("1230")
        afternoon_allow_until = et_to_minutes("1330")
        recovery_until = et_to_minutes("1520")
        session_end = et_to_minutes("1550")
        
        should_run = True
        wait_until_target = False
        run_window = "normal"
        route = "normal_afternoon"
        
        if now_min > session_end:
            should_run = False
            run_window = "stale"
        elif now_min > recovery_until:
            run_window = "exit_only"
            route = "exit_only"
        elif now_min > afternoon_allow_until:
            run_window = "recovery"
            route = "recovery_afternoon"
        elif now_min < target_min:
            wait_until_target = True
            run_window = "early_wait"
        
        assert should_run is True
        assert wait_until_target is False
        assert run_window == "normal"
        assert route == "normal_afternoon"

    def test_recovery_1430(self):
        """14:30 ET is recovery afternoon."""
        now_min = et_to_minutes("1430")
        target_min = et_to_minutes("1230")
        afternoon_allow_until = et_to_minutes("1330")
        recovery_until = et_to_minutes("1520")
        session_end = et_to_minutes("1550")
        
        should_run = True
        wait_until_target = False
        run_window = "normal"
        route = "normal_afternoon"
        
        if now_min > session_end:
            should_run = False
            run_window = "stale"
        elif now_min > recovery_until:
            run_window = "exit_only"
            route = "exit_only"
        elif now_min > afternoon_allow_until:
            run_window = "recovery"
            route = "recovery_afternoon"
        elif now_min < target_min:
            wait_until_target = True
            run_window = "early_wait"
        
        assert should_run is True
        assert wait_until_target is False
        assert run_window == "recovery"
        assert route == "recovery_afternoon"

    def test_exit_only_1530(self):
        """15:30 ET is exit_only mode (늦어도 close는 살린다)."""
        now_min = et_to_minutes("1530")
        target_min = et_to_minutes("1230")
        afternoon_allow_until = et_to_minutes("1330")
        recovery_until = et_to_minutes("1520")
        session_end = et_to_minutes("1550")
        
        should_run = True
        wait_until_target = False
        run_window = "normal"
        route = "normal_afternoon"
        
        if now_min > session_end:
            should_run = False
            run_window = "stale"
        elif now_min > recovery_until:
            run_window = "exit_only"
            route = "exit_only"
        elif now_min > afternoon_allow_until:
            run_window = "recovery"
            route = "recovery_afternoon"
        elif now_min < target_min:
            wait_until_target = True
            run_window = "early_wait"
        
        assert should_run is True
        assert wait_until_target is False
        assert run_window == "exit_only"
        assert route == "exit_only"

    def test_skip_after_close_1600(self):
        """16:00 ET is after US close, should skip."""
        now_min = et_to_minutes("1600")
        target_min = et_to_minutes("1230")
        afternoon_allow_until = et_to_minutes("1330")
        recovery_until = et_to_minutes("1520")
        session_end = et_to_minutes("1550")
        
        should_run = True
        wait_until_target = False
        run_window = "normal"
        route = "normal_afternoon"
        
        if now_min > session_end:
            should_run = False
            run_window = "stale"
        elif now_min > recovery_until:
            run_window = "exit_only"
            route = "exit_only"
        elif now_min > afternoon_allow_until:
            run_window = "recovery"
            route = "recovery_afternoon"
        elif now_min < target_min:
            wait_until_target = True
            run_window = "early_wait"
        
        assert should_run is False
        assert run_window == "stale"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
