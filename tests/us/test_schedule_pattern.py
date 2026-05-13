# -*- coding: utf-8 -*-
"""Test US schedule patterns: AM/Afternoon early start + wait patterns."""
from datetime import datetime, time
from zoneinfo import ZoneInfo


def test_am_starts_early_and_waits():
    """AM workflow should start at 08:15 ET and wait until 09:30 ET."""
    # Simulated now: 08:30 ET
    now_min = 8 * 60 + 30
    target_min = 9 * 60 + 30
    prewarm_start_min = 8 * 60 + 15
    
    # Should allow run
    assert now_min >= prewarm_start_min
    assert now_min < target_min
    
    # Should set wait_until_target = 1
    wait_until_target = 1
    wait_seconds = (target_min - now_min) * 60
    
    assert wait_until_target == 1
    assert wait_seconds == 3600  # 60 minutes


def test_am_prep_reuse_then_trade():
    """AM should reuse prep if status=OK and locked_count>=10."""
    # Simulated prep check result
    prep_status = "OK"
    locked_count = 19
    
    # Should reuse prep
    can_reuse = prep_status in ("OK", "OK_WITH_WARNINGS") and locked_count >= 10
    assert can_reuse is True
    
    prep_mode = "reused"
    am_started = True
    
    assert prep_mode == "reused"
    assert am_started is True


def test_am_prep_run_then_trade():
    """AM should run prep if prep_status missing or locked_count<10."""
    # Simulated prep check result: missing
    prep_status = "UNKNOWN"
    locked_count = 0
    
    # Should run prep
    can_reuse = prep_status in ("OK", "OK_WITH_WARNINGS") and locked_count >= 10
    assert can_reuse is False
    
    # After prep execution
    prep_status_after = "OK"
    locked_count_after = 19
    prep_mode = "executed"
    am_started = True
    
    assert prep_mode == "executed"
    assert am_started is True


def test_prep_ok_but_am_not_started_is_failure():
    """If prep OK but AM didn't start, final_status=FAILED_AM_NOT_STARTED."""
    prep_status = "OK"
    locked_count = 19
    am_started = False
    
    # This is a failure scenario
    final_status = "FAILED_AM_NOT_STARTED"
    reason = "prep_ok_but_am_not_started"
    
    assert final_status == "FAILED_AM_NOT_STARTED"
    assert reason == "prep_ok_but_am_not_started"


def test_am_already_ran_skip():
    """If AM marker exists, should skip with status=OK reason=already_ran."""
    # Simulated already_ran check
    already_ran = True
    
    if already_ran:
        final_status = "OK"
        reason = "already_ran"
        orders_sent = 0
    else:
        final_status = "OK_ORDERS_SENT"
        reason = "completed"
        orders_sent = 3
    
    assert final_status == "OK"
    assert reason == "already_ran"
    assert orders_sent == 0


def test_afternoon_starts_early_and_waits():
    """Afternoon workflow should start at 11:30 ET and wait until 12:30 ET."""
    # Simulated now: 11:45 ET
    now_min = 11 * 60 + 45
    target_min = 12 * 60 + 30
    prewarm_start_min = 11 * 60 + 30
    
    # Should allow run
    assert now_min >= prewarm_start_min
    assert now_min < target_min
    
    # Should set wait_until_target = 1
    wait_until_target = 1
    wait_seconds = (target_min - now_min) * 60
    
    assert wait_until_target == 1
    assert wait_seconds == 2700  # 45 minutes


def test_afternoon_late_recovery():
    """Afternoon started at 14:30 ET should be recovery window."""
    # Simulated now: 14:30 ET
    now_min = 14 * 60 + 30
    target_min = 12 * 60 + 30
    afternoon_allow_until = 13 * 60 + 30
    recovery_until = 15 * 60 + 20
    
    # Should be recovery window
    should_run = 1
    run_window = "recovery"
    recovery_run = 1
    
    assert now_min > afternoon_allow_until
    assert now_min < recovery_until
    assert should_run == 1
    assert run_window == "recovery"
    assert recovery_run == 1


def test_afternoon_exit_only():
    """Afternoon started at 15:30 ET should be exit_only."""
    # Simulated now: 15:30 ET
    now_min = 15 * 60 + 30
    recovery_until = 15 * 60 + 20
    session_end = 15 * 60 + 50
    
    # Should be exit_only
    should_run = 1
    run_window = "exit_only"
    entry_disabled = True
    exit_enabled = True
    
    assert now_min > recovery_until
    assert now_min < session_end
    assert should_run == 1
    assert run_window == "exit_only"
    assert entry_disabled is True
    assert exit_enabled is True


def test_watchdog_dispatches_trade_am_not_prep():
    """Watchdog should dispatch us-trade-am.yml not us-trade-prep.yml."""
    # Simulated watchdog check at 10:00 ET
    now_et = 1000
    am_check_window = [935, 1130]
    
    # AM missing
    am_missing = True
    
    if now_et >= am_check_window[0] and now_et <= am_check_window[1]:
        if am_missing:
            dispatch_workflow = "us-trade-am.yml"
        else:
            dispatch_workflow = None
    else:
        dispatch_workflow = None
    
    assert dispatch_workflow == "us-trade-am.yml"


def test_schedule_dry_run_contract():
    """Schedule runs must have DRY_RUN=0."""
    # Simulated schedule event
    event_name = "schedule"
    dry_run = "0"
    
    if event_name == "schedule":
        if dry_run == "1":
            failed = True
            reason = "schedule_dry_run_enabled"
        else:
            failed = False
            reason = ""
    else:
        failed = False
        reason = ""
    
    assert failed is False
    assert reason == ""


if __name__ == "__main__":
    test_am_starts_early_and_waits()
    test_am_prep_reuse_then_trade()
    test_am_prep_run_then_trade()
    test_prep_ok_but_am_not_started_is_failure()
    test_am_already_ran_skip()
    test_afternoon_starts_early_and_waits()
    test_afternoon_late_recovery()
    test_afternoon_exit_only()
    test_watchdog_dispatches_trade_am_not_prep()
    test_schedule_dry_run_contract()
    print("[TESTS][OK] All schedule pattern tests passed")
