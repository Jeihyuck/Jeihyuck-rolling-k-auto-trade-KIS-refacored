"""
Test log verification logic.

Critical requirements:
- OK, OK_NO_TRADE, OK_WITH_WARNINGS are success
- Recoverable timeout with SESSION_END_OK is success
- FATAL_RUNTIME is failure
- Close NO_TICK in normal window is failure
- result_status=UNKNOWN is failure
"""

import pytest


def test_log_verification_accepts_ok_statuses():
    """Verify OK variants are treated as success."""
    from trader.pb1_runner import evaluate_workflow_log_success
    
    log_ok = """
[PB1][LOOP][SESSION] kind=am now=2026-05-01T09:00:00 session_end=2026-05-01T12:55:00 interval=60
[PB1][TICK][DONE] tick=1
[PB1][SESSION_TERMINAL] terminal_state=SESSION_END_OK result_status=OK exit_reason=session_end
[RUN_SUMMARY][RESULT] status=OK reason=session_end
[PB1][EXIT] reason=session_end
"""
    
    result = evaluate_workflow_log_success(session="am", log_text=log_ok)
    assert result["ok"] is True, f"OK status should pass: {result}"
    assert result["status"] in {"OK", "OK_NO_TRADE", "OK_WITH_WARNINGS"}


def test_log_verification_accepts_ok_with_warnings():
    """Verify OK_WITH_WARNINGS is treated as success."""
    from trader.pb1_runner import evaluate_workflow_log_success
    
    log_warn = """
[PB1][LOOP][SESSION] kind=am
[PB1][TICK][DONE] tick=1
[PB1][TICK_TIMEOUT][RECOVERABLE] kind=am tick=2 last_stage=entry timeout_sec=90
[PB1][SESSION_TERMINAL] terminal_state=SESSION_END_OK_WITH_WARNINGS result_status=OK_WITH_WARNINGS exit_reason=session_end
[RUN_SUMMARY][RESULT] status=OK_WITH_WARNINGS reason=session_end
[PB1][EXIT] reason=session_end
"""
    
    result = evaluate_workflow_log_success(session="am", log_text=log_warn)
    assert result["ok"] is True, f"OK_WITH_WARNINGS should pass: {result}"


def test_log_verification_rejects_fatal_runtime():
    """Verify FATAL_RUNTIME is treated as failure."""
    from trader.pb1_runner import evaluate_workflow_log_success
    
    log_fatal = """
[PB1][LOOP][SESSION] kind=am
[PB1][FATAL_GUARD] unexpected error
Traceback (most recent call last):
[RUN_SUMMARY][RESULT] status=FATAL_RUNTIME reason=unexpected_error
[PB1][EXIT] reason=fatal_runtime
"""
    
    result = evaluate_workflow_log_success(session="am", log_text=log_fatal)
    assert result["ok"] is False, f"FATAL_RUNTIME should fail: {result}"
    assert result["reason"] == "fatal_detected"


def test_log_verification_counts_timeouts():
    """Verify timeout counts are tracked."""
    from trader.pb1_runner import evaluate_workflow_log_success
    
    log_timeouts = """
[PB1][LOOP][SESSION] kind=am now=2026-05-01T09:00:00
[PB1][TICK][CALL_RUN_ONCE] kind=am now=2026-05-01T09:00:00 session_end=2026-05-01T12:55:00
TickTimeoutError: tick_hard_timeout timeout_sec=90 last_stage=entry
[PB1][TICK_TIMEOUT][RECOVERABLE] kind=am tick=1 last_stage=entry timeout_sec=90
[PB1][TICK][CALL_RUN_ONCE] kind=am now=2026-05-01T09:05:00 session_end=2026-05-01T12:55:00
TickTimeoutError: tick_hard_timeout timeout_sec=90 last_stage=exit
[PB1][TICK_TIMEOUT][RECOVERABLE] kind=am tick=2 last_stage=exit timeout_sec=90
[PB1][SESSION_TERMINAL] terminal_state=SESSION_END_OK_WITH_WARNINGS result_status=OK_WITH_WARNINGS exit_reason=session_end
[RUN_SUMMARY][RESULT] status=OK_WITH_WARNINGS reason=session_end
[PB1][EXIT] reason=session_end
"""
    
    result = evaluate_workflow_log_success(session="am", log_text=log_timeouts)
    assert result["ok"] is True, "Session with recoverable timeouts should still be OK"
    assert result["timeout_count"] >= 2, f"Should detect 2+ timeouts, got {result['timeout_count']}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
