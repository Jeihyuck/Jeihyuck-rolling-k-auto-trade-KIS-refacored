from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from trader.pb1_runner import evaluate_workflow_log_success


def test_timeout_warning_but_session_end_success() -> None:
    log_text = """
[RUN_SUMMARY][RESULT] status=OK reason=session_completed session=am event=schedule
[WARN][PB1][TICK_TIMEOUT] kind=am now=2024-01-02T10:01:00+09:00 session_end=2024-01-02T13:00:00+09:00 timeout_sec=90 err=tick_hard_timeout
[PB1][LOOP][SESSION_END_RELEASE] kind=am now=2024-01-02T13:00:00+09:00 session_end=2024-01-02T13:00:00+09:00 result_status=OK
[PB1][SESSION_TERMINAL] terminal_state=SESSION_END_OK_WITH_WARNINGS result_status=OK_WITH_WARNINGS exit_reason=session_end warnings_total=1
"""
    result = evaluate_workflow_log_success(session="am", log_text=log_text)
    assert result["ok"] is True
    assert result["status"] == "OK_WITH_WARNINGS"


def test_db_autocommit_warning_but_session_end_success() -> None:
    log_text = """
[RUN_SUMMARY][RESULT] status=OK reason=session_completed session=pm event=schedule
can't change 'autocommit' now
[PB1][EXIT] reason=session_end
[PB1][SESSION_TERMINAL] terminal_state=SESSION_END_OK_WITH_WARNINGS result_status=OK_WITH_WARNINGS exit_reason=session_end warnings_total=1
"""
    result = evaluate_workflow_log_success(session="pm", log_text=log_text)
    assert result["ok"] is True
    assert result["status"] == "OK_WITH_WARNINGS"


def test_fatal_runtime_fails_workflow() -> None:
    log_text = """
[RUN_SUMMARY][RESULT] status=FATAL_RUNTIME reason=UNHANDLED_RUNTIME_EXCEPTION session=pm event=schedule
[PB1][EXIT] reason=fatal_runtime
Traceback (most recent call last):
"""
    result = evaluate_workflow_log_success(session="pm", log_text=log_text)
    assert result["ok"] is False
    assert result["status"] == "FATAL_RUNTIME"


def test_duplicate_skip_is_policy_failure() -> None:
    log_text = """
[RUN_SUMMARY][RESULT] status=SKIP_PHASE_WINDOW reason=phase_guard_skip_duplicate_pm_run session=pm event=schedule
[PB1][EXIT] reason=phase_guard_skip
"""
    result = evaluate_workflow_log_success(session="pm", log_text=log_text)
    assert result["ok"] is False
    assert result["reason"] == "phase_guard_skip_duplicate_pm_run"