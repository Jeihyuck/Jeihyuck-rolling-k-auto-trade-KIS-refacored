# -*- coding: utf-8 -*-
"""US AM workflow log contract tests."""

import re


def is_skip_ok(log: str) -> bool:
    """Check if log contains acceptable skip patterns."""
    return bool(
        re.search(r"\[RUN_SUMMARY\]\[RESULT\] status=SKIP_PHASE_WINDOW", log)
        or re.search(r"\[US_TRADE_AM\]\[EXIT\] reason=phase_guard_skip", log)
    )


def has_fatal_am_error(log: str) -> bool:
    """Check if log contains fatal AM error patterns."""
    return bool(
        re.search(r"Traceback", log)
        or re.search(r"\[US_KIS\]\[HTTP_FAIL_FINAL\]", log)
        or re.search(r"\[US_FILLS\]\[ERROR\]\[CONTRACT\]", log)
        or re.search(r"\[US_SESSION\]\[TICK_LOOP\]\[ERROR\]", log)
        or re.search(r"\[US_SESSION\]\[END\].*reason=fills_contract_error", log)
    )


def has_success_end(log: str) -> bool:
    """Check if log contains success end patterns."""
    return bool(
        re.search(r"\[US_SESSION\]\[END\].*reason=(session_end|max_ticks|force_now_single_tick|market_skip)", log)
        or re.search(r"\[US_TRADE_AM\]\[DONE\]", log)
    )


def has_trigger_event(log: str, event_name: str) -> bool:
    return bool(re.search(rf"\[US_TRADE_AM\]\[TRIGGER\] event={event_name}\b", log))


def has_final_status(log: str, status: str, reason: str | None = None) -> bool:
    if reason:
        return bool(re.search(rf"\[US_TRADE_AM\]\[FINAL_STATUS\] status={status} .*reason={reason}\b", log))
    return bool(re.search(rf"\[US_TRADE_AM\]\[FINAL_STATUS\] status={status}\b", log))


def test_am_phase_guard_skip_success():
    """Phase guard skip should be recognized as success."""
    log = """
    [RUN_SUMMARY][RESULT] status=SKIP_PHASE_WINDOW reason=skip_stale_start_us_am session=us-am
    [US_TRADE_AM][EXIT] reason=phase_guard_skip
    """
    assert is_skip_ok(log) is True
    assert has_fatal_am_error(log) is False


def test_am_transient_retry_500_not_fatal():
    """Transient HTTP 500 retry should not be fatal."""
    log = """
    [US_KIS][RETRY] attempt=1/5 error=HTTPError('500 Server Error')
    [US_SESSION][END] session=am reason=max_ticks ticks=1
    """
    assert has_fatal_am_error(log) is False
    assert has_success_end(log) is True


def test_am_fills_contract_error_fatal():
    """Fills contract error should be fatal."""
    log = """
    [US_FILLS][ERROR][CONTRACT] field=ORD_STRT_DT msg=bad field
    """
    assert has_fatal_am_error(log) is True


def test_am_http_fail_final_fatal():
    """HTTP_FAIL_FINAL should be fatal."""
    log = """
    [US_KIS][HTTP_FAIL_FINAL] attempts=5 last_error=500
    """
    assert has_fatal_am_error(log) is True


def test_am_success_session_end():
    """Normal session end should be success."""
    log = """
    [US_SESSION][END] session=am reason=session_end ticks=10
    [US_TRADE_AM][DONE]
    """
    assert has_success_end(log) is True
    assert has_fatal_am_error(log) is False


def test_am_fills_schema_retry_not_fatal():
    """Fills schema retry should not be fatal."""
    log = """
    [US_FILLS][SCHEMA_RETRY] failed_schema=ALL_DATES msg=INPUT_FIELD_NAME
    [US_FILLS][FETCHED] count=5 status=OK schema=ORD_DT
    [US_SESSION][END] session=am reason=max_ticks ticks=1
    """
    assert has_fatal_am_error(log) is False
    assert has_success_end(log) is True


def test_am_session_end_fills_contract_error_fatal():
    """[US_SESSION][END] reason=fills_contract_error should be fatal."""
    log = """
    [US_FILLS][ERROR][CONTRACT] all_schemas_failed
    [US_SESSION][END] session=am reason=fills_contract_error tick=1
    """
    assert has_fatal_am_error(log) is True


def test_am_max_ticks_success():
    """[US_SESSION][END] reason=max_ticks should be success."""
    log = """
    [US_SESSION][END] session=am reason=max_ticks ticks=1
    [US_TRADE_AM][DONE]
    """
    assert has_success_end(log) is True
    assert has_fatal_am_error(log) is False


def test_am_manual_trigger_contract():
    log = """
    [US_TRADE_AM][TRIGGER] event=workflow_dispatch actor=tester run_id=10 attempt=1
    [US_TRADE_AM][PHASE_GUARD] should_run=1 run_window=manual_trade recovery_run=0 session_window=0930-1230
    [US_TRADE_AM][RUN_MODE] run_mode=TRADE signal_only=0 order_allowed=1 kis_order_allowed=1
    [US_TRADE_AM][MIGRATION][OK]
    [US_TRADE_AM][TRADE_RUNNER][START]
    """
    assert has_trigger_event(log, "workflow_dispatch") is True
    assert "run_window=manual_trade" in log
    assert "[US_TRADE_AM][TRADE_RUNNER][START]" in log


def test_am_schedule_trigger_contract():
    log = """
    [US_TRADE_AM][TRIGGER] event=schedule actor=github-actions run_id=11 attempt=2
    [US_TRADE_AM][START_META] schedule_expected=0815 actual_start=081531 delay_seconds=31 event=schedule
    [US_TRADE_AM][PHASE_GUARD] should_run=1 run_window=scheduled_trade recovery_run=0 session_window=0930-1230
    [US_SESSION_LOCK][ACQUIRE] trade_date=2026-06-05 session=am env=practice status=OK
    """
    assert has_trigger_event(log, "schedule") is True
    assert "schedule_expected=0815" in log
    assert "run_window=scheduled_trade" in log


def test_am_migration_failure_and_pnl_do_not_mix():
    log = """
    [US_TRADE_AM][MIGRATION][FAIL] version=0043_us_fills_idempotency_and_order_reconcile_fix.sql reason=duplicate_us_fills
    [US_TRADE_AM][TRADE_RUNNER][SKIP] reason=db_migration_failed
    [US_TRADE_AM][FINAL_STATUS] status=FAILED trade_runner_started=0 orders_sent=0 reason=db_migration_failed
    [US_PNL][FINAL_STATUS] status=OK_WITH_WARNINGS source=db_snapshot
    [US_WORKFLOW][FINAL_STATUS] status=FAILED_AM_TRADE_PNL_ONLY
    """
    assert has_final_status(log, "FAILED", "db_migration_failed") is True
    assert "[US_WORKFLOW][FINAL_STATUS] status=FAILED_AM_TRADE_PNL_ONLY" in log
