# -*- coding: utf-8 -*-
"""US Afternoon workflow log contract tests."""

import re


def is_skip_ok(log: str) -> bool:
    """Check if log contains acceptable skip patterns."""
    return bool(
        re.search(r"\[RUN_SUMMARY\]\[RESULT\] status=SKIP_PHASE_WINDOW", log)
        or re.search(r"\[US_TRADE_AFTERNOON\]\[EXIT\] reason=phase_guard_skip", log)
    )


def has_fatal_afternoon_error(log: str) -> bool:
    """Check if log contains fatal afternoon error patterns."""
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
        or re.search(r"\[US_TRADE_AFTERNOON\]\[DONE\]", log)
    )


def has_trade_runner_start(log: str) -> bool:
    return bool(re.search(r"\[US_TRADE_AFTERNOON\]\[TRADE_RUNNER\]\[START\] session=afternoon", log))


def test_afternoon_phase_guard_skip_success():
    """Phase guard skip should be recognized as success."""
    log = """
    [RUN_SUMMARY][RESULT] status=SKIP_PHASE_WINDOW reason=skip_stale_start_us_afternoon session=us-afternoon
    [US_TRADE_AFTERNOON][EXIT] reason=phase_guard_skip
    """
    assert is_skip_ok(log) is True
    assert has_fatal_afternoon_error(log) is False


def test_afternoon_transient_retry_500_not_fatal():
    """Transient HTTP 500 retry should not be fatal."""
    log = """
    [US_KIS][RETRY] attempt=1/5 error=HTTPError('500 Server Error')
    [US_SESSION][END] session=afternoon reason=max_ticks ticks=1
    """
    assert has_fatal_afternoon_error(log) is False
    assert has_success_end(log) is True


def test_afternoon_fills_contract_error_fatal():
    """Fills contract error should be fatal."""
    log = """
    [US_FILLS][ERROR][CONTRACT] field=ORD_STRT_DT msg=bad field
    """
    assert has_fatal_afternoon_error(log) is True


def test_afternoon_http_fail_final_fatal():
    """HTTP_FAIL_FINAL should be fatal."""
    log = """
    [US_KIS][HTTP_FAIL_FINAL] attempts=5 last_error=500
    """
    assert has_fatal_afternoon_error(log) is True


def test_afternoon_success_session_end():
    """Normal session end should be success."""
    log = """
    [US_SESSION][END] session=afternoon reason=session_end ticks=10
    [US_TRADE_AFTERNOON][DONE]
    """
    assert has_success_end(log) is True
    assert has_fatal_afternoon_error(log) is False


def test_afternoon_fills_schema_retry_not_fatal():
    """Fills schema retry should not be fatal."""
    log = """
    [US_FILLS][SCHEMA_RETRY] failed_schema=ALL_DATES msg=INPUT_FIELD_NAME ORD_DT
    [US_FILLS][FETCHED] count=3 status=OK schema=ORD_RANGE
    [US_SESSION][END] session=afternoon reason=max_ticks ticks=1
    """
    assert has_fatal_afternoon_error(log) is False
    assert has_success_end(log) is True


def test_afternoon_session_end_fills_contract_error_fatal():
    """[US_SESSION][END] reason=fills_contract_error should be fatal."""
    log = """
    [US_FILLS][ERROR][CONTRACT] all_schemas_failed
    [US_SESSION][END] session=afternoon reason=fills_contract_error tick=1
    """
    assert has_fatal_afternoon_error(log) is True


def test_afternoon_max_ticks_success():
    """[US_SESSION][END] reason=max_ticks should be success."""
    log = """
    [US_SESSION][END] session=afternoon reason=max_ticks ticks=1
    [US_TRADE_AFTERNOON][DONE]
    """
    assert has_success_end(log) is True
    assert has_fatal_afternoon_error(log) is False


def test_afternoon_manual_actual_trade_contract():
    log = """
    [US_TRADE_AFTERNOON][EXPECTATION] expected_to_trade=1 event=workflow_dispatch phase_should_run=1 run_mode=TRADE order_allowed=1 kis_order_allowed=1 dry_run=false offline_mode=false signal_only=0 smoke_loop=false force_now_set=0 reason=trade_allowed
    [US_TRADE_AFTERNOON][TRADE_RUNNER][START] session=afternoon
    [US_TRADE_AFTERNOON][DONE]
    """
    assert has_trade_runner_start(log) is True
    assert has_fatal_afternoon_error(log) is False


def test_afternoon_schedule_early_wait_contract():
    log = """
    [US_TRADE_AFTERNOON][PHASE_GUARD] should_run=1 run_window=early_wait route=normal_afternoon recovery_run=0 wait_seconds=1800
    [US_WAIT_UNTIL_TARGET][START] session=afternoon wait_seconds=1800
    [US_WAIT_UNTIL_TARGET][DONE] session=afternoon now_et=123000
    [US_TRADE_AFTERNOON][EXPECTATION] expected_to_trade=1 event=schedule phase_should_run=1 run_mode=TRADE order_allowed=1 kis_order_allowed=1 dry_run=false offline_mode=false signal_only=0 smoke_loop=false force_now_set=0 reason=trade_allowed
    [US_TRADE_AFTERNOON][TRADE_RUNNER][START] session=afternoon
    """
    assert "run_window=early_wait" in log
    assert has_trade_runner_start(log) is True


def test_afternoon_already_ran_after_wait_not_failed():
    log = """
    [US_TRADE_AFTERNOON][FINAL_DUPLICATE_GUARD][RESULT] trade_date=2026-06-06 already_ran=True guard_status=DONE reason=already_ran_after_wait
    [US_TRADE_AFTERNOON][EXPECTATION] expected_to_trade=0 event=schedule phase_should_run=1 run_mode=TRADE order_allowed=1 kis_order_allowed=1 dry_run=false offline_mode=false signal_only=0 smoke_loop=false force_now_set=0 reason=already_ran_after_wait
    [US_WORKFLOW][FINAL_STATUS] status=SKIPPED_DUPLICATE_SESSION trade_status=SKIPPED_DUPLICATE_SESSION expected_to_trade=0 trade_runner_started=0 reason=already_ran_after_wait
    """
    assert "SKIPPED_DUPLICATE_SESSION" in log
    assert "FAILED_TRADE_NOT_STARTED" not in log


def test_afternoon_expected_to_trade_requires_runner_start():
    log = """
    [US_TRADE_AFTERNOON][EXPECTATION] expected_to_trade=1 event=workflow_dispatch phase_should_run=1 run_mode=TRADE order_allowed=1 kis_order_allowed=1 dry_run=false offline_mode=false signal_only=0 smoke_loop=false force_now_set=0 reason=trade_allowed
    [US_WORKFLOW][FINAL_STATUS] status=FAILED_TRADE_NOT_STARTED trade_status=READY expected_to_trade=1 trade_runner_started=0 reason=trade_pipeline_gate_failed
    """
    assert "FAILED_TRADE_NOT_STARTED" in log
    assert "status=SKIPPED" not in log
