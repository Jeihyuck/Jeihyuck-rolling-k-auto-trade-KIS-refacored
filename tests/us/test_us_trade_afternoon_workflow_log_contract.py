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
