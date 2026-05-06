# -*- coding: utf-8 -*-
"""US Close workflow log contract tests."""

import re


def is_close_skip_ok(log: str) -> bool:
    """Check if log contains acceptable skip patterns."""
    return bool(
        re.search(r"\[RUN_SUMMARY\]\[RESULT\] status=SKIP_PHASE_WINDOW", log)
        or re.search(r"\[US_TRADE_CLOSE\]\[EXIT\] reason=phase_guard_skip", log)
    )


def has_fatal_close_error(log: str) -> bool:
    """Check if log contains fatal close error patterns."""
    return bool(
        re.search(r"Traceback", log)
        or re.search(r"\[US_TRADE_CLOSE\]\[ERROR\].*final_status=ERROR", log)
        or re.search(r"\[US_FILLS\]\[ERROR\]\[CONTRACT\]", log)
    )


def has_close_success(log: str) -> bool:
    """Check if log contains close success patterns."""
    return bool(
        re.search(r"\[US_TRADE_CLOSE\]\[OK\]", log)
        or re.search(r"\[US_TRADE_CLOSE\]\[WARNINGS\]", log)
    )


def test_close_phase_guard_skip_success():
    """Phase guard skip should be recognized as success for close."""
    log = """
    [RUN_SUMMARY][RESULT] status=SKIP_PHASE_WINDOW reason=skip_after_close session=us-close
    [US_TRADE_CLOSE][EXIT] reason=phase_guard_skip
    """
    assert is_close_skip_ok(log) is True
    assert has_fatal_close_error(log) is False


def test_close_ok_is_success():
    """[US_TRADE_CLOSE][OK] should be success."""
    log = """
    [US_FILLS][FETCHED] count=5 status=OK schema=ALL_DATES
    [US_TRADE_CLOSE][RECONCILE] status=OK
    [US_TRADE_CLOSE][OK]
    """
    assert has_close_success(log) is True
    assert has_fatal_close_error(log) is False


def test_close_warnings_is_success():
    """[US_TRADE_CLOSE][WARNINGS] should be success with warnings."""
    log = """
    [US_FILLS][FETCHED] count=5 status=TEMP_ERROR
    [US_TRADE_CLOSE][WARNINGS] fills_status=TEMP_ERROR reconcile_status=OK
    """
    assert has_close_success(log) is True
    assert has_fatal_close_error(log) is False


def test_close_contract_error_is_fatal():
    """fills CONTRACT_ERROR should be fatal for close."""
    log = """
    [US_FILLS][ERROR][CONTRACT] all_schemas_failed
    [US_TRADE_CLOSE][ERROR] final_status=ERROR fills_status=CONTRACT_ERROR reconcile_status=OK fills_error=contract
    """
    assert has_fatal_close_error(log) is True
    assert has_close_success(log) is False


def test_close_fills_schema_retry_not_fatal():
    """Fills schema retry should not be fatal."""
    log = """
    [US_FILLS][SCHEMA_RETRY] failed_schema=ALL_DATES msg=INPUT_FIELD_NAME
    [US_FILLS][FETCHED] count=3 status=OK schema=ORD_DT
    [US_TRADE_CLOSE][OK]
    """
    assert has_fatal_close_error(log) is False
    assert has_close_success(log) is True


def test_close_temp_error_not_fatal():
    """Temporary fills error should not be fatal for close."""
    log = """
    [US_FILLS][ERROR][TEMP] schema=ALL_DATES msg=RATE_LIMIT
    [US_TRADE_CLOSE][WARNINGS] fills_status=TEMP_ERROR reconcile_status=OK
    """
    assert has_close_success(log) is True
    assert has_fatal_close_error(log) is False


def test_close_http_500_retry_not_fatal():
    """Transient HTTP 500 retry should not be fatal."""
    log = """
    [US_KIS][RETRY] attempt=1/5 error=HTTPError('500 Server Error')
    [US_FILLS][FETCHED] count=2 status=OK schema=ALL_DATES
    [US_TRADE_CLOSE][OK]
    """
    assert has_fatal_close_error(log) is False
    assert has_close_success(log) is True
