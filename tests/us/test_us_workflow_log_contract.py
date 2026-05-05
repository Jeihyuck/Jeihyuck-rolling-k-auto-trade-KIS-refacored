# -*- coding: utf-8 -*-
"""US Trade Prep log contract tests.

Transient KIS retry must not fail US Trade Prep.
Only final failures are fatal.
"""

import re


def has_fatal_us_prep_log_pattern(log_text: str) -> bool:
    """Check if log contains fatal error patterns."""
    fatal_patterns = [
        r"Traceback",
        r"\[US_KIS\]\[HTTP_FAIL_FINAL\]",
        r"\[US_PREP\]\[ERROR\]",
        r"status=ERROR",
    ]
    return any(re.search(pattern, log_text) for pattern in fatal_patterns)


def test_transient_kis_retry_500_is_not_fatal():
    """Transient KIS retry with HTTP 500 should not fail prep."""
    log = """
    [US_KIS][RETRY] attempt=1/5 path='/uapi/overseas-price/v1/quotations/dailyprice'
    error=HTTPError('500 Server Error: Internal Server Error')
    [US_STRATEGY][SCORED] strategy=us_momentum symbol=SMH score=0.403
    [US_PREP][FINISH] status=OK watchlist=41 unique_success=22/22
    """
    assert has_fatal_us_prep_log_pattern(log) is False


def test_kis_http_fail_final_is_fatal():
    """Final KIS HTTP failure after all retries should be fatal."""
    log = """
    [US_KIS][HTTP_FAIL_FINAL] attempt=5/5 path='/uapi/overseas-price/v1/quotations/dailyprice'
    error=HTTPError('500 Server Error: Internal Server Error') temporary=True
    """
    assert has_fatal_us_prep_log_pattern(log) is True


def test_prep_error_marker_is_fatal():
    """[US_PREP][ERROR] marker should be fatal."""
    log = """
    [US_PREP][ERROR] universe load failed: ConnectionError
    """
    assert has_fatal_us_prep_log_pattern(log) is True


def test_prep_status_error_is_fatal():
    """prep_status=ERROR should be fatal."""
    log = """
    [US_PREP][FINISH] status=ERROR watchlist=0 unique_success=0/22
    """
    assert has_fatal_us_prep_log_pattern(log) is True


def test_prep_ok_with_warnings_is_not_fatal():
    """OK_WITH_WARNINGS status should not be fatal."""
    log = """
    [US_PREP][STATUS] OK_WITH_WARNINGS total=22 unique_success=18 event_success=35
    [US_PREP][FINISH] status=OK_WITH_WARNINGS watchlist=35 unique_success=18/22
    """
    assert has_fatal_us_prep_log_pattern(log) is False
