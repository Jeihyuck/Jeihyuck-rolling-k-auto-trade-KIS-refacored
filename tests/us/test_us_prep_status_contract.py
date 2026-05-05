# -*- coding: utf-8 -*-
"""US prep status contract tests.

US prep status must be based on locked watchlist sufficiency, data failures,
critical ETF failures, and fatal errors rather than raw universe signal ratio.
"""

from trader.us.runner.prep_runner import _determine_prep_status


def test_prep_status_ok_when_locked_watchlist_sufficient_even_if_not_all_universe_signaled():
    """Prep status should be OK when locked watchlist is sufficient, regardless of universe size."""
    status = _determine_prep_status(
        total_symbols=22,
        locked_unique_count=19,
        data_failed_symbols=set(),
        critical_etf_failures=set(),
        has_fatal_error=False,
        min_locked_unique=10,
        target_locked_unique=15,
    )
    assert status == "OK"


def test_prep_status_ok_with_warnings_when_data_failed_but_locked_sufficient():
    """Prep status should be OK_WITH_WARNINGS when data failed but locked watchlist is sufficient."""
    status = _determine_prep_status(
        total_symbols=22,
        locked_unique_count=19,
        data_failed_symbols={"META"},
        critical_etf_failures=set(),
        has_fatal_error=False,
        min_locked_unique=10,
        target_locked_unique=15,
        warn_on_data_failure=True,
    )
    assert status == "OK_WITH_WARNINGS"


def test_prep_status_ok_when_data_failed_but_warn_disabled():
    """Prep status should be OK when data failure warning is disabled."""
    status = _determine_prep_status(
        total_symbols=22,
        locked_unique_count=19,
        data_failed_symbols={"META"},
        critical_etf_failures=set(),
        has_fatal_error=False,
        min_locked_unique=10,
        target_locked_unique=15,
        warn_on_data_failure=False,
    )
    assert status == "OK"


def test_prep_status_ok_with_warnings_when_partial_success():
    """Prep status should be OK_WITH_WARNINGS when 70-90% success."""
    status = _determine_prep_status(
        total_symbols=22,
        locked_unique_count=13,  # Between min and target
        data_failed_symbols=set(),
        critical_etf_failures=set(),
        has_fatal_error=False,
        min_locked_unique=10,
        target_locked_unique=15,
    )
    assert status == "OK_WITH_WARNINGS"


def test_prep_status_degraded_when_locked_unique_below_min():
    """Prep status should be DEGRADED when locked unique count is below minimum."""
    status = _determine_prep_status(
        total_symbols=22,
        locked_unique_count=5,
        data_failed_symbols=set(),
        critical_etf_failures=set(),
        has_fatal_error=False,
        min_locked_unique=10,
        target_locked_unique=15,
    )
    assert status == "DEGRADED"


def test_prep_status_degraded_when_critical_etf_failed():
    """Prep status should be DEGRADED when critical ETF failed."""
    status = _determine_prep_status(
        total_symbols=22,
        locked_unique_count=19,
        data_failed_symbols=set(),
        critical_etf_failures={"SPY"},
        has_fatal_error=False,
        min_locked_unique=10,
        target_locked_unique=15,
    )
    assert status == "DEGRADED"


def test_prep_status_error_on_fatal():
    """Prep status should be ERROR when fatal error occurred."""
    status = _determine_prep_status(
        total_symbols=22,
        locked_unique_count=19,
        data_failed_symbols=set(),
        critical_etf_failures=set(),
        has_fatal_error=True,
        min_locked_unique=10,
        target_locked_unique=15,
    )
    assert status == "ERROR"


def test_prep_status_error_on_zero_symbols():
    """Prep status should be ERROR when no symbols loaded."""
    status = _determine_prep_status(
        total_symbols=0,
        locked_unique_count=0,
        data_failed_symbols=set(),
        critical_etf_failures=set(),
        has_fatal_error=False,
        min_locked_unique=10,
        target_locked_unique=15,
    )
    assert status == "ERROR"


def test_prep_status_error_when_locked_unique_is_zero():
    """Prep status should be ERROR when locked unique count is zero."""
    status = _determine_prep_status(
        total_symbols=22,
        locked_unique_count=0,
        data_failed_symbols=set(),
        critical_etf_failures=set(),
        has_fatal_error=False,
        min_locked_unique=10,
        target_locked_unique=15,
    )
    assert status == "ERROR"
