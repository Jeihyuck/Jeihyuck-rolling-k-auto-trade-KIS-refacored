# -*- coding: utf-8 -*-
"""US prep status contract tests.

US prep status must be based on unique symbols and locked watchlist contract,
not duplicated strategy event count.
"""

from trader.us.runner.prep_runner import _determine_prep_status


def test_prep_status_uses_unique_success_count_ok():
    """Prep status should be OK when all symbols scored."""
    status = _determine_prep_status(
        total_symbols=22,
        success_count=22,
        critical_etf_failures=set(),
        has_fatal_error=False,
    )
    assert status == "OK"


def test_prep_status_ok_with_warnings_when_partial_success():
    """Prep status should be OK_WITH_WARNINGS when 70-90% success."""
    status = _determine_prep_status(
        total_symbols=22,
        success_count=18,  # 81.8%
        critical_etf_failures=set(),
        has_fatal_error=False,
    )
    assert status == "OK_WITH_WARNINGS"


def test_prep_status_degraded_when_critical_etf_failed():
    """Prep status should be DEGRADED when critical ETF failed."""
    status = _determine_prep_status(
        total_symbols=22,
        success_count=22,
        critical_etf_failures={"SPY"},
        has_fatal_error=False,
    )
    assert status == "DEGRADED"


def test_prep_status_degraded_when_low_success_rate():
    """Prep status should be DEGRADED when success rate < 70%."""
    status = _determine_prep_status(
        total_symbols=22,
        success_count=12,  # 54.5%
        critical_etf_failures=set(),
        has_fatal_error=False,
    )
    assert status == "DEGRADED"


def test_prep_status_error_on_fatal():
    """Prep status should be ERROR when fatal error occurred."""
    status = _determine_prep_status(
        total_symbols=22,
        success_count=22,
        critical_etf_failures=set(),
        has_fatal_error=True,
    )
    assert status == "ERROR"


def test_prep_status_error_on_zero_symbols():
    """Prep status should be ERROR when no symbols loaded."""
    status = _determine_prep_status(
        total_symbols=0,
        success_count=0,
        critical_etf_failures=set(),
        has_fatal_error=False,
    )
    assert status == "ERROR"
