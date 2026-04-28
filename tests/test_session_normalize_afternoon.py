"""
session normalize (pm/close -> afternoon) 테스트
"""
from __future__ import annotations
import pytest


def test_pm_and_close_normalize_to_afternoon():
    from trader.pb1_runner import normalize_session_kind

    assert normalize_session_kind("pm") == "afternoon"
    assert normalize_session_kind("close") == "afternoon"
    assert normalize_session_kind("trade-pm") == "afternoon"
    assert normalize_session_kind("trade-close") == "afternoon"
    assert normalize_session_kind("afternoon") == "afternoon"
    assert normalize_session_kind("trade-afternoon") == "afternoon"


def test_am_stays_am():
    from trader.pb1_runner import normalize_session_kind

    assert normalize_session_kind("am") == "am"
    assert normalize_session_kind("morning") == "am"


def test_empty_raw_stays_empty():
    from trader.pb1_runner import normalize_session_kind

    result = normalize_session_kind("")
    assert result == ""


def test_unknown_value_passes_through():
    from trader.pb1_runner import normalize_session_kind

    assert normalize_session_kind("diag") == "diag"
    assert normalize_session_kind("prep") == "prep"
