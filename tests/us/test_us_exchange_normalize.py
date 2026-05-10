# -*- coding: utf-8 -*-
"""US exchange code normalization tests.

NASD, NAS → NASDAQ
NYS → NYSE
AMS, ASE → AMEX
"""
import pytest
from trader.us.symbols import normalize_us_exchange


def test_nasd_to_nasdaq():
    """NASD는 NASDAQ으로 정규화되어야 한다."""
    assert normalize_us_exchange("NASD") == "NASDAQ"


def test_nas_to_nasdaq():
    """NAS는 NASDAQ으로 정규화되어야 한다."""
    assert normalize_us_exchange("NAS") == "NASDAQ"


def test_nasdaq_unchanged():
    """NASDAQ은 그대로 유지되어야 한다."""
    assert normalize_us_exchange("NASDAQ") == "NASDAQ"


def test_nys_to_nyse():
    """NYS는 NYSE로 정규화되어야 한다."""
    assert normalize_us_exchange("NYS") == "NYSE"


def test_nyse_unchanged():
    """NYSE는 그대로 유지되어야 한다."""
    assert normalize_us_exchange("NYSE") == "NYSE"


def test_ams_to_amex():
    """AMS는 AMEX로 정규화되어야 한다."""
    assert normalize_us_exchange("AMS") == "AMEX"


def test_ase_to_amex():
    """ASE는 AMEX로 정규화되어야 한다."""
    assert normalize_us_exchange("ASE") == "AMEX"


def test_amex_unchanged():
    """AMEX는 그대로 유지되어야 한다."""
    assert normalize_us_exchange("AMEX") == "AMEX"


def test_case_insensitive():
    """소문자 입력도 처리되어야 한다."""
    assert normalize_us_exchange("nasd") == "NASDAQ"
    assert normalize_us_exchange("nys") == "NYSE"
    assert normalize_us_exchange("ams") == "AMEX"


def test_unknown_exchange_raises():
    """알 수 없는 거래소 코드는 ValueError를 발생시켜야 한다."""
    with pytest.raises(ValueError, match="unknown exchange"):
        normalize_us_exchange("UNKNOWN")


def test_empty_exchange_raises():
    """빈 거래소 코드는 ValueError를 발생시켜야 한다."""
    with pytest.raises(ValueError, match="invalid exchange"):
        normalize_us_exchange("")


def test_none_exchange_raises():
    """None 거래소 코드는 ValueError를 발생시켜야 한다."""
    with pytest.raises(ValueError, match="invalid exchange"):
        normalize_us_exchange(None)


def test_whitespace_handling():
    """공백이 포함된 입력도 처리되어야 한다."""
    assert normalize_us_exchange(" NASD ") == "NASDAQ"
    assert normalize_us_exchange(" NYSE ") == "NYSE"
