# -*- coding: utf-8 -*-
"""Test US Entry Style Extraction.

한국장 PB1 수준의 entry_style 정규화 로직 검증.
"""
import pytest
from trader.us.pb1.us_explain import normalize_us_entry_style


def test_normalize_breakout_variants():
    """ENTRY_BREAKOUT 계열 정규화."""
    assert normalize_us_entry_style("ENTRY_BREAKOUT") == "ENTRY_BREAKOUT"
    assert normalize_us_entry_style("breakout") == "ENTRY_BREAKOUT"
    assert normalize_us_entry_style("ENTRY_BREAKOUT_CONFIRMED") == "ENTRY_BREAKOUT"
    assert normalize_us_entry_style("BREAKOUT_PIVOT") == "ENTRY_BREAKOUT"


def test_normalize_pullback_variants():
    """ENTRY_PULLBACK 계열 정규화."""
    assert normalize_us_entry_style("ENTRY_PULLBACK") == "ENTRY_PULLBACK"
    assert normalize_us_entry_style("pullback") == "ENTRY_PULLBACK"
    assert normalize_us_entry_style("ENTRY_PULLBACK_OVERRIDE") == "ENTRY_PULLBACK"
    assert normalize_us_entry_style("PULLBACK_REVERSAL") == "ENTRY_PULLBACK"


def test_normalize_momentum_variants():
    """ENTRY_MOMENTUM 계열 정규화."""
    assert normalize_us_entry_style("ENTRY_MOMENTUM") == "ENTRY_MOMENTUM"
    assert normalize_us_entry_style("momentum") == "ENTRY_MOMENTUM"
    assert normalize_us_entry_style("ENTRY_MOMENTUM_CONTINUATION") == "ENTRY_MOMENTUM"
    assert normalize_us_entry_style("MOMENTUM_CONTINUATION") == "ENTRY_MOMENTUM"


def test_normalize_vcp_variants():
    """ENTRY_VCP 계열 정규화."""
    assert normalize_us_entry_style("ENTRY_VCP") == "ENTRY_VCP"
    assert normalize_us_entry_style("vcp") == "ENTRY_VCP"
    assert normalize_us_entry_style("ENTRY_VCP_CONFIRMED") == "ENTRY_VCP"


def test_normalize_generic():
    """ENTRY_GENERIC fallback."""
    assert normalize_us_entry_style("ENTRY_GENERIC") == "ENTRY_GENERIC"
    assert normalize_us_entry_style("generic") == "ENTRY_GENERIC"


def test_normalize_unknown_to_skip():
    """알 수 없는 값은 SKIP으로 정규화."""
    assert normalize_us_entry_style("unknown") == "SKIP"
    assert normalize_us_entry_style("") == "SKIP"
    assert normalize_us_entry_style(None) == "SKIP"
    assert normalize_us_entry_style("invalid_style") == "SKIP"


def test_normalize_case_insensitive():
    """대소문자 구분 없이 정규화."""
    assert normalize_us_entry_style("entry_breakout") == "ENTRY_BREAKOUT"
    assert normalize_us_entry_style("EnTrY_MoMeNtUm") == "ENTRY_MOMENTUM"
    assert normalize_us_entry_style("PuLlBaCk") == "ENTRY_PULLBACK"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
