# -*- coding: utf-8 -*-
"""Test US Skip Explanation.

매수 거부 (SKIP decision) 설명 생성 검증.
"""
import pytest
from trader.us.pb1.us_explain import build_us_entry_explanation


def test_build_skip_explanation_sold_today():
    """당일 매도 차단 skip 설명."""
    entry_data = {
        "symbol": "AAPL",
        "entry_style_selected": "ENTRY_BREAKOUT",
        "breakout_score": 85.0,
        "pullback_score": 60.0,
        "momentum_score": 70.0,
        "vcp_score": 55.0,
        "tech_score": 75.0,
        "score_final": 80.0,
        "rs_percentile": 90.0,
        "volume": 10_000_000,
        "volume_avg20": 5_000_000,
        "atr_pct": 8.0,
    }
    
    result = build_us_entry_explanation(
        symbol="AAPL",
        entry_data=entry_data,
        decision="SKIP",
        skip_reason="sold_today",
    )
    
    assert result["symbol"] == "AAPL"
    assert result["decision"] == "SKIP"
    assert "sold_today" in result["reject_reasons"]
    assert result["reasons"] == []


def test_build_skip_explanation_held_position():
    """보유 종목 차단 skip 설명."""
    entry_data = {
        "symbol": "MSFT",
        "entry_style_selected": "ENTRY_PULLBACK",
        "breakout_score": 60.0,
        "pullback_score": 80.0,
        "momentum_score": 65.0,
        "rs_percentile": 85.0,
        "volume": 8_000_000,
        "volume_avg20": 6_000_000,
        "atr_pct": 6.0,
    }
    
    result = build_us_entry_explanation(
        symbol="MSFT",
        entry_data=entry_data,
        decision="SKIP",
        skip_reason="has_position",
    )
    
    assert result["symbol"] == "MSFT"
    assert result["decision"] == "SKIP"
    assert "has_position" in result["reject_reasons"]


def test_build_skip_explanation_insufficient_volume():
    """거래량 부족 skip 설명."""
    entry_data = {
        "symbol": "TSLA",
        "entry_style_selected": "ENTRY_MOMENTUM",
        "breakout_score": 70.0,
        "pullback_score": 65.0,
        "momentum_score": 95.0,
        "rs_percentile": 95.0,
        "volume": 100_000,  # 낮은 거래량
        "volume_avg20": 300_000,
        "atr_pct": 10.0,
    }
    
    result = build_us_entry_explanation(
        symbol="TSLA",
        entry_data=entry_data,
        decision="SKIP",
        skip_reason="insufficient_volume",
    )
    
    assert result["symbol"] == "TSLA"
    assert result["decision"] == "SKIP"
    assert "insufficient_volume" in result["reject_reasons"]
    assert "volume_too_low" in result["reject_reasons"]
    assert "liquidity_filter" in result["filters_failed"]


def test_build_skip_explanation_atr_exceed():
    """ATR 초과 skip 설명."""
    entry_data = {
        "symbol": "NVDA",
        "entry_style_selected": "ENTRY_BREAKOUT",
        "breakout_score": 85.0,
        "pullback_score": 60.0,
        "momentum_score": 70.0,
        "rs_percentile": 90.0,
        "volume": 10_000_000,
        "volume_avg20": 5_000_000,
        "atr_pct": 15.0,  # 12% 초과
    }
    
    result = build_us_entry_explanation(
        symbol="NVDA",
        entry_data=entry_data,
        decision="SKIP",
        skip_reason="atr_exceed",
    )
    
    assert result["symbol"] == "NVDA"
    assert result["decision"] == "SKIP"
    assert "atr_exceed" in result["reject_reasons"]
    assert "atr_exceed_12%" in result["reject_reasons"]
    assert "atr_exceed" in result["filters_failed"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
