# -*- coding: utf-8 -*-
"""Test US Buy Explanation.

매수 결정 (BUY decision) 설명 생성 검증.
"""
import pytest
from trader.us.pb1.us_explain import build_us_entry_explanation, QUALITY_FULL


def test_build_buy_explanation_breakout():
    """ENTRY_BREAKOUT 매수 설명 생성."""
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
        "ma20": 150.0,
        "ma50": 145.0,
        "close": 155.0,
        "volume": 10_000_000,
        "volume_avg20": 5_000_000,
        "atr_pct": 8.0,
        "pivot_price": 154.0,
        "high_50": 154.0,
    }
    
    result = build_us_entry_explanation(
        symbol="AAPL",
        entry_data=entry_data,
        decision="BUY",
        skip_reason=None,
    )
    
    assert result["symbol"] == "AAPL"
    assert result["decision"] == "BUY"
    assert result["entry_style_selected"] == "ENTRY_BREAKOUT"
    assert result["entry_component"] == "breakout_pivot"
    assert result["score_breakdown"]["breakout_score"] == 85.0
    assert "pivot_break" in result["reasons"]
    assert "high_rs_percentile_90+" in result["reasons"]
    assert result["reject_reasons"] == []
    assert "liquidity_filter" in result["filters_passed"]
    assert result["explanation_quality"] == QUALITY_FULL


def test_build_buy_explanation_pullback():
    """ENTRY_PULLBACK 매수 설명 생성."""
    entry_data = {
        "symbol": "MSFT",
        "entry_style_selected": "ENTRY_PULLBACK",
        "breakout_score": 60.0,
        "pullback_score": 80.0,
        "momentum_score": 65.0,
        "vcp_score": 50.0,
        "tech_score": 70.0,
        "score_final": 75.0,
        "rs_percentile": 85.0,
        "ma20": 300.0,
        "ma50": 295.0,
        "close": 305.0,
        "volume": 8_000_000,
        "volume_avg20": 6_000_000,
        "atr_pct": 6.0,
        "pullback_pct": 0.10,  # 10%
        "high_52w": 340.0,
    }
    
    result = build_us_entry_explanation(
        symbol="MSFT",
        entry_data=entry_data,
        decision="BUY",
        skip_reason=None,
    )
    
    assert result["symbol"] == "MSFT"
    assert result["decision"] == "BUY"
    assert result["entry_style_selected"] == "ENTRY_PULLBACK"
    assert result["entry_component"] == "pullback_reversal"
    assert result["score_breakdown"]["pullback_score"] == 80.0
    assert "pullback_optimal_5-15%" in result["reasons"]
    assert "ma_alignment_ok" in result["reasons"]
    assert result["explanation_quality"] == QUALITY_FULL


def test_build_buy_explanation_momentum():
    """ENTRY_MOMENTUM 매수 설명 생성."""
    entry_data = {
        "symbol": "TSLA",
        "entry_style_selected": "ENTRY_MOMENTUM",
        "breakout_score": 70.0,
        "pullback_score": 65.0,
        "momentum_score": 95.0,
        "vcp_score": 60.0,
        "tech_score": 85.0,
        "score_final": 90.0,
        "rs_percentile": 95.0,
        "ma20": 200.0,
        "ma50": 190.0,
        "close": 210.0,
        "volume": 50_000_000,
        "volume_avg20": 20_000_000,
        "atr_pct": 10.0,
    }
    
    result = build_us_entry_explanation(
        symbol="TSLA",
        entry_data=entry_data,
        decision="BUY",
        skip_reason=None,
    )
    
    assert result["symbol"] == "TSLA"
    assert result["decision"] == "BUY"
    assert result["entry_style_selected"] == "ENTRY_MOMENTUM"
    assert result["entry_component"] == "momentum_continuation"
    assert result["score_breakdown"]["momentum_score"] == 95.0
    assert "momentum_rs_threshold" in result["reasons"]
    assert "volume_surge_2x" in result["reasons"]
    assert result["explanation_quality"] == QUALITY_FULL


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
