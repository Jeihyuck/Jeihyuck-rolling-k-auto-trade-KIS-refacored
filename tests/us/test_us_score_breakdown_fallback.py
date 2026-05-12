# -*- coding: utf-8 -*-
"""Test US Score Breakdown Fallback.

watchlist entry에서 score_breakdown 추출 및 fallback 로직 검증.
"""
import pytest
from trader.us.pb1.us_explain import extract_us_score_breakdown


def test_extract_score_breakdown_full():
    """모든 score 필드가 있는 경우."""
    entry_data = {
        "breakout_score": 85.0,
        "pullback_score": 70.0,
        "momentum_score": 90.0,
        "vcp_score": 65.0,
        "tech_score": 80.0,
        "score_final": 82.5,
    }
    
    result = extract_us_score_breakdown(entry_data)
    
    assert result["breakout_score"] == 85.0
    assert result["pullback_score"] == 70.0
    assert result["momentum_score"] == 90.0
    assert result["vcp_score"] == 65.0
    assert result["tech_score"] == 80.0
    assert result["score_final"] == 82.5


def test_extract_score_breakdown_missing_fields():
    """일부 score 필드가 없는 경우 0.0 fallback."""
    entry_data = {
        "breakout_score": 85.0,
        "momentum_score": 90.0,
    }
    
    result = extract_us_score_breakdown(entry_data)
    
    assert result["breakout_score"] == 85.0
    assert result["pullback_score"] == 0.0  # fallback
    assert result["momentum_score"] == 90.0
    assert result["vcp_score"] == 0.0  # fallback
    assert result["tech_score"] == 0.0  # fallback
    assert result["score_final"] == 0.0  # fallback


def test_extract_score_breakdown_empty():
    """score 필드가 하나도 없는 경우."""
    entry_data = {}
    
    result = extract_us_score_breakdown(entry_data)
    
    assert result["breakout_score"] == 0.0
    assert result["pullback_score"] == 0.0
    assert result["momentum_score"] == 0.0
    assert result["vcp_score"] == 0.0
    assert result["tech_score"] == 0.0
    assert result["score_final"] == 0.0


def test_extract_score_breakdown_invalid_values():
    """잘못된 score 값은 0.0으로 변환."""
    entry_data = {
        "breakout_score": "invalid",
        "pullback_score": None,
        "momentum_score": 90.0,
    }
    
    result = extract_us_score_breakdown(entry_data)
    
    assert result["breakout_score"] == 0.0  # invalid → 0.0
    assert result["pullback_score"] == 0.0  # None → 0.0
    assert result["momentum_score"] == 90.0


def test_extract_score_breakdown_negative_scores():
    """음수 score도 그대로 반환 (validation은 상위 로직에서 처리)."""
    entry_data = {
        "breakout_score": -10.0,
        "momentum_score": 90.0,
    }
    
    result = extract_us_score_breakdown(entry_data)
    
    assert result["breakout_score"] == -10.0
    assert result["momentum_score"] == 90.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
