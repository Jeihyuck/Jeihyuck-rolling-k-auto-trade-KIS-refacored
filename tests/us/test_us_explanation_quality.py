# -*- coding: utf-8 -*-
"""Test US Explanation Quality.

설명 품질 레벨 (FULL/PARTIAL/MINIMAL/MISSING) 평가 검증.
"""
import pytest
from trader.us.pb1.us_explain import (
    evaluate_explanation_quality,
    validate_explanations_batch,
    QUALITY_FULL,
    QUALITY_PARTIAL,
    QUALITY_MINIMAL,
    QUALITY_MISSING,
)


def test_evaluate_quality_full():
    """FULL 품질: 모든 핵심 필드 완비."""
    explanation = {
        "entry_style_selected": "ENTRY_BREAKOUT",
        "score_breakdown": {"breakout_score": 85.0},
        "reasons": ["pivot_break", "volume_surge"],
        "filters_passed": ["liquidity_filter", "atr_filter"],
    }
    
    assert evaluate_explanation_quality(explanation) == QUALITY_FULL


def test_evaluate_quality_partial_no_filters():
    """PARTIAL 품질: filters 없음."""
    explanation = {
        "entry_style_selected": "ENTRY_PULLBACK",
        "score_breakdown": {"pullback_score": 80.0},
        "reasons": ["pullback_optimal"],
        "filters_passed": [],  # empty filters
    }
    
    assert evaluate_explanation_quality(explanation) == QUALITY_PARTIAL


def test_evaluate_quality_partial_no_score():
    """PARTIAL 품질: score_breakdown 없음."""
    explanation = {
        "entry_style_selected": "ENTRY_MOMENTUM",
        "score_breakdown": {},
        "reasons": ["momentum_rs_threshold"],
        "filters_passed": ["rs_filter"],
    }
    
    assert evaluate_explanation_quality(explanation) == QUALITY_PARTIAL


def test_evaluate_quality_minimal_only_score():
    """MINIMAL 품질: score만 있음."""
    explanation = {
        "entry_style_selected": "",
        "score_breakdown": {"score_final": 75.0},
        "reasons": [],
        "filters_passed": [],
    }
    
    assert evaluate_explanation_quality(explanation) == QUALITY_MINIMAL


def test_evaluate_quality_minimal_only_reasons():
    """MINIMAL 품질: reasons만 있음."""
    explanation = {
        "entry_style_selected": "",
        "score_breakdown": {},
        "reasons": ["entry_condition_met"],
        "filters_passed": [],
    }
    
    assert evaluate_explanation_quality(explanation) == QUALITY_MINIMAL


def test_evaluate_quality_missing():
    """MISSING 품질: 설명 전무."""
    explanation = {
        "entry_style_selected": "",
        "score_breakdown": {},
        "reasons": [],
        "filters_passed": [],
    }
    
    assert evaluate_explanation_quality(explanation) == QUALITY_MISSING


def test_validate_batch_all_full():
    """일괄 검증: 모두 FULL 품질."""
    explanations = [
        {
            "entry_style_selected": "ENTRY_BREAKOUT",
            "score_breakdown": {"breakout_score": 85.0},
            "reasons": ["pivot_break"],
            "filters_passed": ["liquidity_filter"],
            "explanation_quality": QUALITY_FULL,
        },
        {
            "entry_style_selected": "ENTRY_PULLBACK",
            "score_breakdown": {"pullback_score": 80.0},
            "reasons": ["pullback_optimal"],
            "filters_passed": ["atr_filter"],
            "explanation_quality": QUALITY_FULL,
        },
    ]
    
    result = validate_explanations_batch(explanations)
    
    assert result["total_count"] == 2
    assert result["full_count"] == 2
    assert result["partial_count"] == 0
    assert result["minimal_count"] == 0
    assert result["missing_count"] == 0
    assert result["quality_warning"] is False
    assert "100% FULL" in result["quality_summary"]


def test_validate_batch_mixed_quality():
    """일괄 검증: 혼합 품질."""
    explanations = [
        {
            "entry_style_selected": "ENTRY_BREAKOUT",
            "score_breakdown": {"breakout_score": 85.0},
            "reasons": ["pivot_break"],
            "filters_passed": ["liquidity_filter"],
            "explanation_quality": QUALITY_FULL,
        },
        {
            "entry_style_selected": "ENTRY_PULLBACK",
            "score_breakdown": {"pullback_score": 80.0},
            "reasons": ["pullback_optimal"],
            "filters_passed": [],
            "explanation_quality": QUALITY_PARTIAL,
        },
        {
            "entry_style_selected": "",
            "score_breakdown": {"score_final": 70.0},
            "reasons": [],
            "filters_passed": [],
            "explanation_quality": QUALITY_MINIMAL,
        },
    ]
    
    result = validate_explanations_batch(explanations)
    
    assert result["total_count"] == 3
    assert result["full_count"] == 1
    assert result["partial_count"] == 1
    assert result["minimal_count"] == 1
    assert result["missing_count"] == 0
    assert result["quality_warning"] is False  # FULL이 1개 이상 있으므로 경고 없음
    assert "33% FULL" in result["quality_summary"]
    assert "33% PARTIAL" in result["quality_summary"]
    assert "33% MINIMAL" in result["quality_summary"]


def test_validate_batch_all_minimal_warning():
    """일괄 검증: 모두 MINIMAL이면 경고."""
    explanations = [
        {
            "entry_style_selected": "",
            "score_breakdown": {"score_final": 70.0},
            "reasons": [],
            "filters_passed": [],
            "explanation_quality": QUALITY_MINIMAL,
        },
        {
            "entry_style_selected": "",
            "score_breakdown": {"score_final": 75.0},
            "reasons": [],
            "filters_passed": [],
            "explanation_quality": QUALITY_MINIMAL,
        },
    ]
    
    result = validate_explanations_batch(explanations)
    
    assert result["total_count"] == 2
    assert result["minimal_count"] == 2
    assert result["quality_warning"] is True  # 모두 MINIMAL
    assert "100% MINIMAL" in result["quality_summary"]


def test_validate_batch_has_missing_warning():
    """일괄 검증: MISSING이 있으면 경고."""
    explanations = [
        {
            "entry_style_selected": "ENTRY_BREAKOUT",
            "score_breakdown": {"breakout_score": 85.0},
            "reasons": ["pivot_break"],
            "filters_passed": ["liquidity_filter"],
            "explanation_quality": QUALITY_FULL,
        },
        {
            "entry_style_selected": "",
            "score_breakdown": {},
            "reasons": [],
            "filters_passed": [],
            "explanation_quality": QUALITY_MISSING,
        },
    ]
    
    result = validate_explanations_batch(explanations)
    
    assert result["total_count"] == 2
    assert result["full_count"] == 1
    assert result["missing_count"] == 1
    assert result["quality_warning"] is True  # MISSING 존재
    assert "50% MISSING" in result["quality_summary"]


def test_validate_batch_empty():
    """일괄 검증: 빈 리스트."""
    result = validate_explanations_batch([])
    
    assert result["total_count"] == 0
    assert result["quality_warning"] is True
    assert result["quality_summary"] == "NO_EXPLANATIONS"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
