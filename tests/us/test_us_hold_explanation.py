# -*- coding: utf-8 -*-
"""Test US Hold Explanation.

보유 결정 (HOLD / NO_EXIT_SIGNAL) 설명 생성 검증.
"""
import pytest
from trader.us.pb1.us_explain import build_us_exit_explanation, QUALITY_FULL


def test_build_hold_explanation_no_exit_signal():
    """NO_EXIT_SIGNAL 보유 설명 (청산 조건 미충족)."""
    position = {
        "symbol": "AAPL",
        "exchange": "NASDAQ",
        "qty": 10,
        "entry_price": 150.0,
        "entry_date": "2026-01-01",
        "max_price": 155.0,
    }
    
    current_price = 154.0  # +2.67%, 안정적
    
    result = build_us_exit_explanation(
        symbol="AAPL",
        position=position,
        exit_intent=None,  # 청산 intent 없음
        current_price=current_price,
    )
    
    assert result["symbol"] == "AAPL"
    assert result["decision"] == "HOLD"
    assert result["exit_style"] == "no_exit"
    assert result["exit_trigger"] is None
    assert result["why_not_sell"] == "price_near_high_no_trail_signal"
    assert result["pnl_pct"] > 0
    assert result["explanation_quality"] == QUALITY_FULL


def test_build_hold_explanation_minor_loss():
    """5% 미만 손실로 보유."""
    position = {
        "symbol": "MSFT",
        "exchange": "NASDAQ",
        "qty": 5,
        "entry_price": 300.0,
        "entry_date": "2026-01-01",
        "max_price": 302.0,
    }
    
    current_price = 290.0  # -3.33%, stop 미도달
    
    result = build_us_exit_explanation(
        symbol="MSFT",
        position=position,
        exit_intent=None,
        current_price=current_price,
    )
    
    assert result["symbol"] == "MSFT"
    assert result["decision"] == "HOLD"
    assert result["exit_style"] == "no_exit"
    assert result["why_not_sell"] == "within_risk_tolerance_under_5%"
    assert result["pnl_pct"] < 0
    assert abs(result["pnl_pct"]) < 0.05


def test_build_hold_explanation_stop_not_hit():
    """손절 기준 미도달 보유."""
    position = {
        "symbol": "TSLA",
        "exchange": "NASDAQ",
        "qty": 3,
        "entry_price": 200.0,
        "entry_date": "2026-01-01",
        "max_price": 205.0,
    }
    
    current_price = 190.0  # -5%, but < -7% stop threshold
    
    result = build_us_exit_explanation(
        symbol="TSLA",
        position=position,
        exit_intent=None,
        current_price=current_price,
    )
    
    assert result["symbol"] == "TSLA"
    assert result["decision"] == "HOLD"
    assert result["exit_style"] == "no_exit"
    assert result["why_not_sell"] == "stop_not_hit_yet"
    assert result["pnl_pct"] < 0


def test_build_hold_explanation_trail_not_breached():
    """trailing stop 미충족 보유."""
    position = {
        "symbol": "NVDA",
        "exchange": "NASDAQ",
        "qty": 4,
        "entry_price": 500.0,
        "entry_date": "2026-01-01",
        "max_price": 600.0,
    }
    
    current_price = 590.0  # 고점 대비 -1.67%, < 5% trail threshold
    
    result = build_us_exit_explanation(
        symbol="NVDA",
        position=position,
        exit_intent=None,
        current_price=current_price,
    )
    
    assert result["symbol"] == "NVDA"
    assert result["decision"] == "HOLD"
    assert result["exit_style"] == "no_exit"
    assert result["why_not_sell"] == "price_near_high_no_trail_signal"
    assert result["pnl_pct"] > 0


def test_build_hold_explanation_no_conditions_met():
    """어떤 청산 조건도 충족하지 않음."""
    position = {
        "symbol": "GOOG",
        "exchange": "NASDAQ",
        "qty": 2,
        "entry_price": 1000.0,
        "entry_date": "2026-01-01",
        "max_price": 1000.0,  # max_price가 entry_price와 동일
    }
    
    current_price = 1005.0  # +0.5%
    
    result = build_us_exit_explanation(
        symbol="GOOG",
        position=position,
        exit_intent=None,
        current_price=current_price,
    )
    
    assert result["symbol"] == "GOOG"
    assert result["decision"] == "HOLD"
    assert result["exit_style"] == "no_exit"
    assert result["why_not_sell"] == "no_exit_conditions_met"
    assert result["pnl_pct"] > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
