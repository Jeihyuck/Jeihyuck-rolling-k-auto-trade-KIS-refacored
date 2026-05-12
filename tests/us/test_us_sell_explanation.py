# -*- coding: utf-8 -*-
"""Test US Sell Explanation.

매도 결정 (SELL decision) 설명 생성 검증.
"""
import pytest
from trader.us.pb1.us_explain import build_us_exit_explanation, QUALITY_FULL


def test_build_sell_explanation_hard_stop():
    """hard_stop 매도 설명."""
    position = {
        "symbol": "AAPL",
        "exchange": "NASDAQ",
        "qty": 10,
        "entry_price": 150.0,
        "entry_date": "2026-01-01",
        "max_price": 152.0,
    }
    
    exit_intent = {
        "exit_type": "hard_stop",
        "reason": "pnl_pct=-0.08 <= -0.07",
    }
    
    current_price = 138.0  # -8% 손실
    
    result = build_us_exit_explanation(
        symbol="AAPL",
        position=position,
        exit_intent=exit_intent,
        current_price=current_price,
    )
    
    assert result["symbol"] == "AAPL"
    assert result["decision"] == "SELL"
    assert result["exit_style"] == "hard_stop"
    assert result["exit_trigger"] == "stop_loss_hit"
    assert result["pnl_pct"] < -0.07
    assert "stop_loss_hit" in result["reasons"]
    assert result["explanation_quality"] == QUALITY_FULL


def test_build_sell_explanation_trailing_stop():
    """trailing_stop 매도 설명."""
    position = {
        "symbol": "MSFT",
        "exchange": "NASDAQ",
        "qty": 5,
        "entry_price": 300.0,
        "entry_date": "2026-01-01",
        "max_price": 330.0,  # 고점 +10%
    }
    
    exit_intent = {
        "exit_type": "trailing_stop",
        "reason": "trail_pct=0.06 > 0.05",
    }
    
    current_price = 310.0  # 고점 대비 -6%
    
    result = build_us_exit_explanation(
        symbol="MSFT",
        position=position,
        exit_intent=exit_intent,
        current_price=current_price,
    )
    
    assert result["symbol"] == "MSFT"
    assert result["decision"] == "SELL"
    assert result["exit_style"] == "trailing_stop"
    assert result["exit_trigger"] == "trail_threshold_breach"
    assert result["pnl_pct"] > 0  # 여전히 수익
    assert "trail_threshold_breach" in result["reasons"]


def test_build_sell_explanation_profit_protect():
    """profit_protect 매도 설명."""
    position = {
        "symbol": "TSLA",
        "exchange": "NASDAQ",
        "qty": 3,
        "entry_price": 200.0,
        "entry_date": "2026-01-01",
        "max_price": 240.0,  # 고점 +20%
    }
    
    exit_intent = {
        "exit_type": "profit_protect",
        "reason": "pnl_pct=0.16 high but pulling back",
    }
    
    current_price = 232.0  # +16%, but 고점에서 하락
    
    result = build_us_exit_explanation(
        symbol="TSLA",
        position=position,
        exit_intent=exit_intent,
        current_price=current_price,
    )
    
    assert result["symbol"] == "TSLA"
    assert result["decision"] == "SELL"
    assert result["exit_style"] == "profit_protect"
    assert result["exit_trigger"] == "profit_protect_triggered"
    assert result["pnl_pct"] > 0.15


def test_build_sell_explanation_giveback():
    """giveback 매도 설명."""
    position = {
        "symbol": "NVDA",
        "exchange": "NASDAQ",
        "qty": 4,
        "entry_price": 500.0,
        "entry_date": "2026-01-01",
        "max_price": 600.0,  # 고점 +20%
    }
    
    exit_intent = {
        "exit_type": "giveback",
        "reason": "giveback_ratio=0.40 >= 0.33",
    }
    
    current_price = 560.0  # +12%, but 최고 수익 20%의 40% 반납
    
    result = build_us_exit_explanation(
        symbol="NVDA",
        position=position,
        exit_intent=exit_intent,
        current_price=current_price,
    )
    
    assert result["symbol"] == "NVDA"
    assert result["decision"] == "SELL"
    assert result["exit_style"] == "giveback"
    assert result["exit_trigger"] == "giveback_limit_exceed"
    assert result["pnl_pct"] > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
