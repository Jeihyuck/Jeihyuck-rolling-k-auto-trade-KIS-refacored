# -*- coding: utf-8 -*-
"""tests/us/test_us_max_positions_policy.py

미국장 포지션 수 제한 정책 테스트.

한국장 PB1 기준 반영:
- 기본값 30 (10에서 변경)
- 0이면 무제한
- 10종목 보유 시 max_positions_reached로 막히지 않음
"""
from __future__ import annotations

import os
import pytest


def test_us_default_max_positions_is_30(monkeypatch):
    """US_MAX_POSITIONS 기본값이 10이 아니라 30이다."""
    monkeypatch.delenv("US_MAX_POSITIONS", raising=False)
    
    from trader.us import config as us_config
    
    # config 모듈은 import 시 환경변수를 읽으므로 reload 필요
    import importlib
    importlib.reload(us_config)
    
    assert us_config.US_MAX_POSITIONS == 30, f"Expected 30, got {us_config.US_MAX_POSITIONS}"


def test_us_position_sizing_default_is_30(monkeypatch):
    """us_position_sizing의 기본 max_positions도 30이다."""
    monkeypatch.delenv("US_MAX_POSITIONS", raising=False)
    monkeypatch.setenv("US_MAX_ORDER_USD", "2500")
    
    from trader.us.pb1.us_position_sizing import calc_position_size
    
    # position_count=10일 때 차단되지 않음
    result = calc_position_size(
        price=100.0,
        available_cash_usd=100000.0,
        capital_usd_cap=100000.0,
        position_count=10,
        score=0.5,
    )
    
    assert result["blocked"] is False, f"Position count=10 should not be blocked with max=30: {result}"
    assert result["reason"] != "max_positions_reached"


def test_us_position_count_10_not_blocked_when_max_30(monkeypatch):
    """보유 종목 10개일 때 max_positions=30이면 신규 매수 가능."""
    monkeypatch.setenv("US_MAX_POSITIONS", "30")
    monkeypatch.setenv("US_MAX_ORDER_USD", "2500")
    
    from trader.us.pb1.us_position_sizing import calc_position_size
    
    result = calc_position_size(
        price=100.0,
        available_cash_usd=100000.0,
        capital_usd_cap=100000.0,
        position_count=10,
        score=0.5,
    )
    
    assert result["blocked"] is False, result
    assert result["reason"] != "max_positions_reached"
    assert result["qty"] > 0


def test_us_max_positions_zero_means_unlimited(monkeypatch):
    """US_MAX_POSITIONS=0이면 포지션 수 제한 비활성화."""
    monkeypatch.setenv("US_MAX_POSITIONS", "0")
    monkeypatch.setenv("US_MAX_ORDER_USD", "2500")
    
    from trader.us.pb1.us_position_sizing import calc_position_size
    
    # 보유 종목 999개여도 차단되지 않음
    result = calc_position_size(
        price=100.0,
        available_cash_usd=100000.0,
        capital_usd_cap=100000.0,
        position_count=999,
        score=0.5,
    )
    
    assert result["blocked"] is False, result
    assert result["reason"] != "max_positions_reached"


def test_us_max_positions_30_blocks_at_30(monkeypatch):
    """US_MAX_POSITIONS=30이면 30개 도달 시 차단."""
    monkeypatch.setenv("US_MAX_POSITIONS", "30")
    monkeypatch.setenv("US_MAX_ORDER_USD", "2500")
    
    from trader.us.pb1.us_position_sizing import calc_position_size
    
    result = calc_position_size(
        price=100.0,
        available_cash_usd=100000.0,
        capital_usd_cap=100000.0,
        position_count=30,
        score=0.5,
    )
    
    assert result["blocked"] is True
    assert result["reason"] == "max_positions_reached"
    assert result["qty"] == 0


def test_us_max_order_usd_cap_enforced(monkeypatch):
    """US_MAX_ORDER_USD cap이 반영된다."""
    monkeypatch.setenv("US_MAX_POSITIONS", "30")
    monkeypatch.setenv("US_MAX_ORDER_USD", "2500")
    
    from trader.us.pb1.us_position_sizing import calc_position_size
    
    # 가격 100, 잔고 충분, US_MAX_ORDER_USD=2500 → 최대 25주
    result = calc_position_size(
        price=100.0,
        available_cash_usd=100000.0,
        capital_usd_cap=100000.0,
        position_count=0,
        score=1.0,  # high score
    )
    
    assert result["blocked"] is False
    # US_MAX_ORDER_USD=2500이므로 notional은 2500 이하
    assert result["notional_usd"] <= 2500.0, f"Expected notional <= 2500, got {result['notional_usd']}"
