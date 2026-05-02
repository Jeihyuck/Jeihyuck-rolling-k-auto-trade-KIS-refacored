# -*- coding: utf-8 -*-
"""tests/us/test_us_risk_gate.py

US Risk Gate 단위 테스트.
"""
from __future__ import annotations

import os
import pytest

from trader.us.execution.risk_gate import (
    RiskGateBlocked,
    check_env_flags,
    check_symbol,
    check_exchange,
    check_qty,
    check_notional,
    check_daily_notional,
    check_position_count,
    check_cash_buffer,
    check_duplicate,
    assert_order_allowed,
)


@pytest.fixture(autouse=True)
def us_env(monkeypatch):
    """US paper trading 환경변수 설정."""
    monkeypatch.setenv("TRADING_REGION", "US")
    monkeypatch.setenv("US_AGENT_ENABLED", "1")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_PAPER_TRADING_ENABLED", "1")
    monkeypatch.setenv("US_LIVE_TRADING_ENABLED", "0")
    monkeypatch.setenv("DISABLE_REAL_TRADING", "1")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("US_MAX_ORDER_USD", "100")
    monkeypatch.setenv("US_MAX_DAILY_NOTIONAL_USD", "500")
    monkeypatch.setenv("US_MAX_POSITIONS", "10")
    monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "0.10")
    monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "50")


class TestEnvFlags:
    def test_pass_with_correct_env(self):
        check_env_flags()  # should not raise

    def test_block_when_region_not_us(self, monkeypatch):
        monkeypatch.setenv("TRADING_REGION", "KR")
        with pytest.raises(RiskGateBlocked, match="trading_region_not_us"):
            check_env_flags()

    def test_block_when_kis_env_real(self, monkeypatch):
        monkeypatch.setenv("KIS_ENV", "real")
        with pytest.raises(RiskGateBlocked, match="kis_env_not_practice"):
            check_env_flags()


class TestSymbolChecks:
    def test_known_symbol_passes(self):
        check_symbol("NVDA")

    def test_unknown_symbol_blocked(self):
        with pytest.raises(RiskGateBlocked, match="symbol_not_in_universe"):
            check_symbol("ZZZZZ")


class TestExchangeChecks:
    def test_known_exchange_passes(self):
        check_exchange("NASDAQ")

    def test_unknown_exchange_blocked(self):
        with pytest.raises(RiskGateBlocked, match="exchange_not_in_registry"):
            check_exchange("KRX")


class TestQtyChecks:
    def test_positive_passes(self):
        check_qty(1)
        check_qty(100)

    def test_zero_blocked(self):
        with pytest.raises(RiskGateBlocked, match="invalid_qty"):
            check_qty(0)

    def test_negative_blocked(self):
        with pytest.raises(RiskGateBlocked, match="invalid_qty"):
            check_qty(-1)


class TestNotionalChecks:
    def test_within_limit_passes(self):
        check_notional(99.0)

    def test_exceeds_limit_blocked(self):
        with pytest.raises(RiskGateBlocked, match="notional_exceeds_order_limit"):
            check_notional(101.0)


class TestDailyNotional:
    def test_within_daily_limit(self):
        check_daily_notional(100.0, 300.0)  # total 400 < 500

    def test_exceeds_daily_limit(self):
        with pytest.raises(RiskGateBlocked, match="daily_notional_exceeded"):
            check_daily_notional(100.0, 450.0)  # total 550 > 500


class TestPositionCount:
    def test_under_limit(self):
        check_position_count(9)

    def test_at_limit_blocked(self):
        with pytest.raises(RiskGateBlocked, match="max_positions_reached"):
            check_position_count(10)


class TestCashBuffer:
    def test_sufficient_cash(self):
        check_cash_buffer(200.0, 100.0)  # remaining 100 > 50

    def test_insufficient_cash(self):
        with pytest.raises(RiskGateBlocked, match="cash_below_buffer"):
            check_cash_buffer(120.0, 100.0)  # remaining 20 < 50


class TestDuplicateCheck:
    def test_new_key_passes(self):
        check_duplicate("new-key-123", set())

    def test_existing_key_blocked(self):
        with pytest.raises(RiskGateBlocked):
            check_duplicate("existing-key", {"existing-key"})


class TestAssertOrderAllowed:
    def _make_intent(self, **kwargs):
        base = {
            "symbol": "NVDA",
            "exchange": "NASDAQ",
            "side": "BUY",
            "qty": 1,
            "notional_usd": 90.0,
            "limit_price": 90.0,
            "client_order_key": "test-key-001",
        }
        base.update(kwargs)
        return base

    def test_valid_intent_passes(self):
        assert_order_allowed(
            self._make_intent(),
            available_cash_usd=500.0,
            total_portfolio_usd=1000.0,
        )

    def test_unknown_symbol_blocked(self):
        with pytest.raises(RiskGateBlocked):
            assert_order_allowed(self._make_intent(symbol="ZZZZZ"))

    def test_excessive_notional_blocked(self):
        with pytest.raises(RiskGateBlocked):
            assert_order_allowed(self._make_intent(notional_usd=200.0))
