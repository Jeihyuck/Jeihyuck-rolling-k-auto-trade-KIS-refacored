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
    check_symbol_contract,
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


class TestCheckSymbolContract:
    """check_symbol_contract() BUY/SELL 분리 검증."""

    def test_buy_passes_if_symbol_in_allowed_symbols(self):
        """BUY: allowed_symbols에 있으면 통과해야 한다."""
        allowed = {"AAAA", "BBBB", "CCCC"}
        # 하드코딩 없이 — allowed_symbols에서 임의 하나를 선택
        sym = next(iter(allowed))
        check_symbol_contract(sym, "BUY", allowed_symbols=allowed)  # no exception

    def test_buy_blocked_if_symbol_not_in_allowed_symbols(self):
        """BUY: allowed_symbols에 없으면 차단해야 한다."""
        allowed = {"AAAA", "BBBB"}
        with pytest.raises(RiskGateBlocked, match="symbol_not_in_universe"):
            check_symbol_contract("ZZZZ", "BUY", allowed_symbols=allowed)

    def test_sell_passes_if_symbol_in_current_position_symbols(self):
        """SELL: current_position_symbols에 있으면 통과해야 한다."""
        positions = {"AAAA", "BBBB", "CCCC"}
        sym = next(iter(positions))
        check_symbol_contract(sym, "SELL", current_position_symbols=positions)  # no exception

    def test_sell_blocked_if_not_in_positions_or_watchlist(self):
        """SELL: 보유 포지션 및 watchlist 모두 없으면 차단해야 한다."""
        positions = {"AAAA", "BBBB"}
        with pytest.raises(RiskGateBlocked, match="symbol_not_in_universe"):
            check_symbol_contract("ZZZZ", "SELL", current_position_symbols=positions)

    def test_buy_uses_locked_watchlist_symbols(self):
        """assert_order_allowed에 allowed_symbols 전달 시 locked watchlist 기준으로 BUY universe 검증."""
        # dynamic set — no hardcoded symbol names
        allowed = {"AAAA", "BBBB", "CCCC"}
        sym = next(iter(allowed))
        intent = {
            "symbol": sym,
            "exchange": "NASDAQ",
            "side": "BUY",
            "qty": 1,
            "notional_usd": 90.0,
            "limit_price": 90.0,
            "client_order_key": f"test-wl-{sym}",
        }
        # allowed_symbols에 있으므로 차단되지 않아야 함
        assert_order_allowed(
            intent,
            available_cash_usd=500.0,
            total_portfolio_usd=1000.0,
            allowed_symbols=allowed,
        )

    def test_sell_allows_current_position_even_if_not_locked(self):
        """SELL: 보유 포지션에 있으면 locked watchlist 무관하게 허용해야 한다."""
        positions = {"PPPP", "QQQQ"}
        sym = next(iter(positions))
        intent = {
            "symbol": sym,
            "exchange": "NASDAQ",
            "side": "SELL",
            "qty": 1,
            "notional_usd": 0.0,
            "limit_price": 0.0,
            "client_order_key": f"test-sell-{sym}",
        }
        # allowed_symbols에는 없지만 current_position_symbols에 있으므로 통과
        assert_order_allowed(
            intent,
            available_cash_usd=500.0,
            total_portfolio_usd=1000.0,
            allowed_symbols={"AAAA"},  # sym이 없는 watchlist
            current_position_symbols=positions,
        )


def test_add_to_existing_buy_skips_max_positions(monkeypatch, caplog):
    monkeypatch.setenv("US_MAX_POSITIONS", "30")
    monkeypatch.setenv("US_MAX_ORDER_USD", "2500")
    monkeypatch.setenv("US_MAX_DAILY_NOTIONAL_USD", "10000")
    monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "0")
    monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "1.0")
    intent = {
        "symbol": "DELL", "exchange": "NYSE", "side": "BUY", "qty": 1,
        "notional_usd": 100.0, "limit_price": 100.0, "client_order_key": "add-buy",
        "position_action": "ADD_TO_EXISTING_BUY",
    }
    caplog.set_level("INFO")
    assert_order_allowed(
        intent,
        current_position_count=30,
        available_cash_usd=1000.0,
        total_portfolio_usd=10000.0,
        allowed_symbols={"DELL"},
        current_position_symbols={"DELL", "AMD"},
        is_existing_position_buy=True,
    )
    assert "[US_RISK][POSITION_COUNT_SKIP]" in caplog.text


def test_new_position_buy_blocks_at_max_positions(monkeypatch):
    monkeypatch.setenv("US_MAX_POSITIONS", "30")
    monkeypatch.setenv("US_MAX_ORDER_USD", "2500")
    monkeypatch.setenv("US_MAX_DAILY_NOTIONAL_USD", "10000")
    monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "0")
    monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "1.0")
    intent = {
        "symbol": "FLEX", "exchange": "NASDAQ", "side": "BUY", "qty": 1,
        "notional_usd": 100.0, "limit_price": 100.0, "client_order_key": "new-buy",
        "position_action": "NEW_POSITION_BUY",
    }
    with pytest.raises(RiskGateBlocked, match="max_positions_reached_new_symbol"):
        assert_order_allowed(
            intent,
            current_position_count=30,
            available_cash_usd=1000.0,
            total_portfolio_usd=10000.0,
            allowed_symbols={"FLEX"},
            current_position_symbols={"DELL", "AMD"},
            is_existing_position_buy=False,
        )
