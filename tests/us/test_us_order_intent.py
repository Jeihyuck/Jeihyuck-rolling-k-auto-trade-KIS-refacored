# -*- coding: utf-8 -*-
"""tests/us/test_us_order_intent.py

Order Intent 생성 및 Order Router 단위 테스트.
"""
from __future__ import annotations

import os
import pytest

from trader.us.execution.order_router import route_order, clear_sent_order_keys


@pytest.fixture(autouse=True)
def us_env(monkeypatch):
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
    clear_sent_order_keys()
    yield
    clear_sent_order_keys()


def make_intent(**kwargs):
    base = {
        "run_id": "run-001",
        "trade_date": "2026-04-30",
        "symbol": "NVDA",
        "exchange": "NASDAQ",
        "side": "BUY",
        "qty": 1,
        "limit_price": 90.0,
        "notional_usd": 90.0,
        "strategy": "us_pb1_pullback",
        "reason_json": {},
        "risk_snapshot_json": {},
        "client_order_key": "nvda-buy-2026-04-30-001",
    }
    base.update(kwargs)
    return base


class TestOrderIntentFormat:
    def test_required_fields_present(self):
        intent = make_intent()
        for field in ["symbol", "exchange", "side", "qty", "limit_price", "notional_usd", "client_order_key"]:
            assert field in intent

    def test_dry_run_returns_dry_run_status(self):
        result = route_order(
            make_intent(),
            available_cash_usd=500.0,
            total_portfolio_usd=1000.0,
        )
        assert result["status"] == "DRY_RUN"
        assert result["symbol"] == "NVDA"

    def test_blocked_returns_blocked_status(self):
        result = route_order(
            make_intent(symbol="ZZZZZ"),
            available_cash_usd=500.0,
        )
        assert result["status"] == "BLOCKED"

    def test_sell_intent(self):
        result = route_order(
            make_intent(side="SELL"),
            available_cash_usd=500.0,
            total_portfolio_usd=1000.0,
        )
        assert result["status"] == "DRY_RUN"
        assert result["side"] == "SELL"


class TestDryRunBehavior:
    def test_dry_run_does_not_raise(self):
        """DRY_RUN=1이면 KIS API 호출 없이 반환."""
        result = route_order(
            make_intent(),
            available_cash_usd=500.0,
            total_portfolio_usd=1000.0,
        )
        assert result["status"] == "DRY_RUN"

    def test_dry_run_preserves_intent(self):
        intent = make_intent(client_order_key="test-preserve-key")
        result = route_order(
            intent,
            available_cash_usd=500.0,
            total_portfolio_usd=1000.0,
        )
        assert result["intent"]["client_order_key"] == "test-preserve-key"


class TestEdgeCases:
    def test_zero_qty_blocked(self):
        result = route_order(
            make_intent(qty=0),
            available_cash_usd=500.0,
        )
        assert result["status"] == "BLOCKED"

    def test_notional_exceeds_limit_blocked(self):
        result = route_order(
            make_intent(notional_usd=150.0),
            available_cash_usd=500.0,
        )
        assert result["status"] == "BLOCKED"
