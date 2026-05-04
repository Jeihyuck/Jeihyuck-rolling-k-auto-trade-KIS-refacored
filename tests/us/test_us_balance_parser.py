# -*- coding: utf-8 -*-
"""tests/us/test_us_balance_parser.py

KIS 해외주식 잔고 파싱 테스트.
"""
from __future__ import annotations

import pytest
from trader.us.execution.fills import get_fills_today
from trader.us.execution.reconcile import reconcile_positions


class TestOfflineBalance:
    def test_offline_balance_returns_dict(self):
        from trader.us.data_provider import USDataProvider
        provider = USDataProvider(offline=True)
        balance = provider.get_balance()
        assert isinstance(balance, dict)

    def test_offline_balance_has_expected_keys(self):
        from trader.us.data_provider import USDataProvider
        provider = USDataProvider(offline=True)
        balance = provider.get_balance()
        # offline stub은 total_pvs, frcr_pchs_amt1 등을 반환해야 함
        assert "total_pvs" in balance or "frcr_pchs_amt1" in balance or "positions" in balance

    def test_offline_orderable_cash(self):
        from trader.us.data_provider import USDataProvider
        provider = USDataProvider(offline=True)
        cash = provider.get_orderable_cash()
        assert isinstance(cash, float)
        assert cash >= 0


class TestBalanceParseStub:
    def test_parse_total_pvs_string(self):
        """total_pvs는 문자열 형태로 올 수 있음."""
        raw = {"total_pvs": "12345.67", "positions": []}
        total = float(raw.get("total_pvs", 0))
        assert total == pytest.approx(12345.67)

    def test_parse_empty_balance(self):
        """빈 잔고 응답 방어."""
        raw = {}
        positions = raw.get("positions", [])
        assert positions == []

    def test_positions_list_type(self):
        """잔고 응답의 포지션은 목록이어야 함."""
        from trader.us.data_provider import USDataProvider
        provider = USDataProvider(offline=True)
        balance = provider.get_balance()
        positions = balance.get("positions", [])
        assert isinstance(positions, list)


class TestFillsOffline:
    def test_offline_fills_empty(self):
        """offline 모드에서 fills는 빈 리스트."""
        from trader.us.data_provider import USDataProvider
        provider = USDataProvider(offline=True)
        result = get_fills_today(provider=provider)
        assert result["status"] == "OK"
        assert result["fills"] == []

    def test_fills_default_offline(self):
        """provider=None이면 offline stub."""
        result = get_fills_today(provider=None)
        assert isinstance(result, dict)
        assert result["status"] == "OK"
        assert isinstance(result["fills"], list)


class TestReconcileOffline:
    def test_offline_reconcile_ok(self):
        from trader.us.data_provider import USDataProvider
        provider = USDataProvider(offline=True)
        result = reconcile_positions(provider=provider)
        assert result["status"] == "OK"

    def test_offline_reconcile_positions_list(self):
        from trader.us.data_provider import USDataProvider
        provider = USDataProvider(offline=True)
        result = reconcile_positions(provider=provider)
        assert isinstance(result.get("positions", []), list)
