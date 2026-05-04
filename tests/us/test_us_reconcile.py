# -*- coding: utf-8 -*-
"""tests/us/test_us_reconcile.py

US Reconcile 단위 테스트.
"""
from __future__ import annotations

import pytest
from trader.us.execution.reconcile import reconcile_positions
from trader.us.execution.fills import get_fills_today


class TestReconcile:
    def test_offline_ok(self):
        from trader.us.data_provider import USDataProvider
        provider = USDataProvider(offline=True)
        result = reconcile_positions(provider=provider)
        assert result["status"] == "OK"
        assert "position_count" in result

    def test_default_offline(self):
        result = reconcile_positions(provider=None)
        assert result["status"] == "OK"

    def test_position_count_nonnegative(self):
        from trader.us.data_provider import USDataProvider
        provider = USDataProvider(offline=True)
        result = reconcile_positions(provider=provider)
        assert result["position_count"] >= 0

    def test_total_pvs_present(self):
        from trader.us.data_provider import USDataProvider
        provider = USDataProvider(offline=True)
        result = reconcile_positions(provider=provider)
        assert "total_pvs" in result


class TestFills:
    def test_offline_fills_empty_list(self):
        from trader.us.data_provider import USDataProvider
        provider = USDataProvider(offline=True)
        result = get_fills_today(provider=provider)
        assert isinstance(result, dict)
        assert result["status"] == "OK"
        assert isinstance(result["fills"], list)
        assert len(result["fills"]) == 0

    def test_fills_default_none_provider(self):
        result = get_fills_today(provider=None)
        assert isinstance(result, dict)
        assert result["status"] == "OK"
        assert isinstance(result["fills"], list)


class TestPartialFillReconcile:
    def test_partial_fill_scenario(self):
        """부분 체결 시나리오: fills < qty인 경우 처리.
        
        offline에서는 stub이므로 fills=[] → position mismatch 없음.
        """
        from trader.us.data_provider import USDataProvider
        provider = USDataProvider(offline=True)
        fills = get_fills_today(provider=provider)
        # offline에서 fills가 빈 목록이면 reconcile에서 mismatch가 없어야 함
        result = reconcile_positions(provider=provider)
        assert result["status"] == "OK"
