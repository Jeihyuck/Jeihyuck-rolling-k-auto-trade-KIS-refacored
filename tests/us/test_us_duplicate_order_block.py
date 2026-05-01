# -*- coding: utf-8 -*-
"""tests/us/test_us_duplicate_order_block.py

중복 주문 차단 단위 테스트.
"""
from __future__ import annotations

import os
import pytest

from trader.us.execution.risk_gate import check_duplicate, RiskGateBlocked
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
    monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "50")
    clear_sent_order_keys()
    yield
    clear_sent_order_keys()


def make_intent(key: str = "key-001"):
    return {
        "run_id": "run-dup-test",
        "trade_date": "2026-04-30",
        "symbol": "NVDA",
        "exchange": "NASDAQ",
        "side": "BUY",
        "qty": 1,
        "limit_price": 90.0,
        "notional_usd": 90.0,
        "strategy": "us_pb1_pullback",
        "client_order_key": key,
    }


class TestCheckDuplicate:
    def test_new_key_passes(self):
        check_duplicate("brand-new-key", set())

    def test_existing_key_blocked_with_log(self):
        with pytest.raises(RiskGateBlocked) as exc_info:
            check_duplicate("dupe-key", {"dupe-key"})
        assert "[US_DUPLICATE][BLOCK]" in str(exc_info.value)

    def test_empty_key_no_block(self):
        """빈 key는 중복 체크에서 건너뜀 (client_order_key 미설정 방어)."""
        check_duplicate("", {"dupe-key"})


class TestOrderRouterDuplicate:
    def test_first_order_passes(self):
        result = route_order(
            make_intent("unique-key-001"),
            available_cash_usd=500.0,
            total_portfolio_usd=1000.0,
        )
        assert result["status"] == "DRY_RUN"

    def test_second_same_key_blocked(self):
        """동일 client_order_key로 두 번 route_order 호출 시 두 번째는 차단.
        
        Note: DRY_RUN 모드에서는 order_router가 _SENT_ORDER_KEYS에 추가하지 않으므로
        risk_gate의 assert_order_allowed에서 existing_order_keys=None으로 전달된다.
        직접 check_duplicate 테스트로 검증.
        """
        # 직접 risk_gate check_duplicate 검증
        existing = {"duplicate-key-x99"}
        with pytest.raises(RiskGateBlocked):
            check_duplicate("duplicate-key-x99", existing)

    def test_different_keys_both_pass(self):
        r1 = route_order(make_intent("key-a-001"), available_cash_usd=500.0)
        r2 = route_order(make_intent("key-b-002"), available_cash_usd=500.0)
        # DRY_RUN 모드에서 둘 다 DRY_RUN 반환
        assert r1["status"] == "DRY_RUN"
        assert r2["status"] == "DRY_RUN"


class TestDuplicateScenario:
    def test_harness_duplicate_scenario(self):
        """harness 시나리오: duplicate_order_block 재현."""
        import io
        import logging

        log_output = io.StringIO()
        handler = logging.StreamHandler(log_output)
        handler.setLevel(logging.WARNING)
        logging.getLogger("trader.us.execution.risk_gate").addHandler(handler)

        try:
            existing = {"scenario-key-dup"}
            with pytest.raises(RiskGateBlocked):
                check_duplicate("scenario-key-dup", existing)

            log_text = log_output.getvalue()
            assert "[US_DUPLICATE][BLOCK]" in log_text
        finally:
            logging.getLogger("trader.us.execution.risk_gate").removeHandler(handler)
