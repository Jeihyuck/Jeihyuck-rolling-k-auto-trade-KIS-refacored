# -*- coding: utf-8 -*-
"""Phase 1: AM duplicate guard — buy_orders > 0이어도 exit monitoring 계속 확인.

핵심 검증:
- check_us_am_already_ran: agent_run 완료 레코드만 체크 (buy_orders 제거)
- check_us_am_entry_blocked: buy_orders > 0이면 entry만 차단
- 세션 전체 종료 금지, exit monitoring 계속
"""
from __future__ import annotations

import pytest


# ── check_us_am_already_ran ────────────────────────────────────────────────

def test_already_ran_ignores_buy_orders(monkeypatch):
    """buy_orders가 있어도 agent_run 완료 레코드 없으면 already_ran=False."""
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos._MEM_ORDERS.clear()
    repos._MEM_ORDERS.append({
        "trade_date": "2026-05-01",
        "side": "BUY",
        "symbol": "AAPL",
    })

    result = repos.check_us_am_already_ran("2026-05-01")

    assert result["already_ran"] is False, (
        f"buy_orders만 있는 경우 already_ran은 False여야 함: {result}"
    )
    repos._MEM_ORDERS.clear()


def test_already_ran_no_orders_no_run(monkeypatch):
    """주문도 없고 agent_run도 없으면 already_ran=False."""
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos._MEM_ORDERS.clear()

    result = repos.check_us_am_already_ran("2026-05-01")
    assert result["already_ran"] is False
    repos._MEM_ORDERS.clear()


# ── check_us_am_entry_blocked ──────────────────────────────────────────────

def test_entry_blocked_with_buy_orders(monkeypatch):
    """buy_orders > 0 → entry_blocked=True."""
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos._MEM_ORDERS.clear()
    repos._MEM_ORDERS.append({
        "trade_date": "2026-05-01",
        "side": "BUY",
        "symbol": "AAPL",
    })

    result = repos.check_us_am_entry_blocked("2026-05-01")

    assert result["entry_blocked"] is True
    assert result["buy_orders_count"] >= 1
    repos._MEM_ORDERS.clear()


def test_entry_blocked_no_orders(monkeypatch):
    """buy_orders 없으면 entry_blocked=False."""
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos._MEM_ORDERS.clear()

    result = repos.check_us_am_entry_blocked("2026-05-01")

    assert result["entry_blocked"] is False
    assert result["buy_orders_count"] == 0
    repos._MEM_ORDERS.clear()


def test_entry_blocked_result_fields(monkeypatch):
    """check_us_am_entry_blocked 결과에 필수 필드가 있어야 함."""
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos._MEM_ORDERS.clear()

    result = repos.check_us_am_entry_blocked("2026-05-01")

    assert "entry_blocked" in result
    assert "buy_orders_count" in result
    assert "guard_status" in result
    assert "reason" in result
    repos._MEM_ORDERS.clear()


def test_both_functions_exist():
    """check_us_am_already_ran, check_us_am_entry_blocked 모두 존재해야 함."""
    from trader.us.db import repos
    assert hasattr(repos, "check_us_am_already_ran")
    assert hasattr(repos, "check_us_am_entry_blocked")
    assert callable(repos.check_us_am_already_ran)
    assert callable(repos.check_us_am_entry_blocked)


def test_entry_blocked_sell_orders_not_count(monkeypatch):
    """SELL 주문만 있으면 entry_blocked=False."""
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos._MEM_ORDERS.clear()
    repos._MEM_ORDERS.append({
        "trade_date": "2026-05-01",
        "side": "SELL",
        "symbol": "AAPL",
    })

    result = repos.check_us_am_entry_blocked("2026-05-01")
    assert result["entry_blocked"] is False
    repos._MEM_ORDERS.clear()
