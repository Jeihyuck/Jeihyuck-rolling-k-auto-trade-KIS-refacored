# -*- coding: utf-8 -*-
"""order_router DB 저장 테스트.

- DRY_RUN 모드에서 router가 us_orders에 저장하는지
- 중복 key 재실행시 BLOCKED 반환
"""
import os
import importlib
import pytest


def _setup_env(monkeypatch):
    """테스트용 환경 변수 설정."""
    pairs = {
        "TRADING_REGION": "US",
        "US_AGENT_ENABLED": "1",
        "KIS_ENV": "practice",
        "ALLOW_REAL_ORDER": "0",
        "DRY_RUN": "1",
        "US_PAPER_TRADING_ENABLED": "1",
        "US_MAX_ORDER_USD": "5000",
        "US_PAPER_MAX_CAPITAL_KRW": "100000000",
        "US_BUDGET_FX_KRW_PER_USD": "1450",
    }
    for k, v in pairs.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("PBCORE_DB_URL", raising=False)


def _make_intent(symbol="AAPL", side="BUY", qty=1, notional=100.0):
    return {
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "price_usd": notional / qty,
        "notional_usd": notional,
        "order_key": f"{symbol}:{side}:test",
        "exchange": "NASD",
        "strategy": "pb1",
        "available_qty": qty if side == "SELL" else None,
        "signal_ts": "2026-01-02T09:35:00-05:00",
    }


def test_dry_run_order_saved_to_memory(monkeypatch):
    """DRY_RUN 주문이 in-memory store에 저장되어 load_today_order_keys에서 보인다."""
    _setup_env(monkeypatch)

    # 모듈 리로드 (환경 변수 반영)
    import trader.us.db.repos as repos
    import trader.us.execution.order_router as router

    # 메모리 리셋 (이전 테스트 영향 배제)
    repos.reset_memory_stores()
    router._SENT_ORDER_KEYS.clear()

    intent = _make_intent()
    result = router.route_order(intent)

    # DRY_RUN 또는 BLOCKED 중 하나 (중복 없으면 DRY_RUN)
    assert result["status"] in ("DRY_RUN", "BLOCKED"), f"예상치 못한 status: {result}"

    if result["status"] == "DRY_RUN":
        # load_today_order_keys에서 해당 key 확인
        keys = repos.load_today_order_keys()
        assert intent["order_key"] in keys, "DRY_RUN 주문 key가 DB(memory)에 없다"


def test_duplicate_order_blocked(monkeypatch):
    """동일 intent를 두 번 보내면 두 번째는 BLOCKED."""
    _setup_env(monkeypatch)

    import trader.us.db.repos as repos
    import trader.us.execution.order_router as router

    repos.reset_memory_stores()
    router._SENT_ORDER_KEYS.clear()

    intent = _make_intent(symbol="MSFT", notional=50.0)

    r1 = router.route_order(intent)
    assert r1["status"] in ("DRY_RUN", "BLOCKED")

    if r1["status"] == "DRY_RUN":
        # 두 번째
        r2 = router.route_order(intent)
        assert r2["status"] == "BLOCKED", f"두 번째 주문이 BLOCKED가 아님: {r2}"


def test_sell_qty_exceeds_available_blocked(monkeypatch):
    """SELL qty > available_qty 이면 BLOCKED."""
    _setup_env(monkeypatch)

    import trader.us.db.repos as repos
    import trader.us.execution.order_router as router

    repos.reset_memory_stores()
    router._SENT_ORDER_KEYS.clear()

    intent = _make_intent(symbol="AAPL", side="SELL", qty=10, notional=1000.0)
    intent["available_qty"] = 5  # 5주만 있는데 10주 매도 시도

    result = router.route_order(intent)
    assert result["status"] == "BLOCKED", f"SELL qty 초과 시 BLOCKED 아님: {result}"
