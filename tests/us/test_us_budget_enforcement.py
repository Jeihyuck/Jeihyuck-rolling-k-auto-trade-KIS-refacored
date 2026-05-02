# -*- coding: utf-8 -*-
"""US 예산 상한 차단 테스트.

US_PAPER_MAX_CAPITAL_KRW / FX_RATE_KRW_PER_USD = USD 상한
notional_usd > 상한 → BLOCKED
notional_usd ≤ 상한 → PASS (DRY_RUN)
"""
import pytest


def _setup_env(monkeypatch, *, max_krw="50000000", fx="1450", max_order="36000"):
    monkeypatch.setenv("TRADING_REGION", "US")
    monkeypatch.setenv("US_AGENT_ENABLED", "1")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("ALLOW_REAL_ORDER", "0")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("US_PAPER_TRADING_ENABLED", "1")
    monkeypatch.setenv("US_PAPER_MAX_CAPITAL_KRW", max_krw)
    monkeypatch.setenv("US_BUDGET_FX_KRW_PER_USD", fx)
    monkeypatch.setenv("US_MAX_ORDER_USD", max_order)
    monkeypatch.delenv("PBCORE_DB_URL", raising=False)


def _make_intent(symbol, notional_usd, qty=1):
    return {
        "symbol": symbol,
        "side": "BUY",
        "qty": qty,
        "price_usd": notional_usd / qty,
        "notional_usd": notional_usd,
        "order_key": f"{symbol}:BUY:budget_test",
        "exchange": "NASD",
        "strategy": "pb1",
        "available_qty": None,
        "signal_ts": "2026-01-02T09:35:00-05:00",
    }


def test_notional_above_cap_is_blocked(monkeypatch):
    """notional_usd > cap 이면 risk gate가 BLOCKED 반환."""
    # cap = 50_000_000 / 1450 ≈ 34482.75
    _setup_env(monkeypatch, max_krw="50000000", fx="1450", max_order="36000")

    import trader.us.db.repos as repos
    import trader.us.execution.order_router as router

    repos.reset_memory_stores()
    router._SENT_ORDER_KEYS.clear()

    intent = _make_intent("AMZN", notional_usd=35000.0, qty=1)
    result = router.route_order(intent)
    assert result["status"] == "BLOCKED", f"예산 초과인데 BLOCKED 아님: {result}"


def test_notional_below_cap_passes(monkeypatch):
    """notional_usd ≤ cap 이면 DRY_RUN으로 통과."""
    _setup_env(monkeypatch, max_krw="50000000", fx="1450", max_order="36000")

    import trader.us.db.repos as repos
    import trader.us.execution.order_router as router

    repos.reset_memory_stores()
    router._SENT_ORDER_KEYS.clear()

    intent = _make_intent("NVDA", notional_usd=100.0, qty=1)
    result = router.route_order(intent)
    assert result["status"] in ("DRY_RUN", "BLOCKED"), f"예상치 못한 status: {result}"
    # 같은 symbol을 두 번 보내지 않았으니 DRY_RUN이어야 함
    assert result["status"] == "DRY_RUN", f"소액 BUY가 DRY_RUN 아님: {result}"
