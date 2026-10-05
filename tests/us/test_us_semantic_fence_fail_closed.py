from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from trader.us.execution.order_router import route_order, same_day_semantic_sell_exists


def test_semantic_sell_ledger_lookup_failure_is_not_treated_as_no_prior_action(monkeypatch):
    def unavailable(*args, **kwargs):
        raise RuntimeError("ledger unavailable")

    monkeypatch.setattr("trader.us.db.repos.load_us_daily_orders_for_report", unavailable)
    intent = {
        "trade_date": "2026-10-02",
        "symbol": "AN_EXAMPLE_SYMBOL",
        "side": "SELL",
        "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "cycle-1",
        "reason": "PROFIT_CAPTURE_TP2",
        "meta": {"reason": "PROFIT_CAPTURE_TP2"},
    }
    with pytest.raises(RuntimeError, match="ledger unavailable"):
        same_day_semantic_sell_exists(intent)


def test_completed_tp1_does_not_fence_distinct_tp2_stage(monkeypatch):
    monkeypatch.setattr(
        "trader.us.db.repos.load_us_daily_orders_for_report",
        lambda *_args, **_kwargs: [{
            "symbol": "AN_EXAMPLE_SYMBOL",
            "side": "SELL",
            "status": "FILLED",
            "strategy_owner": "US_STANDARD",
            "position_lifecycle_id": "cycle-1",
            "reason": "PROFIT_CAPTURE_TP1",
            "meta": {"reason": "PROFIT_CAPTURE_TP1", "profit_capture_stage": "TP1"},
        }],
    )
    intent = {
        "trade_date": "2026-10-02",
        "symbol": "AN_EXAMPLE_SYMBOL",
        "side": "SELL",
        "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "cycle-1",
        "reason": "PROFIT_CAPTURE_TP2",
        "meta": {"reason": "PROFIT_CAPTURE_TP2", "profit_capture_stage": "TP2"},
    }
    assert not same_day_semantic_sell_exists(intent)


def test_route_surfaces_semantic_ledger_failure_without_broker_submit(monkeypatch):
    from trader.us.execution import order_router

    monkeypatch.setattr(
        order_router, "same_day_semantic_sell_exists",
        lambda _intent: (_ for _ in ()).throw(RuntimeError("ledger unavailable")),
    )
    monkeypatch.setattr(
        "trader.us.execution.order_identity.normalize_and_validate_order_identity",
        lambda intent, _context: intent,
    )
    broker = MagicMock()
    result = route_order(
        {
            "trade_date": "2026-10-02",
            "client_order_key": "test-semantic-ledger-fail",
            "symbol": "AN_EXAMPLE_SYMBOL",
            "exchange": "NASDAQ",
            "side": "SELL",
            "qty": 1,
            "limit_price": 10,
            "strategy_owner": "US_STANDARD",
            "position_lifecycle_id": "cycle-1",
            "reason": "PROFIT_CAPTURE_TP2",
            "meta": {"profit_capture_stage": "TP2"},
        },
        kis_client=broker,
    )
    assert result["status"] == "ORDER_DISABLED_DURABLE_LEDGER_UNAVAILABLE"
    assert result["broker_submit"] is False
    broker.place_us_sell_order.assert_not_called()


def test_route_fails_closed_when_atomic_submit_claim_is_unavailable(monkeypatch):
    from trader.execution_state import SemanticActionIdentity
    from trader.us.execution import order_router

    monkeypatch.setattr(order_router, "same_day_semantic_sell_exists", lambda _intent: False)
    monkeypatch.setattr(
        "trader.us.execution.order_identity.normalize_and_validate_order_identity",
        lambda intent, _context: intent,
    )
    monkeypatch.setattr(
        "trader.us.db.repos.claim_execution_action",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("claim store unavailable")),
    )
    monkeypatch.setattr(
        order_router, "_semantic_action_identity",
        lambda *_args, **_kwargs: SemanticActionIdentity(
            env="practice", account_id="test", market="US",
            trading_epoch_id="epoch-test", strategy_owner="US_STANDARD",
            lifecycle_id="cycle-test", action="ENTRY",
        ),
    )
    monkeypatch.setattr(order_router, "canonical_order_risk_check", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("trader.us.execution.order_router.resolve_dry_run_for_us_order", lambda: False)
    monkeypatch.setattr("trader.us.db.repos.save_order_intent", lambda *_args, **_kwargs: True)
    monkeypatch.setattr("trader.us.db.repos.load_today_order_keys", lambda **_kwargs: set())
    broker = MagicMock()
    result = route_order(
        {
            "trade_date": "2026-10-02",
            "client_order_key": "test-claim-store-fail",
            "symbol": "AN_EXAMPLE_SYMBOL",
            "exchange": "NASDAQ",
            "side": "BUY",
            "qty": 1,
            "limit_price": 10,
            "notional_usd": 10,
            "strategy_owner": "US_STANDARD",
            "meta": {},
        },
        kis_client=broker,
    )
    assert result["status"] == "ORDER_DISABLED_DURABLE_LEDGER_UNAVAILABLE"
    assert result["reason"] == "execution_claim_unavailable"
    assert result["broker_submit"] is False
    broker.place_us_buy_order.assert_not_called()
