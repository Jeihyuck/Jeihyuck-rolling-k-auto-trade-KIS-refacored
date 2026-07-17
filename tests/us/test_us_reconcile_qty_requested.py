from __future__ import annotations

import logging


class _Provider:
    def get_balance(self) -> dict:
        return {"positions": []}

    def get_fills_by_order_no(self, order_no: str, symbol: str) -> dict:
        return {}


def test_qty_requested_used_for_sell_balance_reconcile(monkeypatch) -> None:
    import trader.us.execution.reconcile as reconcile
    import trader.us.db.repos as repos

    monkeypatch.setattr(repos, "load_pending_ack_orders", lambda trade_date, env="practice": [{
        "symbol": "CIEN",
        "side": "SELL",
        "order_no": "O1",
        "client_order_key": "CK1",
        "qty_requested": 4,
        "avg_price_usd": 500.6966,
    }])

    captured: list[dict] = []
    monkeypatch.setattr(
        repos,
        "mark_order_filled_by_reconcile",
        lambda **kwargs: captured.append(kwargs),
    )

    result = reconcile.reconcile_ack_orders_with_balance(
        provider=_Provider(),
        trade_date="2026-06-05",
        env="practice",
    )

    # Symbol absence alone is not quantity evidence; no synthetic full fill.
    assert result["balance_reconcile_count"] == 0
    assert result["unresolved_count"] == 1
    assert captured == []


def test_missing_qty_requested_is_marked_unresolved(monkeypatch, caplog) -> None:
    import trader.us.execution.reconcile as reconcile
    import trader.us.db.repos as repos

    monkeypatch.setattr(repos, "load_pending_ack_orders", lambda trade_date, env="practice": [{
        "symbol": "CIEN",
        "side": "SELL",
        "order_no": "O2",
        "client_order_key": "CK2",
    }])

    called = {"value": False}

    def _mark(**kwargs):
        called["value"] = True

    monkeypatch.setattr(repos, "mark_order_filled_by_reconcile", _mark)

    with caplog.at_level(logging.WARNING):
        result = reconcile.reconcile_ack_orders_with_balance(
            provider=_Provider(),
            trade_date="2026-06-05",
            env="practice",
        )

    assert result["unresolved_count"] == 1
    assert called["value"] is False
    assert "[US_RECONCILE][ACK_RECONCILE][INVALID_QTY]" in caplog.text
