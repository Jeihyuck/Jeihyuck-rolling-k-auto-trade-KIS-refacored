from __future__ import annotations


class _BuyProvider:
    def get_balance(self) -> dict:
        return {"positions": [{"symbol": "AMD", "qty": 5, "avg_price": 123.45}]}

    def get_fills_by_order_no(self, order_no: str, symbol: str, trade_date: str) -> dict:
        return {}


def test_buy_ack_reconciles_from_kis_balance(monkeypatch) -> None:
    import trader.us.execution.reconcile as reconcile
    import trader.us.db.repos as repos

    monkeypatch.setattr(repos, "load_pending_ack_orders", lambda trade_date, env="practice": [{
        "symbol": "AMD",
        "side": "BUY",
        "order_no": "B1",
        "client_order_key": "CKB1",
        "qty_requested": 5,
        "meta": {"pre_order_position_qty": 0, "pre_order_position_source": "db_position_absent"},
    }])
    captured: list[dict] = []
    monkeypatch.setattr(repos, "mark_order_filled_by_reconcile", lambda **kwargs: (captured.append(kwargs) or {"status": "OK"}))

    result = reconcile.reconcile_ack_orders_with_balance(
        provider=_BuyProvider(),
        trade_date="2026-06-12",
        env="practice",
    )

    assert result["balance_reconcile_count"] == 1
    assert result["unresolved_count"] == 0
    assert captured[0]["symbol"] == "AMD"
    assert captured[0]["side"] == "BUY"
    assert captured[0]["source"] == "balance_reconcile_buy"
    assert captured[0]["trade_date"] == "2026-06-12"


class _ExistingHoldingProvider:
    def get_balance(self) -> dict:
        return {"positions": [{"symbol": "AMD", "qty": 5, "avg_price": 123.45}]}

    def get_fills_by_order_no(self, order_no: str, symbol: str, trade_date: str) -> dict:
        return {}


def test_buy_ack_balance_reconcile_skips_when_snapshot_missing_for_existing_holding(monkeypatch) -> None:
    import trader.us.execution.reconcile as reconcile
    import trader.us.db.repos as repos

    monkeypatch.setattr(repos, "load_pending_ack_orders", lambda trade_date, env="practice": [{
        "symbol": "AMD",
        "side": "BUY",
        "order_no": "B2",
        "client_order_key": "CKB2",
        "qty_requested": 5,
        "meta": {"pre_order_position_source": "lookup_failed"},
    }])
    captured: list[dict] = []
    monkeypatch.setattr(repos, "mark_order_filled_by_reconcile", lambda **kwargs: (captured.append(kwargs) or {"status": "OK"}))

    result = reconcile.reconcile_ack_orders_with_balance(
        provider=_ExistingHoldingProvider(),
        trade_date="2026-06-12",
        env="practice",
    )

    assert result["balance_reconcile_count"] == 0
    assert result["unresolved_count"] == 1
    assert captured == []
