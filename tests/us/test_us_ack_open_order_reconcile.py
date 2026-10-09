from trader.us.execution import reconcile


def _order():
    return {
        "symbol": "NVDA", "side": "SELL", "order_no": "N1", "client_order_key": "NVDA-SELL",
        "qty_requested": 1, "pre_order_position_qty": 1, "limit_price": 180,
    }


def test_ack_zero_fill_with_orderable_reservation_is_open_order_pending(monkeypatch, caplog):
    caplog.set_level("INFO")
    monkeypatch.setattr("trader.us.db.repos.load_pending_ack_orders", lambda **_kwargs: [_order()])

    class Provider:
        def get_balance(self):
            return {"positions": [{"symbol": "NVDA", "qty": 1, "orderable_qty": 0, "avg_price": 170}]}
        def get_fills_by_order_no(self, **_kwargs):
            return {"filled_qty": 0, "remaining_qty": 1}

    result = reconcile.reconcile_ack_orders_with_balance(provider=Provider(), trade_date="2026-08-24")

    assert result["open_order_pending_count"] == 1
    assert result["unresolved_error_count"] == result["unresolved_count"] == 0
    assert result["manual_reconcile_required"] == 0
    assert result["symbols_by_status"]["open_order_pending"] == ["NVDA"]
    assert result["order_nos_by_status"]["open_order_pending"] == ["N1"]
    assert "[US_RECONCILE][ACK_OPEN_ORDER_PENDING]" in caplog.text


def test_ack_without_fill_open_or_reservation_is_true_unresolved(monkeypatch, caplog):
    monkeypatch.setattr("trader.us.db.repos.load_pending_ack_orders", lambda **_kwargs: [_order()])

    class Provider:
        def get_balance(self):
            return {"positions": [{"symbol": "NVDA", "qty": 1, "orderable_qty": 1, "avg_price": 170}]}
        def get_fills_by_order_no(self, **_kwargs):
            return {"filled_qty": 0}

    result = reconcile.reconcile_ack_orders_with_balance(provider=Provider(), trade_date="2026-08-24")

    assert result["open_order_pending_count"] == 0
    assert result["unresolved_error_count"] == result["unresolved_count"] == 1
    assert result["manual_reconcile_required"] == 1
    assert result["symbols_by_status"]["unresolved_error"] == ["NVDA"]
    assert "[US_RECONCILE][ACK_UNRESOLVED_ERROR]" in caplog.text



def test_ack_broker_fill_budget_exhaustion_remains_unknown_and_fenced(monkeypatch, caplog):
    """A KIS deadline is not proof of zero fill or lack of broker open orders."""
    caplog.set_level("INFO")
    monkeypatch.setattr("trader.us.db.repos.load_pending_ack_orders", lambda **_kwargs: [_order()])

    class Provider:
        def get_balance(self):
            raise RuntimeError("balance stage budget exhausted before fetch")

        def get_fills_by_order_no(self, **_kwargs):
            raise RuntimeError("tick deadline budget exhausted before KIS request")

    result = reconcile.reconcile_ack_orders_with_balance(
        provider=Provider(), trade_date="2026-08-24",
    )

    assert result["unresolved_count"] == 1
    assert result["manual_reconcile_required"] == 1
    assert result["symbols_by_status"]["unresolved_error"] == ["NVDA"]
    assert "reason=broker_fill_query_unavailable" in caplog.text
    assert "filled_qty=unknown" in caplog.text
    assert "reason=no_fill_no_open_order_no_balance_delta" not in caplog.text
