from __future__ import annotations


def test_reconcile_ack_orders_confirms_fill_api_and_balance_delta(monkeypatch):
    from trader.us.execution import reconcile

    orders = [
        {"symbol": "B1", "side": "BUY", "order_no": "b1", "qty_requested": 1, "pre_order_position_qty": 0, "limit_price": 10},
        {"symbol": "B2", "side": "BUY", "order_no": "b2", "qty_requested": 1, "pre_order_position_qty": 0, "limit_price": 20},
        *[
            {"symbol": f"S{i}", "side": "SELL", "order_no": f"s{i}", "qty_requested": 1, "pre_order_position_qty": 1, "limit_price": 30 + i}
            for i in range(1, 5)
        ],
    ]
    monkeypatch.setattr("trader.us.db.repos.load_pending_ack_orders", lambda trade_date, env="practice": orders)
    marked = []
    monkeypatch.setattr("trader.us.db.repos.mark_order_filled_by_reconcile", lambda **kwargs: marked.append(kwargs))

    class _Provider:
        def get_balance(self):
            return {"positions": [{"symbol": "B1", "qty": 1, "avg_price": 10}, {"symbol": "B2", "qty": 1, "avg_price": 20}]}
        def get_fills_by_order_no(self, order_no, symbol):
            if order_no.startswith("s"):
                return {"filled_qty": 1, "avg_price": 30}
            return {"filled_qty": 0}

    result = reconcile.reconcile_ack_orders_with_balance(provider=_Provider(), trade_date="2026-06-26", env="practice")

    assert result["confirmed_count"] == 4
    assert result["balance_reconcile_count"] == 2
    assert result["unresolved_count"] == 0
    assert len(result["symbols_by_status"]["fill_api_confirmed"]) == 4
    assert set(result["symbols_by_status"]["balance_confirmed"]) == {"B1", "B2"}
    assert result["symbols_by_status"].get("unresolved", []) == []
    assert len(marked) == 6
    assert sum(1 for call in marked if call["source"] == "fills_reconcile") == 4
    assert sum(1 for call in marked if call["source"] == "balance_reconcile_buy") == 2


def test_buy_balance_reconcile_requires_pre_order_position_qty(monkeypatch):
    from trader.us.execution import reconcile

    orders = [{"symbol": "B1", "side": "BUY", "order_no": "b1", "qty_requested": 1, "limit_price": 10}]
    monkeypatch.setattr("trader.us.db.repos.load_pending_ack_orders", lambda trade_date, env="practice": orders)
    marked = []
    monkeypatch.setattr("trader.us.db.repos.mark_order_filled_by_reconcile", lambda **kwargs: marked.append(kwargs))

    class _Provider:
        def get_balance(self):
            return {"positions": [{"symbol": "B1", "qty": 1, "avg_price": 10}]}
        def get_fills_by_order_no(self, order_no, symbol):
            return {"filled_qty": 0}

    result = reconcile.reconcile_ack_orders_with_balance(provider=_Provider(), trade_date="2026-06-26", env="practice")

    assert result["confirmed_count"] == 0
    assert result["balance_reconcile_count"] == 0
    assert result["unresolved_count"] == 1
    assert result["symbols_by_status"]["unresolved"] == ["B1"]
    assert marked == []


def test_close_final_balance_delta_classifies_pending_out_of_report():
    from trader.us.execution.reconcile import classify_ack_orders_with_final_balance

    orders = [
        {"symbol": "B1", "side": "BUY", "order_no": "b1", "qty_requested": 1, "pre_order_position_qty": 0, "status": "ACK"},
        {"symbol": "S1", "side": "SELL", "order_no": "s1", "qty_requested": 1, "pre_order_position_qty": 1, "status": "ACK"},
        {"symbol": "U1", "side": "BUY", "order_no": "u1", "qty_requested": 1, "pre_order_position_qty": 0, "status": "ACK"},
    ]

    class _Provider:
        def get_balance(self):
            return {"positions": [{"symbol": "B1", "qty": 1, "avg_price": 10}]}

    result = classify_ack_orders_with_final_balance(provider=_Provider(), trade_date="2026-06-26", orders=orders)
    by_symbol = {row["symbol"]: row for row in result["orders"]}
    assert by_symbol["B1"]["final_status"] == "balance_delta_confirmed"
    assert by_symbol["S1"]["final_status"] == "balance_delta_confirmed"
    assert by_symbol["U1"]["final_status"] == "ack_only_unresolved"
    assert result["pending_order_count"] == 1
