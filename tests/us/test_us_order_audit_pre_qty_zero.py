from trader.us.execution.order_journal import append_order_event


def test_new_buy_authoritative_zero_is_preserved_in_journal(monkeypatch, tmp_path):
    monkeypatch.setenv("US_ORDER_JOURNAL_DIR", str(tmp_path))
    monkeypatch.setattr("trader.us.db.repos.append_us_order_event", lambda event: True)
    event = append_order_event("BROKER_SUBMIT_STARTED", {"trade_date": "2026-08-31", "symbol": "JNJ", "side": "BUY",
        "qty": 6, "limit_price": 266.66, "notional_usd": 1599.96,
        "client_order_key": "jnj", "pre_order_holding_qty": 0,
        "pre_order_position_qty": 0, "meta": {"pre_order_holding_qty": 0}})
    assert event["pre_order_holding_qty"] == 0
    assert event["pre_order_position_qty"] == 0
