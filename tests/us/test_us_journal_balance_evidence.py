from trader.us.execution.order_journal import append_order_event, load_order_events


def test_journal_records_balance_delta_evidence(tmp_path, monkeypatch):
    monkeypatch.setenv("US_ORDER_JOURNAL_DIR", str(tmp_path))
    append_order_event("BROKER_SUBMIT_STARTED", {"trade_date":"2026-07-16","client_order_key":"K","symbol":"AMD","side":"SELL","qty":3,"pre_order_position_qty":10,"position_lifecycle_id":"L1","limit_price":100,"notional_usd":300})
    event=load_order_events("2026-07-16")[0]
    assert event["pre_order_position_qty"] == 10
    assert event["position_lifecycle_id"] == "L1"
    assert event["limit_price"] == 100 and event["notional_usd"] == 300
