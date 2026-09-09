from trader.us.runner.trade_session_runner import (
    attribute_session_fills,
    count_confirmed_broker_orders,
)


def test_session_fill_attribution_dedupes_pre_and_post_ack_events():
    orders = [
        {
            "session": "am", "session_run_id": "run-1", "event_type": "BROKER_SUBMIT_STARTED",
            "submit_attempt_id": "attempt-1", "client_order_key": "client-1",
            "symbol": "ABBV", "side": "SELL", "order_no": "",
        },
        {
            "session": "am", "session_run_id": "run-1", "event_type": "BROKER_ACK_RECEIVED",
            "submit_attempt_id": "attempt-1", "client_order_key": "client-1",
            "symbol": "ABBV", "side": "SELL", "order_no": "57",
        },
    ]
    fills = [{
        "submit_attempt_id": "attempt-1", "client_order_key": "client-1",
        "symbol": "ABBV", "side": "SELL", "order_no": "57", "qty": 2,
    }]
    result = attribute_session_fills(orders, fills, session="am", session_run_id="run-1")
    assert result == {"session_fills_count": 1, "unresolved_order_count": 0}


def test_confirmed_broker_order_count_ignores_cumulative_tick_repetition():
    events = [
        {"event_type": "BROKER_SUBMIT_STARTED", "submit_attempt_id": "a1", "client_order_key": "c1", "side": "SELL"},
        {"event_type": "BROKER_ACK_RECEIVED", "submit_attempt_id": "a1", "client_order_key": "c1", "side": "SELL"},
        {"event_type": "ORDER_FILLED", "submit_attempt_id": "a1", "client_order_key": "c1", "side": "SELL"},
        {"event_type": "BROKER_SUBMIT_STARTED", "submit_attempt_id": "a2", "client_order_key": "c2", "side": "SELL"},
        {"event_type": "BROKER_ACK_RECEIVED", "submit_attempt_id": "a2", "client_order_key": "c2", "side": "SELL"},
    ]
    assert count_confirmed_broker_orders(events) == (0, 2)
