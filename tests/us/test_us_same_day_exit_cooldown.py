from trader.us.market_state_overlay import build_defense_trim_intents


def test_ack_journal_event_blocks_same_day_defense_family(monkeypatch):
    event = {"event_type": "BROKER_ACK_RECEIVED", "trade_date": "2026-08-21",
             "symbol": "MSFT", "side": "SELL", "position_lifecycle_id": "L1",
             "meta": {"exit_family": "DEFENSE_RISK_OFF_TRIM"}}
    monkeypatch.setattr("trader.us.execution.order_journal.load_order_events", lambda trade_date: [event])
    overlay = {"market_state": "DEFENSE_RISK_OFF"}
    intents = build_defense_trim_intents(
        [{"symbol": "MSFT", "qty": 4, "current_price": 483.55, "avg_price": 482.50,
          "theme_cluster": "AI_SEMICONDUCTOR", "position_lifecycle_id": "L1"}],
        overlay, trade_date="2026-08-21",
    )
    assert intents == []
    assert overlay["same_day_exit_cooldown_blocked_count"] == 1
    assert overlay["same_day_exit_cooldown_blocked_symbols"] == ["MSFT"]
    assert overlay["same_day_exit_cooldown_blocked_reasons"] == ["DEFENSE_RISK_OFF_TRIM"]
