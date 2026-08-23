from trader.us.market_state_overlay import build_defense_trim_intents
from trader.us.execution.order_journal import append_order_event


def test_buy_journal_audit_reason_fields_are_complete(monkeypatch, tmp_path):
    monkeypatch.setenv("US_ORDER_JOURNAL_DIR", str(tmp_path))
    monkeypatch.setattr("trader.us.db.repos.append_us_order_event", lambda event: True)
    required = {"rank_final30": 2, "score_final": .5653, "theme_cluster": "HEALTHCARE",
                "market_state": "DEFENSE_RISK_OFF", "risk_gate_result": "PASS"}
    event = append_order_event("BROKER_SUBMIT_STARTED", {
        "trade_date": "2026-08-21", "symbol": "MRK", "side": "BUY", "qty": 1,
        "submit_attempt_id": "BUY-MRK-1", "meta": required,
    })
    assert all(event["meta"].get(key) is not None for key in required)


def test_defense_sell_journal_has_top_level_reason_evidence(monkeypatch, tmp_path):
    monkeypatch.setenv("US_ORDER_JOURNAL_DIR", str(tmp_path))
    monkeypatch.setattr("trader.us.db.repos.append_us_order_event", lambda event: True)
    monkeypatch.setattr("trader.us.db.repos.has_same_day_exit", lambda *args, **kwargs: False)
    intent = build_defense_trim_intents(
        [{"symbol": "MSFT", "qty": 4, "current_price": 483.55, "avg_price": 482.50,
          "theme_cluster": "AI_SEMICONDUCTOR", "position_lifecycle_id": "L1"}],
        {"market_state": "DEFENSE_RISK_OFF"}, trade_date="2026-08-21",
    )[0]
    intent["submit_attempt_id"] = "SELL-MSFT-1"
    event = append_order_event("BROKER_SUBMIT_STARTED", intent)
    required = ("broker_avg_price", "entry_price", "decision_price", "return_rate_at_decision",
                "expected_realized_pnl", "exit_family")
    assert all(event.get(key) is not None for key in required)
