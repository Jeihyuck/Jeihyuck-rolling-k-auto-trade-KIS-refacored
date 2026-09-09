def test_soft_stop_repeat_is_blocked_for_general_pb1():
    from trader.us.pb1.us_exit_engine import soft_stop_repeat_allowed

    allowed, reason = soft_stop_repeat_allowed({
        "symbol": "TEST", "entry_price": 100.0, "current_price": 94.0,
        "soft_stop_triggered_today": True, "first_soft_stop_price": 94.0,
    })

    assert allowed is False
    assert reason == "SOFT_STOP_REPEAT_BLOCKED"


def test_soft_stop_repeat_allows_risk_escalation_and_tqqq_bypass():
    from trader.us.pb1.us_exit_engine import soft_stop_repeat_allowed

    allowed, reason = soft_stop_repeat_allowed({
        "symbol": "TEST", "entry_price": 100.0, "current_price": 91.0,
        "soft_stop_triggered_today": True, "first_soft_stop_price": 94.0,
    })
    assert allowed is True
    assert reason == "SOFT_STOP_REPEAT_ALLOWED_BY_RISK_ESCALATION"

    allowed, reason = soft_stop_repeat_allowed({
        "symbol": "TQQQ", "soft_stop_triggered_today": True,
    })
    assert allowed is True
    assert reason == "TQQQ_INFINITE_OVERLAY_BYPASS"


def test_confirmed_soft_stop_fill_state_reaches_repeat_gate(monkeypatch):
    from trader.us.db import repos
    from trader.us.pb1.us_exit_engine import prepare_exit_position_snapshots, soft_stop_repeat_allowed
    from trader.us.runner.trade_tick_runner import _mark_soft_stop_stages_from_records

    store = {
        "trade_date": "2026-09-08", "symbol": "ABBV",
        "soft_stop_breach_count": 2,
        "state": {"lifecycle": {"lifecycle_id": "life-abbv", "is_open": True}},
    }

    def load_state(symbol, trade_date):
        return dict(store)

    def save_state(symbol, trade_date, state):
        store.clear()
        store.update(state)

    monkeypatch.setattr(repos, "load_us_position_risk_state", load_state)
    monkeypatch.setattr(repos, "save_us_position_risk_state", save_state)

    _mark_soft_stop_stages_from_records([{
        "symbol": "ABBV", "side": "SELL", "qty": 6, "price": 251.0,
        "order_no": "57", "client_order_key": "abbv-soft",
        "meta": {
            "exit_reason": "soft_stop_loss",
            "position_lifecycle_id": "life-abbv",
        },
    }], trade_date="2026-09-08", status="FILLED")

    soft_state = store["state"]["soft_stop_execution"]
    assert soft_state["soft_stop_triggered_today"] is True
    assert soft_state["soft_stop_partial_done"] is True
    assert soft_state["first_soft_stop_price"] == 251.0

    monkeypatch.setattr(
        repos,
        "update_us_soft_stop_risk_state",
        lambda **kwargs: {
            **store,
            "soft_stop_breach_count": 3,
        },
    )
    monkeypatch.setattr(
        "trader.us.position_lifecycle_state.update_us_position_high_watermark",
        lambda **kwargs: {"high_watermark": 264.91, "lifecycle_id": "life-abbv"},
    )

    class Provider:
        def get_current_price(self, symbol, exchange):
            return {"last": 251.0}

    snapshots = prepare_exit_position_snapshots([{
        "symbol": "ABBV", "exchange": "NYSE", "qty": 7, "holding_qty": 7,
        "orderable_qty": 7, "entry_price": 264.91,
        "position_lifecycle_id": "life-abbv",
    }], Provider())
    assert snapshots[0]["soft_stop_triggered_today"] is True
    allowed, reason = soft_stop_repeat_allowed(snapshots[0])
    assert allowed is False
    assert reason == "SOFT_STOP_REPEAT_BLOCKED"
