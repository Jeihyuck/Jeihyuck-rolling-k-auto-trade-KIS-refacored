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
