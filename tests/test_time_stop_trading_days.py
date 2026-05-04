from trader.exit_policy.router import apply_swing_exit_decision, resolve_exit_policy_for_position


def _pos() -> dict:
    return {
        "code": "005930",
        "avg_buy_price": 10000.0,
        "qty": 10,
        "orderable_qty": 10,
        "entry_style_selected": "ENTRY_PULLBACK",
        "exit_policy_family": "SWING_STAGED_EXIT",
        "position_meta": {"initial_stop_price": 9500.0},
    }


def test_time_stop_uses_trading_days_not_calendar_days(monkeypatch):
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    monkeypatch.setenv("PB1_SWING_TIME_STOP_DAYS", "10")

    pos = _pos()
    policy = resolve_exit_policy_for_position(
        pos=pos,
        features={},
        holding_ctx={
            "days_held": 8,
            "calendar_days_held": 12,
            "trading_days_held": 8,
            "holding_bars": 7,
            "legacy_time_stop_hit": False,
            "current_return_pct": 1.0,
            "current_r": 0.5,
            "highest_return_pct": 4.0,
            "mark": 10100.0,
        },
        market_ctx={},
    )

    result = apply_swing_exit_decision(
        pos | {"calendar_days_held": 12, "trading_days_held": 8, "holding_bars": 7, "legacy_time_stop_hit": False},
        10100.0,
        policy,
        ret_pct=1.0,
        current_r=0.5,
        highest_ret_pct=4.0,
        days_held=8,
        stop_hit=False,
        effective_stop=9500.0,
        effective_r=500.0,
    )

    assert result["exit_ok"] is False
    assert result["reason"] != "EXIT_SWING_TIME_STOP"


def test_time_stop_allows_exit_when_trading_days_reached(monkeypatch):
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    monkeypatch.setenv("PB1_SWING_TIME_STOP_DAYS", "10")

    pos = _pos()
    policy = resolve_exit_policy_for_position(
        pos=pos,
        features={},
        holding_ctx={
            "days_held": 11,
            "calendar_days_held": 12,
            "trading_days_held": 11,
            "holding_bars": 10,
            "legacy_time_stop_hit": True,
            "current_return_pct": 1.0,
            "current_r": 0.5,
            "highest_return_pct": 4.0,
            "mark": 10100.0,
        },
        market_ctx={},
    )

    result = apply_swing_exit_decision(
        pos | {"calendar_days_held": 12, "trading_days_held": 11, "holding_bars": 10, "legacy_time_stop_hit": True},
        10100.0,
        policy,
        ret_pct=1.0,
        current_r=0.5,
        highest_ret_pct=4.0,
        days_held=11,
        stop_hit=False,
        effective_stop=9500.0,
        effective_r=500.0,
    )

    assert result["exit_ok"] is True
    assert result["reason"] == "EXIT_SWING_TIME_STOP"