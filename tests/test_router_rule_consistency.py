from trader.exit_policy.router import apply_swing_exit_decision, resolve_exit_policy_for_position


def test_router_blocks_time_stop_when_legacy_rule_is_false(monkeypatch):
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    monkeypatch.setenv("PB1_SWING_TIME_STOP_DAYS", "10")

    pos = {
        "code": "005930",
        "avg_buy_price": 10000.0,
        "qty": 5,
        "orderable_qty": 5,
        "entry_style_selected": "ENTRY_PULLBACK",
        "exit_policy_family": "SWING_STAGED_EXIT",
        "position_meta": {"initial_stop_price": 9500.0},
        "calendar_days_held": 14,
        "trading_days_held": 11,
        "holding_bars": 9,
        "legacy_time_stop_hit": False,
    }
    policy = resolve_exit_policy_for_position(
        pos=pos,
        features={},
        holding_ctx={
            "days_held": 11,
            "calendar_days_held": 14,
            "trading_days_held": 11,
            "holding_bars": 9,
            "legacy_time_stop_hit": False,
            "current_return_pct": 1.0,
            "current_r": 0.4,
            "highest_return_pct": 3.0,
            "mark": 10100.0,
        },
        market_ctx={},
    )

    result = apply_swing_exit_decision(
        pos,
        10100.0,
        policy,
        ret_pct=1.0,
        current_r=0.4,
        highest_ret_pct=3.0,
        days_held=11,
        stop_hit=False,
        effective_stop=9500.0,
        effective_r=500.0,
    )

    assert result["exit_ok"] is False
    assert result["reason"] == "TIME_STOP_RULE_ROUTER_MISMATCH"