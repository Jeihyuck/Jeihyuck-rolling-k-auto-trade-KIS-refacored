from trader.kr.entry_identity_proof import entry_style_identity_proof


def test_breakout_trigger_does_not_promote_momentum_to_breakout_owner():
    value = entry_style_identity_proof(
        selected_style="MOMENTUM", trigger_ok=True,
        decision_family="ENTRY_BREAKOUT_CONFIRMED", trigger_policy="BREAKOUT_CONFIRMED")
    assert value["state"] == "VERIFIED"
    assert value["selected_style"] == "ENTRY_MOMENTUM"
    assert value["trigger_event"] == "BREAKOUT_PRICE_CONFIRMED"
    assert value["contract_trade_horizon"] == "DAY_TRADE"
    assert value["contract_exit_policy_family"] == "INTRADAY_PROFIT_PROTECT"


def test_pullback_override_keeps_swing_owner():
    value = entry_style_identity_proof(
        selected_style="PULLBACK", trigger_ok=False,
        decision_family="ENTRY_PULLBACK_OVERRIDE", trigger_policy="PULLBACK_OVERRIDE")
    assert value["contract_trade_horizon"] == "SWING"
    assert value["contract_eod_action"] == "CARRY_IF_NO_EXIT_SIGNAL"


def test_unrecognized_style_never_infers_swing_or_day():
    proof = entry_style_identity_proof(
        selected_style="UNKNOWN", trigger_ok=True,
        decision_family="ENTRY_BREAKOUT_CONFIRMED", trigger_policy="BREAKOUT_CONFIRMED")
    assert proof["state"] == "UNKNOWN"
    assert "contract_trade_horizon" not in proof
