from trader.trade_plan import build_entry_exit_plan, classify_close_action_from_plan


def _plan(style):
    return build_entry_exit_plan(code="005930", market="KOSPI", entry_style_selected=style, entry_reason=style, entry_price=10000, features={"stop_price": 9500}).to_dict()


def test_day_trade_force_exit_sell_intent():
    assert classify_close_action_from_plan(_plan("ENTRY_BREAKOUT"))[0] == "FORCE_SELL"


def test_swing_carry_no_sell_without_signal():
    assert classify_close_action_from_plan(_plan("ENTRY_PULLBACK")) == ("CARRY", "SWING_CARRY")


def test_swing_stop_signal_can_still_sell():
    action, reason = classify_close_action_from_plan(_plan("ENTRY_PULLBACK"))
    stop_hit = True
    assert action == "CARRY" and stop_hit


def test_policy_missing_skip():
    assert classify_close_action_from_plan({}) == ("SKIP", "POLICY_MISSING")
