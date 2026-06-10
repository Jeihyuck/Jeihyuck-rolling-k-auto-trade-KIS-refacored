import pytest

from trader.trade_plan import build_entry_exit_plan, classify_close_action_from_plan, parse_plan_bool


def _plan(style):
    return build_entry_exit_plan(
        code="005930",
        market="KOSPI",
        entry_style_selected=style,
        entry_reason=style,
        entry_price=10000,
        features={"stop_price": 9500},
    ).to_dict()


def test_entry_breakout_day_trade_plan():
    plan = _plan("ENTRY_BREAKOUT")
    assert plan["trade_horizon"] == "DAY_TRADE"
    assert plan["exit_policy_family"] == "INTRADAY_PROFIT_PROTECT"
    assert plan["eod_action"] == "FORCE_EXIT"
    assert plan["force_eod_close"] is True


def test_entry_pullback_swing_plan():
    plan = _plan("ENTRY_PULLBACK")
    assert plan["trade_horizon"] == "SWING"
    assert plan["exit_policy_family"] == "SWING_STAGED_EXIT"
    assert plan["eod_action"] == "CARRY_IF_NO_EXIT_SIGNAL"
    assert plan["force_eod_close"] is False


def test_entry_core_plan():
    plan = _plan("ENTRY_CORE")
    assert plan["trade_horizon"] == "CORE"
    assert plan["exit_policy_family"] == "CORE_TREND_FOLLOW"


def test_unknown_entry_style_rejected():
    with pytest.raises(ValueError):
        _plan("ENTRY_UNKNOWN")


def test_day_trade_close_force_sell():
    assert classify_close_action_from_plan(_plan("ENTRY_BREAKOUT")) == ("FORCE_SELL", "EOD_FORCE_EXIT")


def test_swing_close_carry():
    assert classify_close_action_from_plan(_plan("ENTRY_PULLBACK")) == ("CARRY", "SWING_CARRY")


def test_empty_plan_policy_missing():
    assert classify_close_action_from_plan({}) == ("SKIP", "POLICY_MISSING")


def test_force_eod_close_string_false_is_false():
    assert parse_plan_bool("False") is False
    assert parse_plan_bool("0") is False
    assert parse_plan_bool("true") is True
    plan = build_entry_exit_plan(
        code="005930", market="KOSPI", entry_style_selected="ENTRY_PULLBACK", entry_reason="ENTRY_PULLBACK", entry_price=10000,
        features={"stop_price": 9500, "force_eod_close": "False"},
    ).to_dict()
    assert plan["force_eod_close"] is False
