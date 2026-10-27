import pytest

from trader.trade_plan import build_entry_exit_plan


@pytest.mark.parametrize("style,bad_horizon", [
    ("ENTRY_MOMENTUM", "SWING"),
    ("ENTRY_BREAKOUT", "SWING"),
    ("ENTRY_PULLBACK", "DAY_TRADE"),
])
def test_kr_frozen_exit_owner_blocks_conflicting_horizon(style, bad_horizon):
    with pytest.raises(ValueError, match="KR_ENTRY_EXIT_STYLE_CONFLICT"):
        build_entry_exit_plan(code="005930", market="KOSPI", entry_style_selected=style,
                              entry_reason=style, entry_price=10000,
                              features={"stop_price": 9500, "trade_horizon": bad_horizon})


def test_kr_frozen_exit_owner_blocks_forged_eod():
    with pytest.raises(ValueError, match="KR_ENTRY_EXIT_STYLE_CONFLICT"):
        build_entry_exit_plan(code="005930", market="J", entry_style_selected="ENTRY_MOMENTUM",
                              entry_reason="ENTRY_MOMENTUM", entry_price=10000,
                              features={"stop_price": 9500, "force_eod_close": False})


def test_kr_momentum_preserves_current_day_trade_policy():
    p = build_entry_exit_plan(code="005930", market="KOSPI",
                              entry_style_selected="ENTRY_MOMENTUM",
                              entry_reason="ENTRY_MOMENTUM", entry_price=10000,
                              features={"stop_price": 9500})
    assert p.trade_horizon == "DAY_TRADE"
    assert p.exit_policy_family == "INTRADAY_PROFIT_PROTECT"


def test_kr_pullback_keeps_swing_without_new_policy():
    p = build_entry_exit_plan(code="005930", market="KOSPI",
                              entry_style_selected="ENTRY_PULLBACK",
                              entry_reason="ENTRY_PULLBACK", entry_price=10000,
                              features={"stop_price": 9500})
    assert p.trade_horizon == "SWING"
    assert p.exit_policy_family == "SWING_STAGED_EXIT"


def test_legacy_us_custom_plan_not_changed():
    p = build_entry_exit_plan(code="AAPL", market="NAS",
                              entry_style_selected="ENTRY_MOMENTUM",
                              entry_reason="ENTRY_MOMENTUM", entry_price=100,
                              features={"stop_price": 95, "trade_horizon": "SWING",
                                        "exit_policy_family": "SWING_STAGED_EXIT",
                                        "eod_action": "CARRY_IF_NO_EXIT_SIGNAL",
                                        "force_eod_close": False})
    assert p.trade_horizon == "SWING"


@pytest.mark.parametrize("style", ["ENTRY_MOMENTUM_SWING", "ENTRY_BREAKOUT_SWING"])
def test_explicit_new_trend_swing_contract_carries_only_when_no_exit_signal(style):
    from trader.trade_plan import classify_close_action_from_plan
    plan = build_entry_exit_plan(code="005930", market="J",
                                 entry_style_selected=style,
                                 entry_reason="ENTRY_MOMENTUM" if "MOMENTUM" in style else "ENTRY_BREAKOUT",
                                 entry_price=10000, features={"stop_price": 9500})
    assert plan.trade_horizon == "SWING"
    assert plan.exit_policy_family == "SWING_STAGED_EXIT"
    assert plan.eod_action == "CARRY_IF_NO_EXIT_SIGNAL"
    assert plan.force_eod_close is False
    assert classify_close_action_from_plan(plan.to_dict()) == ("CARRY", "SWING_CARRY")


@pytest.mark.parametrize("style", ["ENTRY_MOMENTUM", "ENTRY_BREAKOUT"])
def test_original_day_contract_still_eod_exits(style):
    from trader.trade_plan import classify_close_action_from_plan
    plan = build_entry_exit_plan(code="005930", market="J",
                                 entry_style_selected=style,
                                 entry_reason=style, entry_price=10000,
                                 features={"stop_price": 9500})
    assert classify_close_action_from_plan(plan.to_dict()) == ("FORCE_SELL", "EOD_FORCE_EXIT")


def test_existing_frozen_position_horizon_not_promoted():
    from trader.pb1_engine import _resolve_position_horizon
    old = build_entry_exit_plan(code="005930", market="KR", entry_style_selected="MOMENTUM",
                                entry_reason="MOMENTUM", entry_price=10000,
                                features={"stop_price": 9700})
    new = build_entry_exit_plan(code="005930", market="KR", entry_style_selected="ENTRY_MOMENTUM_SWING",
                                entry_reason="MOMENTUM", entry_price=10000,
                                features={"stop_price": 9700})
    assert _resolve_position_horizon({"entry_exit_plan_json": old.to_dict()}) == "DAY_PROTECT"
    assert _resolve_position_horizon({"entry_exit_plan_json": new.to_dict()}) == "SWING_CARRY"
