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
