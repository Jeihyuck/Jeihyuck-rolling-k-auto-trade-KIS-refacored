import pytest

from trader.exit_policy.router import resolve_exit_policy_for_position


@pytest.mark.parametrize("code", ["067290", "095340"])
def test_explicit_policy_missing_never_falls_through_to_swing(code):
    policy = resolve_exit_policy_for_position(
        {"code": code, "exit_policy_family": "POLICY_MISSING"},
        {}, {"current_return_pct": 40, "current_r": 5, "days_held": 100}, {},
    )
    assert policy["exit_family"] == "POLICY_MISSING"
    assert policy["hard_stop_enabled"] is True
    assert policy["partial_sell_rules"] == []
    assert policy["full_exit_rules"] == []
    for key in ("r_take_profit_enabled", "percent_take_profit_enabled",
                "profit_protect_enabled", "trend_follow_enabled", "time_stop_enabled"):
        assert policy[key] is False
