from trader.db.repos import _plan_position_values
from trader.trade_plan import build_entry_exit_plan, classify_close_action_from_plan


def _record():
    plan = build_entry_exit_plan(code="005930", market="KOSPI", entry_style_selected="ENTRY_PULLBACK", entry_reason="ENTRY_PULLBACK", entry_price=10000, features={"stop_price": 9500}).to_dict()
    return {"entry_exit_plan": plan, "entry_meta": {"source": "test"}}


def test_restore_values_from_latest_buy_fill_plan():
    values = _plan_position_values(_record())
    assert values["trade_horizon"] == "SWING"
    assert values["entry_exit_plan_json"]["exit_policy_family"] == "SWING_STAGED_EXIT"


def test_restore_values_from_latest_buy_order_plan():
    values = _plan_position_values(_record())
    assert values["exit_policy_family"] == "SWING_STAGED_EXIT"


def test_restore_values_policy_missing_when_no_plan():
    values = _plan_position_values(None, missing=True)
    assert values["entry_thesis"] == "POLICY_MISSING"
    assert values["exit_policy_family"] == "POLICY_MISSING"
    assert values["force_eod_close"] is False


def test_policy_missing_not_force_sold():
    assert classify_close_action_from_plan({}) == ("SKIP", "POLICY_MISSING")
