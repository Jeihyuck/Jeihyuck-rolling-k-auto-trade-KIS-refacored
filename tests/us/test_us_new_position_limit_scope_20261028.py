"""Regression: 5-per-Tick is not proof of 5 cumulative new symbols/day."""
from trader.us.runner.new_position_limit_scope import (
    US_NEW_POSITION_LIMIT_APPLIED_SCOPE,
    explain_effective_new_position_limit,
)


def test_effective_cap_is_per_tick_slot_limit_not_daily_counter():
    proof = explain_effective_new_position_limit(
        available_current_position_slots=20,
        effective_max_new_positions=5,
    )
    assert proof["applied_scope"] == US_NEW_POSITION_LIMIT_APPLIED_SCOPE
    assert proof["tick_new_slots_after_cap"] == 5
    assert proof["daily_cumulative_new_positions_proven"] is False
    # On the next Tick the implementation recomputes from current holdings;
    # it does not automatically subtract the preceding 3 newly-filled buys.
    next_tick = explain_effective_new_position_limit(
        available_current_position_slots=17,
        effective_max_new_positions=5,
    )
    assert next_tick["tick_new_slots_after_cap"] == 5


def test_current_held_capacity_is_still_respected():
    assert explain_effective_new_position_limit(
        available_current_position_slots=2,
        effective_max_new_positions=5,
    )["tick_new_slots_after_cap"] == 2


def test_zero_cap_blocks_new_symbols_without_changing_existing_policy():
    assert explain_effective_new_position_limit(
        available_current_position_slots=20,
        effective_max_new_positions=0,
    )["tick_new_slots_after_cap"] == 0


def test_absent_regime_cap_does_not_create_new_limit():
    proof = explain_effective_new_position_limit(
        available_current_position_slots=8,
        effective_max_new_positions=None,
    )
    assert proof["configured_cap"] is None
    assert proof["tick_new_slots_after_cap"] == 8


def test_20261027_multiple_ticks_can_buy_eight_symbols_without_daily_five_cap():
    # This intentionally does not introduce any daily-new-symbol counter.
    # The policy bounds the per-Tick available position slots only.
    filled_each_tick = (3, 3, 1, 1)
    running_new_buys = 0
    for filled in filled_each_tick:
        current_capacity = 35 - running_new_buys
        proof = explain_effective_new_position_limit(
            available_current_position_slots=current_capacity,
            effective_max_new_positions=5,
        )
        assert filled <= proof["tick_new_slots_after_cap"]
        assert not proof["daily_cumulative_new_positions_proven"]
        running_new_buys += filled
    assert running_new_buys == 8  # 8 is not blocked merely because 8 > 5
