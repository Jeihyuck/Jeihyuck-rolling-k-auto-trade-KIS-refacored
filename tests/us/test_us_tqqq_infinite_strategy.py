from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from trader.us.infinite.config import InfiniteConfig
from trader.us.infinite.models import Action, InfiniteState, PositionSnapshot, Status
from trader.us.infinite.risk_adapter import assess_market_risk
from trader.us.infinite.strategy import evaluate

TODAY = date(2026, 8, 11)
CFG = InfiniteConfig(enabled=True)
NORMAL = {"market_state": "NORMAL"}


def decide(state=None, position=None, **kwargs):
    return evaluate(config=CFG, state=state, position=position or PositionSnapshot(price=50),
                    trading_date=TODAY, overlay=NORMAL, **kwargs)


def test_no_position_first_buy_is_one_unit_or_less():
    result = decide(InfiniteState())
    assert (result.action, result.qty, result.notional) == (Action.BUY, 5, 250)


@pytest.mark.parametrize(("price", "action"), [(51.5, Action.BUY), (51.51, Action.WAIT)])
def test_average_buy_premium(price, action):
    result = decide(InfiniteState(cycle_id="c", status=Status.ACTIVE),
                    PositionSnapshot(qty=10, average_price=50, price=price))
    assert result.action == action


@pytest.mark.parametrize(("flags", "reason"), [
    ({"pending_buy": True}, "pending_buy"),
    ({"pending_sell": True}, "pending_sell"),
    ({"daily_filled_buy_notional": 250}, "daily_buy_limit"),
])
def test_duplicate_and_pending_protection(flags, reason):
    result = decide(InfiniteState(cycle_id="c", status=Status.ACTIVE),
                    PositionSnapshot(qty=1, average_price=50, price=50), **flags)
    assert (result.action, result.reason) == (Action.BLOCK, reason)


def test_same_trading_day_second_buy_blocks_from_persisted_fill_date():
    result = decide(InfiniteState(cycle_id="c", status=Status.ACTIVE, last_buy_date=TODAY),
                    PositionSnapshot(qty=1, average_price=50, price=50))
    assert result.reason == "daily_buy_limit"


def test_take_profit_sells_all_and_skips_buy_guards():
    result = decide(InfiniteState(cycle_id="c", status=Status.PAUSED_AGE, cycle_age_trading_days=121),
                    PositionSnapshot(qty=7, average_price=50, price=55))
    assert (result.action, result.qty, result.next_status) == (Action.SELL, 7, Status.EXIT_PENDING)


def test_sell_waits_for_broker_confirmed_zero_before_complete():
    state = InfiniteState(cycle_id="c", status=Status.EXIT_PENDING)
    assert decide(state, PositionSnapshot(qty=1, average_price=50, price=50)).next_status == Status.EXIT_PENDING
    assert decide(state, PositionSnapshot(qty=0, price=50)).next_status == Status.COMPLETE


def test_same_day_restart_after_exit_blocks_but_next_day_can_start():
    state = InfiniteState(status=Status.COMPLETE, last_exit_date=TODAY)
    assert decide(state).reason == "same_day_cycle_restart"
    next_day = evaluate(config=CFG, state=state, position=PositionSnapshot(price=50),
                        trading_date=date(2026, 8, 12), overlay=NORMAL)
    assert next_day.action == Action.BUY


def test_cap_that_cannot_fit_whole_share_blocks():
    state = InfiniteState(cycle_id="c", status=Status.ACTIVE, core_filled_notional=7_500,
                          reserve_filled_notional=2_480, reserve_unlocked=True,
                          material_market_crash=True)
    result = decide(state, PositionSnapshot(qty=1, average_price=50, price=50))
    assert result.reason == "unit_cannot_buy_whole_share"


def test_core_exhaustion_does_not_automatically_unlock_reserve():
    state = InfiniteState(cycle_id="c", status=Status.ACTIVE, core_filled_notional=7_500)
    assert decide(state, PositionSnapshot(qty=1, average_price=50, price=50)).reason == "reserve_locked"


def test_partial_fill_accounting_leaves_actual_cap_available():
    state = InfiniteState(cycle_id="c", status=Status.ACTIVE, core_filled_notional=7_400)
    result = decide(state, PositionSnapshot(qty=1, average_price=50, price=50))
    assert result.notional == 100  # actual fills, not the prior requested $250


@pytest.mark.parametrize(("overlay", "allow", "reason"), [
    ({"market_state": "MARKET_CRASH"}, True, "market_crash_limited"),
    ({"market_state": "ACCOUNT_CRASH"}, False, "account_crash"),
    ({"market_state": "DEGRADED_DATA"}, False, "data_or_system_risk"),
    ({"market_state": "ALIEN"}, False, "unknown_market_risk"),
    ({"market_state": "DEFENSE_CRASH_REBOUND"}, True, "verified_rebound"),
])
def test_market_risk_classification(overlay, allow, reason):
    result = assess_market_risk(overlay)
    assert (result.allow_buy, result.reason) == (allow, reason)


@pytest.mark.parametrize(("streak", "action"), [(0, Action.BUY), (1, Action.BUY), (2, Action.BLOCK)])
def test_market_crash_only_allows_two_trading_days(streak, action):
    state = InfiniteState(cycle_id="c", status=Status.ACTIVE, market_crash_streak=streak,
                          material_market_crash=bool(streak))
    result = evaluate(config=CFG, state=state, position=PositionSnapshot(qty=1, average_price=50, price=50),
                      trading_date=TODAY, overlay={"market_state": "MARKET_CRASH"})
    assert result.action == action


def test_drawdown_and_age_pause_buy_but_not_sell():
    dd = InfiniteState(cycle_id="c", status=Status.ACTIVE, anchor_price=100)
    assert decide(dd, PositionSnapshot(qty=1, average_price=100, price=70)).reason == "drawdown_pause"
    aged = replace(dd, anchor_price=50, cycle_age_trading_days=121)
    assert decide(aged, PositionSnapshot(qty=1, average_price=50, price=50)).reason == "cycle_age_pause"
    assert decide(aged, PositionSnapshot(qty=1, average_price=50, price=55)).action == Action.SELL


@pytest.mark.parametrize("state", [
    InfiniteState(core_filled_notional=-1),
    InfiniteState(core_filled_notional=10_001),
    InfiniteState(reserve_unlocked=True),
])
def test_corrupt_state_fails_closed(state):
    assert decide(state).action == Action.BLOCK


def test_orphan_broker_position_fails_closed():
    result = decide(None, PositionSnapshot(qty=1, average_price=50, price=50))
    assert (result.action, result.reason) == (Action.BLOCK, "orphan_position")
