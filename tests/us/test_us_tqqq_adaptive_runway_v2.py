from dataclasses import replace
from datetime import date, timedelta
import math

import pytest

from trader.us.infinite.config import InfiniteConfig
from trader.us.infinite.models import Action, InfiniteState, PositionSnapshot, Status
from trader.us.infinite.risk_adapter import CANONICAL_MARKET_STATES, assess_market_risk
from trader.us.infinite.strategy import _trading_days_since, evaluate
from trader.us.market_state_overlay import _market_returns


TODAY = date(2026, 8, 12)
CFG = InfiniteConfig()


def overlay(state="NORMAL", **values):
    return {"market_state": state, "tqqq_context_quality": "ok",
            "qqq_completed_close": 100, "qqq_ma50": 99, "qqq_ma200": 98,
            "qqq_ma200_slope": .1, "qqq_20d_return": .02, "qqq_drawdown_252": -.05,
            "qqq_realized_vol_20d": .2, "qqq_trend_efficiency_20d": .5, **values}


def owned(**metadata):
    return InfiniteState(cycle_id="cycle", status=Status.ACTIVE,
                         core_filled_notional=250, last_buy_date=TODAY - timedelta(days=30),
                         metadata={"last_buy_fill_price": 100, **metadata})


def decide(state, position, context):
    if position.qty > 0 and position.orderable_qty is None:
        position = replace(position, orderable_qty=position.qty)
    return evaluate(config=CFG, state=state, position=position,
                    trading_date=TODAY, overlay=context)


def test_all_eight_dual_agent_states_are_explicit_and_regimes_are_not_states():
    assert CANONICAL_MARKET_STATES == {
        "STRONG_RISK_ON", "RISK_ON", "NORMAL", "DEFENSE_CAUTION",
        "DEFENSE_RISK_OFF", "DEFENSE_CRASH_PENDING", "DEFENSE_CRASH_CONFIRMED",
        "DEFENSE_CRASH_REBOUND",
    }
    assert assess_market_risk({"market_state": "DEFENSE_CAUTION"}).allow_buy
    assert assess_market_risk({"market_state": "GROWTH_LEADERSHIP"}).reason == "unknown_market_risk"


@pytest.mark.parametrize("price", [0, float("nan"), float("inf")])
def test_invalid_executable_quote_blocks_every_order(price):
    result = decide(InfiniteState(), PositionSnapshot(price=price), overlay())
    assert (result.action, result.reason) == (Action.BLOCK, "tqqq_price_unavailable")


@pytest.mark.parametrize("market_state", list(CANONICAL_MARKET_STATES))
def test_take_profit_is_exit_first_in_every_market_state(market_state):
    result = decide(owned(long_trend="BEAR", chop_high_vol=True, capital_preservation=True),
                    PositionSnapshot(qty=4, average_price=50, price=55), overlay(market_state))
    expected = 4 if market_state in {"DEFENSE_CRASH_PENDING", "DEFENSE_CRASH_CONFIRMED"} else 2
    assert result.action == Action.SELL and result.qty == expected


def test_completed_qqq_rows_detect_high_vol_chop_without_extra_fetch():
    closes = [100.0]
    for i in range(260):
        closes.append(closes[-1] * (1.05 if i % 2 == 0 else 1 / 1.05))
    rows = [{"date": f"{i:04d}", "close": value} for i, value in enumerate(closes)]
    provider = {symbol: rows for symbol in ("SPY", "QQQ", "SMH", "DIA", "IWM", "RSP",
                                             "XLK", "XLI", "XLF", "XLV", "XLP", "XLU", "XLE")}
    result = _market_returns(provider, TODAY.isoformat(), [])
    assert result["qqq_realized_vol_20d"] >= CFG.chop_rv20_min
    assert result["qqq_trend_efficiency_20d"] <= CFG.chop_efficiency_max
    assert result["tqqq_context_quality"] == "ok"
    blocked = decide(InfiniteState(), PositionSnapshot(price=50), overlay(
        qqq_realized_vol_20d=result["qqq_realized_vol_20d"],
        qqq_trend_efficiency_20d=result["qqq_trend_efficiency_20d"]))
    assert blocked.reason == "chop_high_vol_new_cycle_block"


def test_chop_existing_cycle_requires_gap_step_and_average_ceiling():
    context = overlay(qqq_realized_vol_20d=.5, qqq_trend_efficiency_20d=.05)
    recent = replace(owned(), last_buy_date=TODAY - timedelta(days=3))
    assert decide(recent, PositionSnapshot(qty=5, average_price=100, price=92), context).reason == "chop_high_vol_wait"
    assert decide(owned(), PositionSnapshot(qty=5, average_price=100, price=93), context).reason == "chop_high_vol_wait"
    assert decide(owned(), PositionSnapshot(qty=5, average_price=100, price=92), context).action == Action.BUY


def test_capital_preservation_requires_bear_gap_and_ten_percent_step():
    state = owned(long_trend="BEAR")
    cp = overlay(qqq_drawdown_252=-.31)
    assert decide(replace(state, last_buy_date=TODAY - timedelta(days=5)),
                  PositionSnapshot(qty=5, average_price=100, price=89), cp).reason == "capital_preservation_wait"
    assert decide(state, PositionSnapshot(qty=5, average_price=100, price=91), cp).reason == "capital_preservation_wait"
    assert decide(state, PositionSnapshot(qty=5, average_price=100, price=90), cp).action == Action.BUY
    assert decide(state, PositionSnapshot(qty=5, average_price=100, price=90),
                  overlay("DEFENSE_RISK_OFF", qqq_drawdown_252=-.31)).action == Action.BUY


def test_age_alone_only_enables_capital_preservation_during_bear():
    aged_bull = replace(owned(long_trend="BULL"), cycle_age_trading_days=121)
    aged_bear = replace(owned(long_trend="BEAR"), cycle_age_trading_days=121)
    assert decide(aged_bull, PositionSnapshot(qty=5, average_price=100, price=100), overlay()).reason != "capital_preservation_wait"
    assert decide(aged_bear, PositionSnapshot(qty=5, average_price=100, price=95), overlay()).reason == "capital_preservation_wait"


@pytest.mark.parametrize(("context", "qty", "reason"), [
    ({"force_entry_block": True}, 0, "overlay_force_entry_block"),
    ({"allow_new_buy": False}, 0, "overlay_new_buy_block"),
    # Standard add/gross overlay does not own the Infinite sleeve.
    ({"allow_new_buy": True, "allow_add_to_existing": False}, 5, "average_buy"),
])
def test_production_overlay_buy_gates_are_enforced_but_sell_remains_first(context, qty, reason):
    state = owned() if qty else InfiniteState()
    position = PositionSnapshot(qty=qty, average_price=50 if qty else 0, price=50)
    assert decide(state, position, overlay(**context)).reason == reason
    if qty:
        sell = decide(state, replace(position, price=55), overlay(**context))
        assert sell.action == Action.SELL and sell.qty == max(1, int(qty * 0.5))


def test_context_quality_fails_closed_for_buys_but_not_take_profit_sell():
    context = overlay(tqqq_context_quality="insufficient")
    assert decide(InfiniteState(), PositionSnapshot(price=50), context).reason == "tqqq_required_market_data_missing"
    result = decide(owned(), PositionSnapshot(qty=2, average_price=50, price=55), context)
    assert result.action == Action.SELL


@pytest.mark.parametrize(("sticky", "drawdown", "expected"), [
    (True, -.10, Action.BUY),
    (False, -.10, Action.BLOCK),
    (False, -.20, Action.BUY),
])
def test_deep_bear_unlock_is_sticky_within_cycle(sticky, drawdown, expected):
    state = replace(owned(long_trend="BEAR", deep_bear_unlocked=sticky), core_filled_notional=5_500)
    result = decide(state, PositionSnapshot(qty=5, average_price=100, price=90),
                    overlay(qqq_drawdown_252=drawdown))
    assert result.action == expected
    if sticky:
        assert result.reason != "deep_bear_core_locked"


def test_trading_days_since_excludes_independence_day_observed():
    # Fri 2026-07-03 is the observed NYSE Independence Day holiday.  Only
    # Jul 2 and Jul 6 are sessions in this interval.
    assert _trading_days_since(date(2026, 7, 1), date(2026, 7, 6)) == 2
    assert _trading_days_since(date(2026, 7, 2), date(2026, 7, 3)) == 0


def test_bear_seven_session_gap_does_not_open_early_across_us_holiday():
    state = InfiniteState(
        cycle_id="cycle", status=Status.ACTIVE, core_filled_notional=250,
        last_buy_date=date(2026, 6, 25),
        metadata={"long_trend": "BEAR", "last_buy_fill_price": 100},
    )
    position = PositionSnapshot(qty=5, average_price=100, price=94)
    before = evaluate(config=CFG, state=state, position=position,
                      trading_date=date(2026, 7, 6), overlay=overlay())
    on_seventh_session = evaluate(config=CFG, state=state, position=position,
                                  trading_date=date(2026, 7, 7), overlay=overlay())
    assert (before.action, before.reason) == (Action.BLOCK, "bear_runway_wait")
    assert on_seventh_session.action == Action.BUY
