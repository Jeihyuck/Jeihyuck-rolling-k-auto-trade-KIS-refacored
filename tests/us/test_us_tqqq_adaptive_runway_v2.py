from dataclasses import replace
from datetime import date, timedelta
import math

import pytest

from trader.us.infinite.config import InfiniteConfig
from trader.us.infinite.models import Action, InfiniteState, PositionSnapshot, Status
from trader.us.infinite.risk_adapter import CANONICAL_MARKET_STATES, assess_market_risk
from trader.us.infinite.strategy import evaluate
from trader.us.market_state_overlay import _market_returns


TODAY = date(2026, 8, 12)
CFG = InfiniteConfig()


def overlay(state="NORMAL", **values):
    return {"market_state": state, **values}


def owned(**metadata):
    return InfiniteState(cycle_id="cycle", status=Status.ACTIVE,
                         core_filled_notional=250, last_buy_date=TODAY - timedelta(days=30),
                         metadata={"last_buy_fill_price": 100, **metadata})


def decide(state, position, context):
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
    assert result.action == Action.SELL and result.qty == 4


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
                  overlay("DEFENSE_RISK_OFF", qqq_drawdown_252=-.31)).reason == "capital_preservation_wait"


def test_age_alone_only_enables_capital_preservation_during_bear():
    aged_bull = replace(owned(long_trend="BULL"), cycle_age_trading_days=121)
    aged_bear = replace(owned(long_trend="BEAR"), cycle_age_trading_days=121)
    assert decide(aged_bull, PositionSnapshot(qty=5, average_price=100, price=100), overlay()).reason != "capital_preservation_wait"
    assert decide(aged_bear, PositionSnapshot(qty=5, average_price=100, price=95), overlay()).reason == "capital_preservation_wait"
