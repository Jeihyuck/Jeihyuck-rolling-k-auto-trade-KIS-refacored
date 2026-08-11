from __future__ import annotations

from dataclasses import replace
from datetime import date

from trader.us.infinite.config import InfiniteConfig
from trader.us.infinite.models import Action, InfiniteState, PositionSnapshot, Status
from trader.us.infinite.repository import InfiniteRepository
from trader.us.infinite.strategy import evaluate

TODAY = date(2026, 8, 11)
CYCLE = "cycle-a"
STATE = InfiniteState(cycle_id=CYCLE, cycle_start_date=date(2026, 8, 1), status=Status.ACTIVE)


def fill(*, side="BUY", qty=1, price=100, key=None, strategy="TQQQ_INFINITE_V3",
         book="TQQQ_INFINITE", cycle=CYCLE, trade_date=TODAY):
    return {
        "trade_date": trade_date, "side": side, "qty": qty, "price_usd": price,
        "client_order_key": key or f"TQQQ_INF_V3:{cycle}:{trade_date}:BUY",
        "intent_strategy": strategy,
        "intent_meta": {"strategy": strategy, "book": book, "cycle_id": cycle},
        "order_meta": {}, "meta": {},
    }


def test_same_day_legacy_and_infinite_fill_only_counts_infinite():
    legacy = fill(key="PB1:2026-08-11:TQQQ:BUY", strategy="US_PB1", book="SWING", cycle="")
    actual = fill(qty=2, price=100)
    result = InfiniteRepository._summarize_fill_rows([legacy, actual], STATE, TODAY)
    assert result[:3] == (200, 200, 0)


def test_historical_manual_tqqq_fill_is_excluded_from_capital_and_anchor():
    manual = fill(key="MANUAL:TQQQ:BUY", strategy="", book="", cycle="",
                  trade_date=date(2026, 8, 4), price=12)
    actual = fill(trade_date=date(2026, 8, 5), price=80)
    buys, daily, sells, last_buy, anchor = InfiniteRepository._summarize_fill_rows(
        [manual, actual], STATE, TODAY
    )
    assert (buys, daily, sells, last_buy, anchor) == (80, 0, 0, date(2026, 8, 5), 80)


def test_unrelated_tqqq_sell_is_not_cycle_accounting():
    unrelated = fill(side="SELL", key="LEGACY:TQQQ:SELL", strategy="US_PB1", book="SWING", cycle="")
    related = fill(side="SELL", key=f"TQQQ_INF_V3:{CYCLE}:2026-08-11:SELL")
    assert InfiniteRepository._summarize_fill_rows([unrelated, related], STATE, TODAY)[2] == 100


def test_restart_preserves_cycle_attribution_and_rejects_other_cycle():
    current = fill(qty=2)
    old = fill(key="TQQQ_INF_V3:old-cycle:2026-08-11:BUY", cycle="old-cycle", qty=50)
    first = InfiniteRepository._summarize_fill_rows([current, old], STATE, TODAY)
    restarted = InfiniteRepository._summarize_fill_rows([old, current], replace(STATE), TODAY)
    assert first == restarted
    assert first[0] == 200


def test_non_infinite_fills_do_not_affect_10k_cap_or_250_daily_limit():
    legacy = fill(key="LEGACY:TQQQ:BUY", strategy="US_PB1", book="SWING", cycle="", qty=100, price=100)
    actual = fill(qty=1, price=100)
    cycle, daily, *_ = InfiniteRepository._summarize_fill_rows([legacy, actual], STATE, TODAY)
    accounted = replace(STATE, core_filled_notional=cycle, last_buy_date=None)
    decision = evaluate(
        config=InfiniteConfig(enabled=True), state=accounted,
        position=PositionSnapshot(qty=1, average_price=100, price=100), trading_date=TODAY,
        daily_filled_buy_notional=daily, overlay={"market_state": "NORMAL"},
    )
    assert cycle == daily == 100
    assert decision.action == Action.BUY and decision.notional == 100


def test_attribution_requires_key_strategy_book_and_cycle():
    rows = [
        fill(strategy="WRONG"), fill(book="WRONG"), fill(cycle="wrong"),
        fill(key="TQQQ_INF_V3:wrong:2026-08-11:BUY"),
    ]
    assert InfiniteRepository._summarize_fill_rows(rows, STATE, TODAY)[0] == 0
