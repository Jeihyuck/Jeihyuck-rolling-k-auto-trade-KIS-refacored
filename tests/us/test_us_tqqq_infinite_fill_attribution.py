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
         book="TQQQ_INFINITE", cycle=CYCLE, trade_date=TODAY, policy_action=None):
    return {
        "trade_date": trade_date, "side": side, "qty": qty, "price_usd": price,
        "client_order_key": key or f"TQQQ_INF_V3:{cycle}:{trade_date}:BUY",
        "intent_strategy": strategy,
        "intent_meta": {"strategy": strategy, "book": book, "cycle_id": cycle,
                        "policy_action": policy_action},
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
        daily_filled_buy_notional=daily, overlay={"market_state": "NORMAL",
            "tqqq_context_quality": "ok", "qqq_completed_close": 100,
            "qqq_ma50": 99, "qqq_ma200": 98, "qqq_ma200_slope": .1,
            "qqq_20d_return": .02, "qqq_drawdown_252": -.05,
            "qqq_realized_vol_20d": .2, "qqq_trend_efficiency_20d": .5},
    )
    assert cycle == daily == 100
    assert decision.action == Action.BUY and decision.notional == 100


def test_attribution_requires_key_strategy_book_and_cycle():
    rows = [
        fill(strategy="WRONG"), fill(book="WRONG"), fill(cycle="wrong"),
        fill(key="TQQQ_INF_V3:wrong:2026-08-11:BUY"),
    ]
    assert InfiniteRepository._summarize_fill_rows(rows, STATE, TODAY)[0] == 0


def test_rebound_probe_is_consumed_only_by_attributed_actual_fill():
    ack_or_intent_only = []  # no us_fills row exists
    ordinary_fill = fill(policy_action=None)
    rebound_fill = fill(policy_action="REBOUND_PROBE", trade_date=date(2026, 8, 8))
    unrelated = fill(policy_action="REBOUND_PROBE", cycle="other")
    assert InfiniteRepository._last_rebound_probe_fill_date(ack_or_intent_only, STATE) is None
    assert InfiniteRepository._last_rebound_probe_fill_date([ordinary_fill, unrelated], STATE) is None
    assert InfiniteRepository._last_rebound_probe_fill_date([ordinary_fill, rebound_fill], STATE) == date(2026, 8, 8)


def test_reconcile_records_actual_probe_fill_and_cooldown_date(monkeypatch):
    repo = InfiniteRepository.__new__(InfiniteRepository)
    repo.cycle_fill_stats = lambda *_: {
        "total_buy_notional": 250, "daily_buy_notional": 0, "total_sell_notional": 0,
        "last_buy_date": date(2026, 8, 8), "first_fill_price": 50,
        "last_buy_fill_price": 50, "last_rebound_probe_fill_date": date(2026, 8, 8),
    }
    monkeypatch.setattr("trader.us.market_calendar.is_us_trading_day", lambda day: day.weekday() < 5)
    result = repo.reconcile_metadata(
        STATE, trading_date=TODAY, broker_qty=5, broker_average_price=50,
        core_cap=7_500, rebound_cooldown=3,
    )
    assert result.metadata["rebound_probe_date"] == "2026-08-08"
    assert result.metadata["rebound_cooldown_until"] == "2026-08-12"


def test_rebound_repository_cooldown_matches_strategy_us_session_count():
    from trader.us.infinite.strategy import _trading_days_since

    repo = InfiniteRepository.__new__(InfiniteRepository)
    repo.cycle_fill_stats = lambda *_: {
        "total_buy_notional": 250, "daily_buy_notional": 0, "total_sell_notional": 0,
        "last_buy_date": date(2026, 7, 2), "first_fill_price": 50,
        "last_buy_fill_price": 50, "last_rebound_probe_fill_date": date(2026, 7, 2),
    }
    state = replace(STATE, cycle_start_date=date(2026, 7, 1))
    result = repo.reconcile_metadata(
        state, trading_date=date(2026, 7, 8), broker_qty=5,
        broker_average_price=50, core_cap=7_500, rebound_cooldown=3,
    )
    cooldown_until = date.fromisoformat(result.metadata["rebound_cooldown_until"])
    assert cooldown_until == date(2026, 7, 8)
    assert _trading_days_since(date(2026, 7, 2), cooldown_until) == 3
    assert _trading_days_since(date(2026, 7, 2), date(2026, 7, 7)) == 2
