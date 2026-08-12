from datetime import date, timedelta

from trader.us.infinite.replay import ReplayBar, run_replay, session_dates


def bar(day, price=50, state="NORMAL", **context):
    return ReplayBar(day, 100, price, {"market_state": state, **context})


def test_replay_calendar_is_exactly_completed_bar_dates_not_weekdays():
    # Real 2022 QQQ sessions around Independence Day: Fri Jul 1 then Tue Jul 5.
    bars = [bar(date(2022, 7, 1)), bar(date(2022, 7, 5)), bar(date(2022, 7, 6))]
    assert session_dates(bars) == (date(2022, 7, 1), date(2022, 7, 5), date(2022, 7, 6))
    assert run_replay(bars)["session_count"] == 3
    assert date(2022, 7, 4) not in run_replay(bars)["session_dates"]


def test_replay_bear_gap_uses_bar_index_across_2022_holiday():
    # Seven supplied sessions; calendar dates deliberately cross Jul 4.
    dates = [date(2022, 6, 30), date(2022, 7, 1), date(2022, 7, 5),
             date(2022, 7, 6), date(2022, 7, 7), date(2022, 7, 8), date(2022, 7, 11)]
    result = run_replay([bar(day, price=50 - i * 3, long_trend="BEAR",
                             qqq_drawdown_252=-.20) for i, day in enumerate(dates)])
    assert result["session_count"] == 7
    assert date(2022, 7, 4) not in result["session_dates"]
    assert result["daily_cap_violation_count"] == 0
    assert result["hard_cap_violation_count"] == 0


def test_synthetic_chop_and_crash_replay_invariants():
    start = date(2030, 1, 2)
    dates = [start + timedelta(days=i * 2) for i in range(24)]
    bars = []
    for i, day in enumerate(dates):
        state = "DEFENSE_CRASH_PENDING" if i in {5, 6} else (
            "DEFENSE_CRASH_CONFIRMED" if i == 7 else "NORMAL")
        bars.append(bar(day, 50 if i % 2 else 52, state,
                        qqq_realized_vol_20d=.50, qqq_trend_efficiency_20d=.05))
    result = run_replay(bars)
    assert result["crash_buy_count"] == 0
    assert result["chop_buy_count"] == 0
    assert result["daily_cap_violation_count"] == 0
    assert result["hard_cap_violation_count"] == 0
    assert result["invalid_quote_order_count"] == 0
    assert result["duplicate_same_day_buy_count"] == 0


def test_invalid_quote_never_creates_replay_order():
    result = run_replay([ReplayBar(date(2022, 1, 3), 100, 50,
                                   {"market_state": "NORMAL"}, quote_valid=False)])
    assert result["invalid_quote_order_count"] == 0
