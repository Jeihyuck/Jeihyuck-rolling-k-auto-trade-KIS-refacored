from datetime import date, timedelta

from trader.us.infinite.replay import ReplayBar, build_replay_bars, run_replay, session_dates
from trader.us.infinite.config import InfiniteConfig
from trader.us.infinite.models import InfiniteState, Status
from trader.us.infinite.policy_state import update_adaptive_policy_state
from trader.us.infinite.strategy import classify_long_trend
from trader.us.market_state_overlay import _market_returns, calculate_qqq_long_context


def bar(day, price=50, state="NORMAL", **context):
    defaults = {"tqqq_context_quality": "ok", "qqq_completed_close": 100,
                "qqq_ma50": 99, "qqq_ma200": 98, "qqq_ma200_slope": .1,
                "qqq_20d_return": .02, "qqq_drawdown_252": -.05,
                "qqq_realized_vol_20d": .2, "qqq_trend_efficiency_20d": .5}
    return ReplayBar(day, 100, price, {"market_state": state, **defaults, **context})


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
    assert result["crash_pending_buy_count"] == 0
    assert result["crash_confirmed_buy_count"] == 0
    assert result["chop_buy_count"] == 0
    assert result["daily_cap_violation_count"] == 0
    assert result["hard_cap_violation_count"] == 0
    assert result["invalid_quote_order_count"] == 0
    assert result["duplicate_same_day_buy_count"] == 0


def test_real_bar_builder_uses_intersection_and_production_indicator_helper():
    start = date(2021, 1, 4)
    qqq = [{"date": start + timedelta(days=i), "close": 100 + i} for i in range(260)]
    tqqq = [{"date": start + timedelta(days=i), "close": 50 + i} for i in range(260) if i != 10]
    bars = build_replay_bars(qqq, tqqq)
    assert len(bars) == 259 and start + timedelta(days=10) not in session_dates(bars)
    assert bars[-1].overlay["tqqq_context_quality"] == "ok"
    assert bars[-1].overlay["market_state_replay_mode"] == "neutral_normal"
    bounded = build_replay_bars(qqq, tqqq, start=start + timedelta(days=252))
    assert bounded[0].overlay["tqqq_context_quality"] == "ok"  # warmup retained, capital starts here
    assert "qqq_20d_return" in bars[-1].overlay


def test_long_context_classifies_rising_falling_and_recovery_histories():
    rising = calculate_qqq_long_context([100 + i for i in range(260)])
    falling = calculate_qqq_long_context([400 - i for i in range(260)])
    assert classify_long_trend({"market_state": "NORMAL", **rising}) == "BULL"
    assert classify_long_trend({"market_state": "NORMAL", **falling}) == "BEAR"
    recovery = {**falling, "qqq_completed_close": falling["qqq_ma50"] + 1,
                "qqq_20d_return": .01, "market_state": "NORMAL"}
    assert classify_long_trend(recovery, structural_bear_seen=True) == "RECOVERY"


def test_production_market_returns_and_pure_context_have_same_20d_return():
    closes = [100 + i for i in range(260)]
    rows = [{"date": f"{i:04d}", "close": close}
            for i, close in enumerate(closes)]
    provider = {symbol: rows for symbol in ("SPY", "QQQ", "SMH", "DIA", "IWM", "RSP",
                                             "XLK", "XLI", "XLF", "XLV", "XLP", "XLU", "XLE")}
    assert _market_returns(provider, "2021-01-01", [])["qqq_20d_return"] == \
        calculate_qqq_long_context(closes)["qqq_20d_return"]


def test_shared_policy_transition_is_daily_idempotent_and_unlocks_recovery_reserve():
    config = InfiniteConfig()
    state = InfiniteState(cycle_id="c", status=Status.ACTIVE, core_filled_notional=7_500,
                          material_market_crash=True, metadata={"structural_bear_seen": True})
    def recovery(day, current):
        overlay = {"market_state": "NORMAL", "qqq_completed_close": 110,
                   "qqq_ma50": 100, "qqq_ma200": 105, "qqq_ma200_slope": -1,
                   "qqq_20d_return": .05, "qqq_drawdown_252": -.15,
                   "qqq_realized_vol_20d": .2, "qqq_trend_efficiency_20d": .5}
        return update_adaptive_policy_state(state=current, trading_date=day,
                                            overlay=overlay, config=config)
    day1 = recovery(date(2023, 1, 3), state)
    same_day = recovery(date(2023, 1, 3), day1)
    day2 = recovery(date(2023, 1, 4), same_day)
    assert day1.metadata["recovery_streak"] == same_day.metadata["recovery_streak"] == 1
    assert day2.metadata["recovery_streak"] == 2 and day2.reserve_unlocked


def test_invalid_quote_never_creates_replay_order():
    result = run_replay([ReplayBar(date(2022, 1, 3), 100, 50,
                                   {"market_state": "NORMAL"}, quote_valid=False)])
    assert result["invalid_quote_order_count"] == 0


def test_completed_cycle_resets_age_and_metadata_before_next_cycle():
    bars = [bar(date(2023, 1, 3), 50), bar(date(2023, 1, 4), 55),
            bar(date(2023, 1, 5), 50), bar(date(2023, 1, 6), 55)]
    result = run_replay(bars)
    assert result["cycle_count"] == result["take_profit_cycle_count"] == 2
    assert result["take_profit_completion_sessions"] == [1, 1]
    assert result["maximum_cycle_completion_sessions"] == 1


def test_first_buy_resets_precycle_policy_metadata_before_fill_is_applied():
    polluted = InfiniteState(
        metadata={"structural_bear_seen": True, "deep_bear_unlocked": True,
                  "recovery_streak": 2}, material_market_crash=True,
        reserve_unlocked=True,
    )
    result = run_replay([bar(date(2023, 1, 3), 50)], initial_state=polluted)
    state = result["final_state"]
    assert state.cycle_id == "replay-1" and state.cycle_start_date == date(2023, 1, 3)
    assert state.core_filled_notional == 250 and state.reserve_filled_notional == 0
    assert not state.material_market_crash and not state.reserve_unlocked
    assert state.market_crash_streak == state.cycle_age_trading_days == 0
    assert state.metadata == {"last_buy_fill_price": 50, "long_trend": None}


def test_replay_reports_open_cycle_and_risk_off_audit_metrics():
    bars = [
        bar(date(2022, 1, 3), 50),
        bar(date(2022, 1, 4), 49, "DEFENSE_RISK_OFF"),
        bar(date(2022, 1, 14), 44, "DEFENSE_RISK_OFF"),
    ]
    result = run_replay(bars)
    assert result["incomplete_cycle_count"] == 1
    assert result["final_qty"] > 0
    assert result["maximum_cycle_duration_sessions"] == 2
    assert result["risk_off_buy_count"] >= 0
    assert result["maximum_deployed_notional"] <= 10_000
    assert result["daily_cap_violation_count"] == 0
