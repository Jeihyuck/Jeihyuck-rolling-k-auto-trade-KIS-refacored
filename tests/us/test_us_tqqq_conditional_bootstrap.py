"""TQQQ Infinite: account-scoped, two-session, <=$125 first-buy exception.

This is an opt-in *policy* in the normal code path, not a historical replay of
old epoch fills and not a relaxation of any PB1 or TQQQ crash/route gate.
"""
from dataclasses import replace
from datetime import date

import pytest

from trader.us.infinite.config import InfiniteConfig
from trader.us.infinite.models import Action, InfiniteState, PositionSnapshot, Status
from trader.us.infinite.policy_state import update_adaptive_policy_state
from trader.us.infinite.risk_adapter import (
    effective_regime, is_conflict_first_buy_signal, previous_us_trading_day,
)
from trader.us.infinite.strategy import evaluate


D1 = date(2026, 10, 28)  # Wednesday, STRONG_RISK_ON / DEFENSIVE Prep
D2 = date(2026, 10, 29)  # Thursday, next trading session


def overlay(**changes):
    result = {
        "market_state": "STRONG_RISK_ON", "market_regime": "DEFENSIVE",
        "entry_can_proceed": True, "exit_can_proceed": True, "allow_new_buy": True,
        "force_entry_block": False, "hard_system_failure": False,
        "reconcile_entry_block": False,
        "tqqq_context_quality": "ok", "tqqq_quote_stale": False,
        "qqq_completed_close": 120, "qqq_ma50": 115, "qqq_ma200": 100,
        "qqq_ma200_slope": .1, "qqq_20d_return": .03,
        "qqq_drawdown_252": -.05, "qqq_realized_vol_20d": .2,
        "qqq_trend_efficiency_20d": .6,
    }
    result.update(changes)
    return result


def observe(state, day, market=None, cfg=None):
    return update_adaptive_policy_state(
        state=state, trading_date=day, overlay=market or overlay(),
        config=cfg or InfiniteConfig(),
    )


def decision(state, day, market=None, cfg=None, *, price=100, qty=0):
    market = market or overlay()
    cfg = cfg or InfiniteConfig()
    regime, multiplier, reserve, allowed, _ = effective_regime(market)
    return evaluate(
        config=cfg, state=state,
        position=PositionSnapshot(qty=qty, orderable_qty=qty,
                                  average_price=100 if qty else 0, price=price),
        trading_date=day, overlay=market,
        entry_allowed=allowed, buy_multiplier=multiplier,
        regime_reserve_permission=reserve, effective_regime_name=regime,
    )


def test_first_session_blocked_second_session_uses_one_share_and_hard_cap():
    cfg = InfiniteConfig()
    state = observe(InfiniteState(), D1)
    assert state.metadata["conflict_first_buy_confirmed_dates"] == ["2026-10-28"]
    first = decision(state, D1)
    assert (first.action, first.reason) == (Action.BLOCK, "tqqq_regime_new_cycle_block")
    state = observe(state, D2)
    assert state.metadata["conflict_first_buy_confirmed_dates"] == ["2026-10-28", "2026-10-29"]
    second = decision(state, D2)
    assert (second.action, second.reason, second.qty) == (
        Action.BUY, "CONFLICT_STRONG_RISK_ON_FIRST_BUY", 1
    )
    assert second.notional == 100 <= 125
    assert second.metadata["conflict_first_buy_confirmed_dates"] == ["2026-10-28", "2026-10-29"]
    assert second.metadata["conflict_first_buy_structural_regime"] == "DEFENSIVE"
    assert decision(state, D2, price=130).action == Action.BLOCK  # whole share > $125


def test_duplicate_prep_or_tick_does_not_count_as_two_sessions():
    state = observe(InfiniteState(), D1)
    for _ in range(12):
        state = observe(state, D1)
    assert state.metadata["conflict_first_buy_confirmed_dates"] == ["2026-10-28"]
    assert decision(state, D1).action == Action.BLOCK


def test_missing_one_trading_session_restarts_streak():
    state = observe(InfiniteState(), D1)
    state = observe(state, date(2026, 10, 30))  # Thu was missed
    assert state.metadata["conflict_first_buy_confirmed_dates"] == ["2026-10-30"]
    assert decision(state, date(2026, 10, 30)).action == Action.BLOCK


def test_holiday_and_weekend_are_not_missing_sessions():
    # Friday 2026-11-27 is an early-close US trading session after Thanksgiving.
    assert previous_us_trading_day(date(2026, 11, 27)) == date(2026, 11, 25)
    assert previous_us_trading_day(date(2026, 11, 2)) == date(2026, 10, 30)
    state = observe(InfiniteState(), date(2026, 11, 25))
    state = observe(state, date(2026, 11, 27))
    assert decision(state, date(2026, 11, 27)).action == Action.BUY


@pytest.mark.parametrize("override", [
    {"entry_can_proceed": False}, {"exit_can_proceed": False},
    {"allow_new_buy": False}, {"force_entry_block": True},
    {"hard_system_failure": True}, {"reconcile_entry_block": True},
    {"tqqq_context_quality": "degraded"}, {"tqqq_quote_stale": True},
    {"qqq_suspect": True}, {"qqq_ma200_slope": 0},
    {"qqq_realized_vol_20d": float("nan")}, {"qqq_completed_close": None},
    {"tqqq_capital_preservation": True},
])
def test_bad_data_or_operational_fence_never_records_confirmation(override):
    bad = overlay(**override)
    assert not is_conflict_first_buy_signal(bad)
    state = observe(InfiniteState(), D1)
    state = observe(state, D2, bad)
    assert state.metadata["conflict_first_buy_confirmed_dates"] == ["2026-10-28"]
    assert decision(state, D2, bad).action != Action.BUY


def test_current_tick_cannot_reuse_two_day_evidence_after_market_changes():
    state = observe(observe(InfiniteState(), D1), D2)
    assert decision(state, D2, overlay(market_state="DEFENSE_CRASH_CONFIRMED",
                                       market_regime="RISK_OFF")).action == Action.BLOCK
    # Nov-11 approved policy: caution+DEFENSIVE may independently probe with
    # a tiny first buy. This is not the original two-session bullish exception.
    caution = decision(state, D2, overlay(market_state="DEFENSE_CAUTION"))
    assert caution.reason == "DEFENSIVE_MICRO_FIRST_BUY"
    assert decision(state, D2, overlay(qqq_ma200=130)).action == Action.BLOCK


def test_existing_or_previous_cycle_never_uses_first_buy_exception():
    state = observe(observe(InfiniteState(), D1), D2)
    old_cycle = replace(state, cycle_id="old-epoch-cycle", status=Status.ACTIVE,
                        core_filled_notional=100)
    assert decision(old_cycle, D2).reason == "tqqq_regime_new_cycle_block"
    # Even when a position is held, this special new-cycle exception is NOT a
    # replacement for the existing fast-dip / regular-add strategy policy.
    held = decision(state, D2, qty=1)
    assert held.reason != "CONFLICT_STRONG_RISK_ON_FIRST_BUY"


def test_feature_off_or_invalid_config_cannot_weaken_safety_ceiling():
    cfg = InfiniteConfig(conflict_first_buy_enabled=False)
    state = observe(observe(InfiniteState(), D1, cfg=cfg), D2, cfg=cfg)
    assert "conflict_first_buy_confirmed_dates" not in state.metadata
    assert decision(state, D2, cfg=cfg).action == Action.BLOCK
    with pytest.raises(ValueError, match="hard ceiling"):
        InfiniteConfig(conflict_first_buy_max_usd=250).validate()
    with pytest.raises(ValueError, match="2-5"):
        InfiniteConfig(conflict_first_buy_confirm_days=1).validate()


def test_no_first_buy_without_two_consecutive_dates_in_same_state():
    state = InfiniteState(metadata={
        "conflict_first_buy_confirmed_dates": ["2026-10-27", "2026-10-29"]
    })
    assert decision(state, D2).action == Action.BLOCK
    # Spoofed earlier epoch evidence is not imported; the repo's load_state()
    # is scoped by active trading_epoch_id, and the first-buy code never queries
    # or copies an older epoch record.


def test_reserved_but_unfilled_current_session_may_retry_with_same_proof():
    state = observe(observe(InfiniteState(), D1), D2)
    reserved = replace(
        state, cycle_id="new-epoch-probe-reservation",
        cycle_start_date=D2, status=Status.READY,
        metadata={**state.metadata, "conflict_first_buy_reserved_cycle": True},
    )
    assert decision(reserved, D2).action == Action.BUY
    # The runner may have moved the reserved state to ACTIVE before a failed
    # fresh-balance probe; this must not deadlock the rest of the session.
    assert decision(replace(reserved, status=Status.ACTIVE), D2).action == Action.BUY
    # But a reservation from a prior day, a filled cycle, or absent attestation
    # may NOT borrow the exception.
    assert decision(reserved, date(2026, 10, 30)).action == Action.BLOCK
    assert decision(replace(reserved, core_filled_notional=100), D2).action == Action.BLOCK
    no_marker = replace(reserved, metadata={
        k: v for k, v in reserved.metadata.items()
        if k != "conflict_first_buy_reserved_cycle"
    })
    assert decision(no_marker, D2).action == Action.BLOCK


def test_non_trading_day_does_not_advance_streak_or_authorize_order():
    start = date(2026, 11, 25)
    state = observe(InfiniteState(), start)
    # Thanksgiving holiday must not supply a fake second confirmation.
    holiday = date(2026, 11, 26)
    after_holiday = observe(state, holiday)
    assert after_holiday.metadata["conflict_first_buy_confirmed_dates"] == ["2026-11-25"]
    assert decision(after_holiday, holiday).action != Action.BUY
    friday = date(2026, 11, 27)
    state = observe(after_holiday, friday)
    assert decision(state, friday).action == Action.BUY
