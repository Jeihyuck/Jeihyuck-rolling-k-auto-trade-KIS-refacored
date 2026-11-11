"""Nov 11 policy: bounded DEFENSIVE micro-first-entry, TQQQ owner only.

All assertions are pure/offline; a BUY decision is not a KIS submission.
"""
from dataclasses import replace
from datetime import date

import pytest

from trader.us.infinite.config import InfiniteConfig
from trader.us.infinite.models import Action, InfiniteState, PositionSnapshot, Status
from trader.us.infinite.risk_adapter import effective_regime
from trader.us.infinite.strategy import evaluate


DAY = date(2026, 11, 10)


def caution(**updates):
    # The observed *type* of regime conflict: DEFENSE_CAUTION + DEFENSIVE,
    # QQQ TRANSITION and no TQQQ position (prices are synthetic).
    o = {
        "market_state": "DEFENSE_CAUTION", "market_regime": "DEFENSIVE",
        "entry_can_proceed": True, "exit_can_proceed": True, "allow_new_buy": True,
        "force_entry_block": False, "hard_system_failure": False,
        "reconcile_entry_block": False, "tqqq_capital_preservation": False,
        "tqqq_context_quality": "ok", "tqqq_quote_stale": False,
        "qqq_completed_close": 108.0, "qqq_ma50": 100.0,
        "qqq_ma200": 105.0, "qqq_ma200_slope": 0.0,
        "qqq_20d_return": -0.02, "qqq_drawdown_252": -0.10,
        "qqq_realized_vol_20d": 0.20, "qqq_trend_efficiency_20d": 0.50,
    }
    o.update(updates)
    return o


def decide(o=None, state=None, cfg=None, price=100.0, *, day=DAY,
           pending_buy=False, pending_sell=False):
    overlay = caution() if o is None else o
    regime, mult, reserve, allowed, _ = effective_regime(overlay)
    return evaluate(
        config=cfg or InfiniteConfig(), state=state or InfiniteState(),
        position=PositionSnapshot(qty=0, orderable_qty=0, average_price=0, price=price),
        trading_date=day, pending_buy=pending_buy, pending_sell=pending_sell,
        overlay=overlay, entry_allowed=allowed, buy_multiplier=mult,
        regime_reserve_permission=reserve, effective_regime_name=regime,
    )


def test_nov10_defensive_transition_proposes_one_small_share_default_on():
    out = decide()
    assert (out.action, out.reason, out.qty) == (
        Action.BUY, "DEFENSIVE_MICRO_FIRST_BUY", 1,
    )
    assert out.notional == 100
    assert out.metadata["defensive_micro_first_buy_raw_state"] == "DEFENSE_CAUTION"
    assert out.metadata["defensive_micro_first_buy_effective_regime"] == "DEFENSIVE"
    assert out.metadata["defensive_micro_first_buy_long_trend"] == "TRANSITION"
    assert out.metadata["defensive_micro_first_buy_gross_cap_proof"] <= 125


def test_cost_reserve_stops_order_if_whole_share_all_in_exceeds_cap():
    assert decide(price=123).action == Action.BUY
    out = decide(price=124.5)
    assert (out.action, out.reason) == (
        Action.BLOCK, "defensive_micro_whole_share_exceeds_cap",
    )
    assert decide(price=200).action == Action.BLOCK


@pytest.mark.parametrize("patch", [
    {"market_state": "DEFENSE_RISK_OFF"},
    {"market_state": "DEFENSE_CRASH_CONFIRMED"},
    {"market_state": "DEFENSE_CRASH_PENDING"},
    {"market_state": "DEFENSE_CRASH_REBOUND"},
    {"market_state": "STRONG_RISK_ON"},  # Existing 2-day proof still required
    {"market_regime": "RISK_OFF"},
    {"market_regime": "CHOP_HIGH_VOL"},
    {"entry_can_proceed": False},
    {"exit_can_proceed": False},
    {"allow_new_buy": False},
    {"hard_system_failure": True},
    {"reconcile_entry_block": True},
    {"force_entry_block": True},
    {"tqqq_capital_preservation": True},
    {"tqqq_context_quality": "degraded"},
    {"tqqq_quote_stale": True},
    {"qqq_suspect": True},
    {"qqq_completed_close": None},
    {"qqq_realized_vol_20d": 0.45, "qqq_trend_efficiency_20d": 0.15},
    {"qqq_completed_close": 90, "qqq_ma50": 95, "qqq_ma200": 110,
     "qqq_ma200_slope": -0.1},  # BEAR
])
def test_micro_entry_never_weakens_existing_risk_fence(patch):
    out = decide(caution(**patch))
    assert out.action != Action.BUY, (patch, out)


def test_pending_orders_and_operator_pause_block_micro_buy():
    assert decide(pending_buy=True).action == Action.BLOCK
    assert decide(pending_sell=True).action == Action.BLOCK
    cfg = InfiniteConfig(defensive_micro_first_buy_enabled=False)
    assert decide(cfg=cfg).reason == "tqqq_regime_new_cycle_block"


def test_reserved_same_day_cycle_may_retry_but_old_or_filled_cycles_cannot():
    start = InfiniteState(
        cycle_id="same-epoch-reservation", cycle_start_date=DAY,
        status=Status.READY, metadata={
            "defensive_micro_first_buy_reserved_cycle": True,
            "defensive_micro_first_buy_trade_date": DAY.isoformat(),
        }
    )
    assert decide(state=start).action == Action.BUY
    assert decide(state=replace(start, status=Status.ACTIVE)).action == Action.BUY
    assert decide(state=replace(start, cycle_start_date=date(2026, 11, 9))).action == Action.BLOCK
    assert decide(state=replace(start, core_filled_notional=100)).action == Action.BLOCK
    assert decide(state=replace(start, metadata={})).action == Action.BLOCK
    assert decide(state=replace(start, status=Status.COMPLETE)).action == Action.BLOCK


def test_invalid_cap_or_buffer_is_not_silently_accepted(monkeypatch):
    for max_usd in ("0", "-1", "125.001", "nan", "not_a_number"):
        monkeypatch.setenv("US_TQQQ_INFINITE_DEFENSIVE_MICRO_FIRST_BUY_MAX_USD", max_usd)
        with pytest.raises(ValueError):
            InfiniteConfig.from_env()
    monkeypatch.delenv("US_TQQQ_INFINITE_DEFENSIVE_MICRO_FIRST_BUY_MAX_USD")
    monkeypatch.setenv("US_TQQQ_INFINITE_DEFENSIVE_MICRO_FIRST_BUY_COST_BUFFER_PCT", "nan")
    with pytest.raises(ValueError):
        InfiniteConfig.from_env()
