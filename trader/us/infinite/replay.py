"""Deterministic, bar-session historical harness for ADAPTIVE_RUNWAY_V2.

The harness deliberately treats the supplied completed OHLCV dates as the
calendar.  It never synthesizes weekdays or consults the production calendar,
which is only authoritative for its configured runtime years.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from typing import Iterable

from .config import InfiniteConfig
from .models import Action, InfiniteState, PositionSnapshot, Status
from .policy_state import ActualFillEvidence, reserve_new_cycle, update_adaptive_policy_state
from .strategy import evaluate
from trader.us.market_state_overlay import calculate_qqq_long_context


@dataclass(frozen=True)
class ReplayBar:
    trading_date: date
    qqq_close: float
    tqqq_close: float
    overlay: dict
    quote_valid: bool = True


def build_replay_bars(qqq_rows: Iterable[dict], tqqq_rows: Iterable[dict],
                      *, market_states: dict[date, str] | None = None,
                      start: date | None = None, end: date | None = None) -> tuple[ReplayBar, ...]:
    """Build neutral/reconstructed replay input from the actual date intersection."""
    def normalize(rows):
        result = {}
        for row in rows:
            raw_date = row.get("date") or row.get("trade_date")
            day = raw_date if isinstance(raw_date, date) else date.fromisoformat(str(raw_date))
            result[day] = float(row.get("close"))
        return result
    qqq, tqqq = normalize(qqq_rows), normalize(tqqq_rows)
    dates = sorted(set(qqq) & set(tqqq)); closes: list[float] = []; bars = []
    for day in dates:
        closes.append(qqq[day])
        context = calculate_qqq_long_context(closes)
        context["market_state"] = (market_states or {}).get(day, "NORMAL")
        context["market_state_replay_mode"] = "reconstructed" if market_states else "neutral_normal"
        if (start is None or day >= start) and (end is None or day <= end):
            bars.append(ReplayBar(day, qqq[day], tqqq[day], context))
    return tuple(bars)


def session_dates(bars: Iterable[ReplayBar]) -> tuple[date, ...]:
    dates = tuple(bar.trading_date for bar in bars)
    if dates != tuple(sorted(set(dates))):
        raise ValueError("replay bars must have unique ascending trading dates")
    return dates


def run_replay(bars: Iterable[ReplayBar], config: InfiniteConfig | None = None,
               *, initial_state: InfiniteState | None = None) -> dict:
    """Replay fills at completed TQQQ closes and return audit metrics.

    This is an approval/invariant harness, not an execution-price simulator:
    callers must provide historically sourced completed bars and overlay state.
    """
    bars = tuple(bars)
    dates = session_dates(bars)
    sessions = frozenset(dates)
    config = config or InfiniteConfig()
    state = initial_state or InfiniteState()
    qty = 0
    cash = config.max_total_capital_usd
    cost = 0.0
    peak_equity = cash
    mdd = 0.0
    lowest_cash = cash
    lowest_units = int(cash // config.unit_usd)
    cycle_count = pending_buys = confirmed_buys = rebound_buys = chop_buys = cp_buys = invalid_orders = duplicate_buys = 0
    hard_cap_violations = daily_cap_violations = 0
    core_exhaustion_date = reserve_unlock_date = None
    cycle_start = None
    completion_durations: list[int] = []
    last_buy_price = None
    bought_dates: set[date] = set()
    reserve_used_notional = 0.0

    for index, bar in enumerate(bars):
        overlay = dict(bar.overlay)
        if cycle_start is not None:
            state = replace(state, cycle_age_trading_days=index - cycle_start)
        state = update_adaptive_policy_state(state=state, trading_date=bar.trading_date,
                                             overlay=overlay, config=config)
        price = bar.tqqq_close if bar.quote_valid else 0.0
        position = PositionSnapshot(qty=qty, average_price=(cost / qty if qty else 0), price=price)
        decision = evaluate(config=config, state=state, position=position,
                            trading_date=bar.trading_date, overlay=overlay,
                            trading_sessions=sessions)
        if decision.action in {Action.BUY, Action.SELL} and not bar.quote_valid:
            invalid_orders += 1
        if decision.action == Action.BUY:
            if bar.trading_date in bought_dates: duplicate_buys += 1
            bought_dates.add(bar.trading_date)
            if decision.notional > config.max_daily_buy_usd + 1e-6: daily_cap_violations += 1
            if state.total_filled_notional + decision.notional > config.max_total_capital_usd + 1e-6:
                hard_cap_violations += 1
            if not state.cycle_id:
                cycle_count += 1; cycle_start = index
                state = reserve_new_cycle(state, bar.trading_date, cycle_id=f"replay-{cycle_count}")
                state = replace(state, status=Status.ACTIVE)
            qty += decision.qty; cost += decision.notional; cash -= decision.notional
            core = min(config.core_capital_usd, state.core_filled_notional + decision.notional)
            reserve = max(0.0, state.core_filled_notional + state.reserve_filled_notional + decision.notional - core)
            last_buy_price = price
            state = replace(state, core_filled_notional=core, reserve_filled_notional=reserve,
                            last_buy_date=bar.trading_date,
                            metadata={**state.metadata, "last_buy_fill_price": last_buy_price,
                                      "long_trend": overlay.get("long_trend", state.metadata.get("long_trend"))})
            market_state = str(overlay.get("market_state", ""))
            pending_buys += int(market_state == "DEFENSE_CRASH_PENDING")
            confirmed_buys += int(market_state == "DEFENSE_CRASH_CONFIRMED")
            is_probe = market_state == "DEFENSE_CRASH_REBOUND"
            rebound_buys += int(is_probe)
            chop_buys += int(bool(state.metadata.get("chop_high_vol")))
            cp_buys += int(bool(state.metadata.get("capital_preservation")))
            reserve_used_notional = max(reserve_used_notional, reserve)
            if is_probe:
                state = update_adaptive_policy_state(
                    state=state, trading_date=bar.trading_date, overlay=overlay, config=config,
                    actual_fill_evidence=ActualFillEvidence(bar.trading_date))
            if core >= config.core_capital_usd and core_exhaustion_date is None: core_exhaustion_date = bar.trading_date
        elif decision.action == Action.SELL:
            cash += decision.notional; qty = 0; cost = 0
            if cycle_start is not None: completion_durations.append(index - cycle_start)
            state = InfiniteState(status=Status.COMPLETE, last_exit_date=bar.trading_date)
            cycle_start = None
        if state.reserve_unlocked and reserve_unlock_date is None: reserve_unlock_date = bar.trading_date
        equity = cash + qty * (bar.tqqq_close if bar.tqqq_close > 0 else 0)
        peak_equity = max(peak_equity, equity)
        mdd = min(mdd, equity / peak_equity - 1 if peak_equity else 0)
        lowest_cash = min(lowest_cash, cash)
        lowest_units = min(lowest_units, int(max(0, cash) // config.unit_usd))

    final_equity = cash + qty * (bars[-1].tqqq_close if bars else 0)
    return {
        "total_return": final_equity / config.max_total_capital_usd - 1,
        "mdd": mdd, "lowest_cash": lowest_cash, "lowest_remaining_units": lowest_units,
        "core_exhaustion_date": core_exhaustion_date, "reserve_unlock_date": reserve_unlock_date,
        "cycle_count": cycle_count, "take_profit_cycle_count": len(completion_durations),
        "take_profit_completion_sessions": completion_durations,
        "average_cycle_completion_sessions": (sum(completion_durations) / len(completion_durations)
                                                if completion_durations else None),
        "maximum_cycle_completion_sessions": max(completion_durations) if completion_durations else None,
        "crash_pending_buy_count": pending_buys, "crash_confirmed_buy_count": confirmed_buys,
        "rebound_probe_buy_count": rebound_buys, "chop_buy_count": chop_buys,
        "capital_preservation_buy_count": cp_buys,
        "reserve_used_notional": reserve_used_notional,
        "hard_cap_violation_count": hard_cap_violations,
        "daily_cap_violation_count": daily_cap_violations,
        "invalid_quote_order_count": invalid_orders,
        "duplicate_same_day_buy_count": duplicate_buys,
        "session_count": len(dates), "session_dates": dates,
        "final_state": state,
    }
