from __future__ import annotations

import math
from datetime import date

from .config import InfiniteConfig
from .models import Action, Decision, InfiniteState, PositionSnapshot, Status
from .risk_adapter import assess_market_risk


def evaluate(*, config: InfiniteConfig, state: InfiniteState | None, position: PositionSnapshot,
             trading_date: date, pending_buy: bool = False, pending_sell: bool = False,
             daily_filled_buy_notional: float = 0.0, overlay: dict | None = None) -> Decision:
    """Pure exit-first strategy decision; broker position is always authoritative."""
    if not config.enabled:
        return Decision(Action.WAIT, "feature_disabled")
    try:
        config.validate()
        if state is not None:
            state.validate(config.max_total_capital_usd)
    except (TypeError, ValueError) as exc:
        return Decision(Action.BLOCK, f"state_or_config_corrupt:{exc}")
    if position.qty > 0 and state is None:
        return Decision(Action.BLOCK, "orphan_position")
    if state is None:
        state = InfiniteState(symbol=config.symbol)

    # EXIT is evaluated before every BUY pause and remains active during age/DD pauses.
    target = position.average_price * (1 + config.take_profit_pct)
    if position.qty > 0 and position.average_price > 0 and position.price + 1e-9 >= target:
        if not config.allow_sell:
            return Decision(Action.BLOCK, "sell_permission_disabled")
        if pending_sell:
            return Decision(Action.BLOCK, "pending_sell", next_status=Status.EXIT_PENDING)
        return Decision(Action.SELL, "take_profit", qty=position.qty,
                        notional=position.qty * position.price, next_status=Status.EXIT_PENDING)
    if state.status == Status.EXIT_PENDING:
        if position.qty == 0:
            return Decision(Action.WAIT, "confirmed_exit_complete", next_status=Status.COMPLETE)
        return Decision(Action.WAIT, "exit_pending", next_status=Status.EXIT_PENDING)
    if position.qty == 0 and state.status == Status.COMPLETE and state.last_exit_date == trading_date:
        return Decision(Action.BLOCK, "same_day_cycle_restart")
    if pending_sell:
        return Decision(Action.BLOCK, "pending_sell")
    if pending_buy:
        return Decision(Action.BLOCK, "pending_buy")
    if not config.allow_buy:
        return Decision(Action.BLOCK, "buy_permission_paused")
    if state.last_buy_date == trading_date or daily_filled_buy_notional >= config.max_daily_buy_usd - 1e-6:
        return Decision(Action.BLOCK, "daily_buy_limit")
    if state.cycle_age_trading_days > config.max_cycle_age_trading_days:
        return Decision(Action.BLOCK, "cycle_age_pause", next_status=Status.PAUSED_AGE)
    anchor = state.anchor_price or (position.average_price if position.average_price > 0 else None)
    if anchor and position.price / anchor - 1 <= config.drawdown_pause_pct:
        return Decision(Action.BLOCK, "drawdown_pause", next_status=Status.PAUSED_DRAWDOWN)

    risk = assess_market_risk(overlay)
    if not risk.allow_buy:
        return Decision(Action.BLOCK, risk.reason)
    if risk.market_crash and state.market_crash_streak >= 2:
        return Decision(Action.BLOCK, "market_crash_day_3", next_status=Status.PAUSED_CRASH)
    if position.qty > 0 and (position.average_price <= 0 or position.price > position.average_price * (1 + config.buy_premium_pct)):
        return Decision(Action.WAIT, "price_above_buy_premium")

    core_left = max(0.0, config.core_capital_usd - state.core_filled_notional)
    reserve_left = max(0.0, config.reserve_capital_usd - state.reserve_filled_notional)
    if core_left <= 0 and not state.reserve_unlocked:
        return Decision(Action.BLOCK, "reserve_locked")
    available = core_left if core_left > 0 else reserve_left
    total_left = config.max_total_capital_usd - state.total_filled_notional
    daily_left = config.max_daily_buy_usd - daily_filled_buy_notional
    budget = min(config.unit_usd, available, total_left, daily_left)
    if position.price <= 0:
        return Decision(Action.BLOCK, "executable_price_unavailable")
    qty = math.floor(budget / position.price)
    notional = qty * position.price
    if qty < 1:
        return Decision(Action.BLOCK, "unit_cannot_buy_whole_share")
    if state.total_filled_notional + notional > config.max_total_capital_usd + 1e-6:
        return Decision(Action.BLOCK, "hard_cap_exceeded")
    return Decision(Action.BUY, "first_buy" if position.qty == 0 else "average_buy", qty=qty,
                    notional=notional, next_status=Status.ACTIVE if core_left > 0 else Status.RESERVE)
