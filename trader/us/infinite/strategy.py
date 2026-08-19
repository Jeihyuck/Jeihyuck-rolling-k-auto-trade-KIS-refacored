from __future__ import annotations

import math
from datetime import date

from .config import InfiniteConfig
from .models import Action, Decision, InfiniteState, PositionSnapshot, Status
from .risk_adapter import assess_market_risk


def _trading_days_since(start: date | None, end: date,
                        trading_sessions: frozenset[date] | None = None) -> int:
    if not start or start >= end:
        return 0
    from datetime import timedelta
    from trader.us.market_calendar import is_us_trading_day
    cursor, count = start, 0
    while cursor < end:
        cursor += timedelta(days=1)
        count += int(cursor in trading_sessions if trading_sessions is not None else is_us_trading_day(cursor))
    return count


def classify_long_trend(overlay: dict | None, structural_bear_seen: bool = False) -> str:
    o = overlay or {}
    try:
        close, ma50, ma200 = (float(o[k]) for k in ("qqq_completed_close", "qqq_ma50", "qqq_ma200"))
        slope, ret20 = float(o["qqq_ma200_slope"]), float(o["qqq_20d_return"])
    except (KeyError, TypeError, ValueError):
        return "TRANSITION"
    state = str(o.get("market_state") or "")
    if structural_bear_seen and close > ma50 and ret20 > 0 and state not in {
        "DEFENSE_CRASH_PENDING", "DEFENSE_CRASH_CONFIRMED"
    }:
        return "RECOVERY"
    if close > ma200 and ma50 > ma200 and slope > 0:
        return "BULL"
    if close < ma200 and ma50 < ma200 and slope < 0:
        return "BEAR"
    return "TRANSITION"


def evaluate(*, config: InfiniteConfig, state: InfiniteState | None, position: PositionSnapshot,
             trading_date: date, pending_buy: bool = False, pending_sell: bool = False,
             daily_filled_buy_notional: float = 0.0, overlay: dict | None = None,
             trading_sessions: frozenset[date] | None = None,
             entry_allowed: bool = True, buy_multiplier: float = 1.0,
             regime_reserve_permission: bool = True,
             effective_regime_name: str = "") -> Decision:
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
        recovered_notional = position.qty * position.average_price
        state = InfiniteState(
            symbol=config.symbol, cycle_id="SYMBOL_INVARIANT_RECOVERY", status=Status.ACTIVE,
            anchor_price=position.average_price or None,
            core_filled_notional=min(recovered_notional, config.core_capital_usd),
            reserve_filled_notional=max(0.0, recovered_notional - config.core_capital_usd),
            metadata={"strategy_owner": "TQQQ_INFINITE", "strategy_name": "TQQQ_INFINITE",
                      "strategy_version": config.policy_version, "sleeve_id": "TQQQ_INFINITE",
                      "ownership_source": "SYMBOL_INVARIANT_RECOVERY"},
        )
    if state is None:
        state = InfiniteState(symbol=config.symbol)

    if not math.isfinite(position.price) or position.price <= 0:
        return Decision(Action.BLOCK, "tqqq_price_unavailable")

    # EXIT is evaluated before every BUY pause and remains active during age/DD pauses.
    target = position.average_price * (1 + config.take_profit_pct)
    if position.qty > 0 and position.average_price > 0 and position.price + 1e-9 >= target:
        if not config.allow_sell:
            return Decision(Action.BLOCK, "sell_permission_disabled")
        if pending_sell:
            return Decision(Action.BLOCK, "tqqq_full_exit_pending", next_status=Status.EXIT_PENDING)
        if position.orderable_qty is None:
            return Decision(Action.BLOCK, "tqqq_orderable_qty_missing")
        if position.orderable_qty <= 0:
            return Decision(Action.BLOCK, "tqqq_no_orderable_qty")
        if position.orderable_qty != position.qty:
            return Decision(Action.BLOCK, "tqqq_full_exit_qty_not_ready")
        sell_qty = position.qty
        return Decision(Action.SELL, "take_profit", qty=sell_qty,
                        notional=sell_qty * position.price, next_status=Status.EXIT_PENDING)
    if state.status == Status.EXIT_PENDING:
        if position.qty == 0:
            return Decision(Action.WAIT, "confirmed_exit_complete", next_status=Status.COMPLETE)
        return Decision(Action.WAIT, "exit_pending", next_status=Status.EXIT_PENDING)
    if position.qty == 0 and state.status == Status.COMPLETE and state.last_exit_date == trading_date:
        return Decision(Action.BLOCK, "same_day_cycle_restart")
    if pending_sell:
        return Decision(Action.BLOCK, "tqqq_pending_order_exists")
    if pending_buy:
        return Decision(Action.BLOCK, "tqqq_pending_order_exists")
    if not entry_allowed:
        if config.symbol != "TQQQ" or not (overlay and (overlay.get("intraday_market_overlay") or overlay.get("intraday_rotation_overlay"))):
            return Decision(Action.BLOCK, "tqqq_effective_regime_entry_block")
    if not config.allow_buy:
        return Decision(Action.BLOCK, "buy_permission_paused")
    if state.last_buy_date == trading_date or daily_filled_buy_notional >= config.max_daily_buy_usd - 1e-6:
        return Decision(Action.BLOCK, "daily_buy_limit")
    overlay = dict(overlay or {})
    infinite_overlay_bypass = config.symbol == "TQQQ" and bool(
        overlay.get("intraday_market_overlay") or overlay.get("intraday_rotation_overlay")
    )
    if infinite_overlay_bypass:
        overlay["strategy_owner"] = "TQQQ_INFINITE"
    risk = assess_market_risk(overlay)
    if not risk.allow_buy and not infinite_overlay_bypass:
        return Decision(Action.BLOCK, risk.reason)
    required = {
        "qqq_completed_close": True, "qqq_ma50": True, "qqq_ma200": True,
        "qqq_ma200_slope": False, "qqq_20d_return": False, "qqq_drawdown_252": False,
        "qqq_realized_vol_20d": False, "qqq_trend_efficiency_20d": False,
    }
    valid_context = overlay.get("tqqq_context_quality") == "ok"
    valid_context = valid_context and not any(bool(overlay.get(flag)) for flag in (
        "qqq_stale", "qqq_suspect", "qqq_degraded", "tqqq_context_stale",
        "tqqq_context_suspect", "tqqq_context_degraded",
    ))
    for field, positive in required.items():
        try:
            value = float(overlay[field])
            valid_context = valid_context and math.isfinite(value) and (value > 0 if positive else True)
            if field == "qqq_realized_vol_20d":
                valid_context = valid_context and value >= 0
        except (KeyError, TypeError, ValueError):
            valid_context = False
    if not valid_context:
        return Decision(Action.BLOCK, "tqqq_required_market_data_missing")
    if not infinite_overlay_bypass and position.qty <= 0 and effective_regime_name in {
        "RISK_OFF", "DEFENSIVE", "CHOP_HIGH_VOL", "CAPITAL_PRESERVATION"
    }:
        return Decision(Action.BLOCK, "tqqq_regime_new_cycle_block")
    if not infinite_overlay_bypass and overlay.get("force_entry_block") is True:
        return Decision(Action.BLOCK, "overlay_force_entry_block")
    if not infinite_overlay_bypass and position.qty <= 0 and overlay.get("allow_new_buy") is False:
        return Decision(Action.BLOCK, "overlay_new_buy_block")
    if position.qty > 0 and overlay.get("allow_add_to_existing") is False:
        # Standard gross/cluster overlays do not own this sleeve. Only an
        # explicitly TQQQ-scoped capital flag may pause its adds.
        if overlay.get("tqqq_capital_preservation"):
            return Decision(Action.BLOCK, "tqqq_capital_preservation")

    metadata = state.metadata or {}
    policy_regime = effective_regime_name or risk.market_state
    if infinite_overlay_bypass and (overlay.get("intraday_market_overlay") or overlay.get("intraday_rotation_overlay")):
        policy_regime = str(overlay.get("base_market_state") or "NORMAL")
    recovery_uncertain = bool(position.qty > 0 and metadata.get("recovery_accounting_uncertain")
                              and not metadata.get("last_buy_fill_price"))
    # Uncertain recovery may resume conservative core buys, but can never use
    # reserve or a bullish/recovery premium until attributed fill evidence is
    # found.  The broker average remains a decision reference, never a fill.
    effective_reserve_available = bool(state.reserve_unlocked and regime_reserve_permission
                                       and not recovery_uncertain)
    policy_overlay = {**overlay, "market_state": policy_regime}
    long_trend = str(metadata.get("long_trend") or classify_long_trend(
        policy_overlay, bool(metadata.get("structural_bear_seen"))))
    try:
        drawdown = float((overlay or {}).get("qqq_drawdown_252"))
    except (TypeError, ValueError):
        drawdown = 0.0
    try:
        rv20 = float((overlay or {}).get("qqq_realized_vol_20d"))
        efficiency = float((overlay or {}).get("qqq_trend_efficiency_20d"))
        chop = policy_regime == "CHOP_HIGH_VOL" or (
            rv20 >= config.chop_rv20_min and efficiency <= config.chop_efficiency_max)
    except (TypeError, ValueError):
        chop = False
    capital_preservation = policy_regime == "CAPITAL_PRESERVATION" or (long_trend == "BEAR" and (
        drawdown <= config.capital_preservation_drawdown
        or state.cycle_age_trading_days >= config.max_cycle_age_trading_days
        or state.core_filled_notional >= config.capital_preservation_core_used
    ))
    last_price = metadata.get("last_buy_fill_price") or metadata.get("buy_reference_price")
    try: last_price = float(last_price) if last_price is not None else None
    except (TypeError, ValueError): last_price = None
    if recovery_uncertain:
        try:
            conservative_reference = float(metadata.get("buy_reference_price") or position.average_price)
        except (TypeError, ValueError):
            conservative_reference = 0.0
        if conservative_reference <= 0:
            return Decision(Action.BLOCK, "tqqq_recovery_reconcile_required")
        if position.price > conservative_reference:
            return Decision(Action.WAIT, "tqqq_recovery_price_above_broker_average")
    days_since = (_trading_days_since(state.last_buy_date, trading_date, trading_sessions)
                  if state.last_buy_date else 10_000)

    if policy_regime == "DEFENSE_CRASH_REBOUND":
        probe_date = metadata.get("rebound_probe_date")
        try:
            probe_date = date.fromisoformat(str(probe_date)) if probe_date else None
        except ValueError:
            probe_date = trading_date
        if position.qty <= 0 or state.core_filled_notional >= config.core_capital_usd or (
            probe_date and _trading_days_since(probe_date, trading_date, trading_sessions) < config.rebound_cooldown
        ):
            return Decision(Action.BLOCK, "rebound_cooldown")
    elif capital_preservation:
        allowed = (position.qty > 0
                   and days_since >= config.capital_preservation_gap and last_price
                   and position.price <= last_price * (1 - config.capital_preservation_step)
                   and position.average_price > 0 and position.price <= position.average_price
                   and state.reserve_filled_notional <= 0)
        if not allowed:
            return Decision(Action.BLOCK, "capital_preservation_wait")
    elif chop:
        if position.qty <= 0:
            return Decision(Action.BLOCK, "chop_high_vol_new_cycle_block")
        allowed = (days_since >= config.chop_gap and last_price
                   and position.price <= last_price * (1 - config.chop_step)
                   and position.average_price > 0 and position.price <= position.average_price)
        if not allowed:
            return Decision(Action.BLOCK, "chop_high_vol_wait")
    elif policy_regime in {"RISK_OFF", "DEFENSIVE"}:
        allowed = (position.qty > 0 and long_trend == "BEAR" and days_since >= config.bear_fallback_gap
                   and last_price and position.price <= last_price * (1 - config.bear_step)
                   and position.average_price > 0 and position.price <= position.average_price)
        if not allowed:
            return Decision(Action.BLOCK, "defense_risk_off_wait")
    elif long_trend == "BEAR" and position.qty > 0:
        step = days_since >= config.bear_gap and last_price and position.price <= last_price * (1 - config.bear_step)
        fallback = days_since >= config.bear_fallback_gap and position.average_price > 0 and position.price <= position.average_price
        if not (step or fallback):
            return Decision(Action.BLOCK, "bear_runway_wait")
    elif position.qty > 0:
        gap = 3 if policy_regime == "DEFENSE_CAUTION" else (1 if policy_regime in {"STRONG_RISK_ON", "RISK_ON"} else 2)
        if days_since < gap:
            return Decision(Action.BLOCK, "routine_gap_wait")
        premium = (0.02 if effective_reserve_available and long_trend == "RECOVERY" else
                   config.buy_premium_pct if long_trend == "BULL" and policy_regime in {"STRONG_RISK_ON", "RISK_ON"} else 0.0)
        if recovery_uncertain:
            premium = 0.0
        if position.average_price <= 0 or position.price > position.average_price * (1 + premium):
            return Decision(Action.WAIT, "price_above_buy_premium")

    if effective_reserve_available and state.core_filled_notional >= config.core_capital_usd and position.qty > 0:
        if long_trend != "RECOVERY" or days_since < 2 or position.price > position.average_price * 1.02:
            return Decision(Action.BLOCK, "recovery_reserve_wait")

    core_left = max(0.0, config.core_capital_usd - state.core_filled_notional)
    reserve_left = max(0.0, config.reserve_capital_usd - state.reserve_filled_notional)
    if state.core_filled_notional >= config.routine_core_usd and state.core_filled_notional < config.core_capital_usd:
        deep_bear = bool(metadata.get("deep_bear_unlocked")) or (
            long_trend == "BEAR" and drawdown <= config.deep_bear_unlock_drawdown
        )
        if not deep_bear:
            return Decision(Action.BLOCK, "deep_bear_core_locked")
    if core_left <= 0 and not effective_reserve_available:
        return Decision(Action.BLOCK, "reserve_locked")
    available = core_left if core_left > 0 else reserve_left
    total_left = config.max_total_capital_usd - state.total_filled_notional
    daily_left = config.max_daily_buy_usd - daily_filled_buy_notional
    effective_buy_multiplier = 1.0 if infinite_overlay_bypass else buy_multiplier
    budget = min(config.unit_usd * max(0.0, effective_buy_multiplier), available, total_left, daily_left)
    qty = math.floor(budget / position.price)
    notional = qty * position.price
    if qty < 1:
        return Decision(Action.BLOCK, "unit_cannot_buy_whole_share")
    if state.total_filled_notional + notional > config.max_total_capital_usd + 1e-6:
        return Decision(Action.BLOCK, "hard_cap_exceeded")
    return Decision(Action.BUY, "first_buy" if position.qty == 0 else "average_buy", qty=qty,
                    notional=notional, next_status=Status.ACTIVE if core_left > 0 else Status.RESERVE)
