from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from .config import InfiniteConfig
from .models import CycleStatus, Decision, MarketInput, SleeveState
from .policy import integer_buy_quantity, net_liquidation_return
from .risk_adapter import REGIME_POLICY, validate_regime


def decide(config: InfiniteConfig, state: SleeveState, market: MarketInput, now: datetime) -> Decision:
    if not config.enabled: return Decision("BLOCK", "SLEEVE_DISABLED")
    if state.status == CycleStatus.OWNERSHIP_CONFLICT: return Decision("BLOCK", "OWNERSHIP_CONFLICT")
    if not state.reconciled: return Decision("BLOCK", "RECONCILE_MISMATCH")
    if market.quote_price <= 0: return Decision("BLOCK", "QUOTE_INVALID")
    qtime = market.quote_at if market.quote_at.tzinfo else market.quote_at.replace(tzinfo=timezone.utc)
    current = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    if qtime > current or current - qtime > timedelta(minutes=2): return Decision("BLOCK", "QUOTE_STALE")
    # Safety/target exits precede pending-order and entry evaluation.
    if state.filled_quantity and config.allow_sell:
        pnl = net_liquidation_return(
            market.quote_price, state.filled_quantity, state.buy_notional, config, state.sell_notional
        )
        if market.kill_switch: return Decision("SELL", "CATASTROPHIC_SAFETY_EXIT", state.filled_quantity, sell_kind="SAFETY")
        if pnl >= config.target_net_return: return Decision("SELL", "TARGET_NET_RETURN", state.filled_quantity, sell_kind="PROFIT_FULL")
        if market.long_trend_broken and market.regime_state in REGIME_POLICY and REGIME_POLICY[market.regime_state][0] == 0:
            return Decision("SELL", "DEFENSE_RISK_REDUCTION", max(1, state.filled_quantity // 2), sell_kind="DEFENSE_PARTIAL")
    if state.pending_order_key: return Decision("BLOCK", "PENDING_ORDER")
    if not config.allow_buy: return Decision("BLOCK", "BUY_DISABLED")
    regime_error = validate_regime(market, now)
    if regime_error: return Decision("BLOCK", regime_error)
    if state.last_buy_date == market.trade_date: return Decision("BLOCK", "DAILY_BUY_FILL_LIMIT")
    if state.last_sell_date == market.trade_date: return Decision("BLOCK", "SAME_DAY_SELL_REBUY")
    fraction_raw, exposure_cap = REGIME_POLICY[market.regime_state]
    if market.regime_state == "KR_DEFENSE_CAUTION" and not market.recovery_confirmed:
        return Decision("BLOCK", "RECOVERY_NOT_CONFIRMED")
    fraction = Decimal(str(fraction_raw))
    used_units = state.buy_notional / config.unit_krw
    if used_units >= config.units: return Decision("BLOCK", "UNIT_CAP")
    if state.buy_notional >= config.capital_krw: return Decision("BLOCK", "CAPITAL_CAP")
    if used_units >= Decimal(exposure_cap): return Decision("BLOCK", "REGIME_EXPOSURE_CAP")
    fraction = min(fraction, Decimal(exposure_cap) - used_units, Decimal(1))
    remaining = config.capital_krw - state.buy_notional
    qty = integer_buy_quantity(market.quote_price, fraction, remaining, market.orderable_cash, config)
    if qty <= 0: return Decision("BLOCK", "ZERO_QUANTITY")
    return Decision("BUY", "REGIME_ENTRY", qty, fraction)
