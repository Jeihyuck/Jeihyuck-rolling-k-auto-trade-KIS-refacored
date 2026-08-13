from __future__ import annotations

import hashlib
import math
from datetime import date, datetime, timezone

from .config import InfiniteConfig
from .models import Action, BrokerPosition, Decision, InfiniteState, Quote
from .risk_adapter import RegimeAction


def client_order_key(state: InfiniteState, trade_date: date, side: str) -> str:
    raw = f"{state.strategy_id}:{state.book}:{state.cycle_id or 'NEW'}:{trade_date}:{side}"
    return "KRINF:" + hashlib.sha256(raw.encode()).hexdigest()[:32]


def evaluate(*, config: InfiniteConfig, state: InfiniteState | None, broker: BrokerPosition,
             quote: Quote, trade_date: date, regime: RegimeAction, pending: bool,
             daily_filled_buy_notional: float, ownership_conflict: bool = False,
             reconciled: bool = True) -> Decision:
    now = datetime.now(timezone.utc)
    if ownership_conflict or (broker.quantity > 0 and state is None):
        return Decision(Action.BLOCK, "OWNERSHIP_CONFLICT")
    state = state or InfiniteState(symbol=config.symbol, policy_version=config.policy_version)
    if not reconciled or state.filled_quantity != broker.quantity:
        return Decision(Action.BLOCK, "RECONCILE_REQUIRED")
    if not math.isfinite(quote.price) or quote.price <= 0 or quote.observed_at > now or (now - quote.observed_at).total_seconds() > config.quote_max_age_seconds:
        return Decision(Action.BLOCK, "INVALID_OR_STALE_QUOTE")
    # Authoritative average cost is never replaced by the current quote.
    if broker.quantity > 0 and broker.average_price <= 0:
        return Decision(Action.BLOCK, "MISSING_AUTHORITATIVE_AVERAGE")
    if broker.quantity > 0 and config.allow_sell and quote.price >= broker.average_price * (1 + config.take_profit_pct):
        return Decision(Action.SELL, "TAKE_PROFIT_SELL_FIRST", broker.quantity,
                        broker.quantity * quote.price, client_order_key(state, trade_date, "SELL"))
    if not config.allow_buy:
        return Decision(Action.BLOCK, "BUY_DISABLED")
    if not config.deployable_buy_policy:
        return Decision(Action.BLOCK, "NO_DEPLOYABLE_POLICY")
    if not regime.allow_buy:
        return Decision(Action.BLOCK, regime.decision)
    if pending or state.pending_order_key:
        return Decision(Action.BLOCK, "PENDING_ORDER")
    if state.last_buy_trade_date == trade_date or daily_filled_buy_notional >= config.unit_krw:
        return Decision(Action.BLOCK, "DAILY_UNIT_LIMIT")
    remaining_daily = max(0.0, config.unit_krw - daily_filled_buy_notional)
    remaining_cap = max(0.0, config.capital_krw - state.authoritative_buy_notional)
    budget = min(remaining_daily, remaining_cap, broker.orderable_cash)
    qty = int(budget / (quote.price * (1 + config.buy_fee_rate)))
    cost = qty * quote.price * (1 + config.buy_fee_rate)
    if state.used_unit_fraction >= config.units or qty <= 0 or cost > remaining_cap + 1e-6:
        return Decision(Action.BLOCK, "BLOCK_BY_CAP")
    return Decision(Action.BUY, regime.decision, qty, cost, client_order_key(state, trade_date, "BUY"))
