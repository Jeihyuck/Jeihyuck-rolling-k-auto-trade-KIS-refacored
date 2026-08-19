import math
from decimal import Decimal
from dataclasses import replace
from datetime import date
from .models import BrokerOrderState, OrderIntent, State, Status

def validate_invariants(state: State, broker_qty: int) -> None:
    if broker_qty < 0: raise ValueError("KR_INF_BROKER_QTY_INVALID")
    if not (0 <= state.units_used <= 40 and 0 <= state.core_units_used <= 30 and 0 <= state.reserve_units_used <= 10):
        raise ValueError("KR_INF_UNIT_INVARIANT")
    if state.core_units_used + state.reserve_units_used != state.units_used: raise ValueError("KR_INF_UNIT_SUM_INVARIANT")
    filled = Decimal(str(state.filled_notional))
    allocated = Decimal(str(state.allocated_capital_krw))
    if min(Decimal(str(state.core_filled_notional)), Decimal(str(state.reserve_filled_notional))) < 0 or filled > allocated + Decimal("0.01"):
        raise ValueError("KR_INF_CAPITAL_INVARIANT")

def buy_quantity(state: State, orderable_cash: float, price: float) -> tuple[int,float]:
    available=max(Decimal("0"), min(Decimal(str(orderable_cash)), Decimal(str(state.allocated_capital_krw))-Decimal(str(state.filled_notional))))
    budget=min(Decimal(str(state.unit_krw)),available)
    price_decimal = Decimal(str(price))
    qty=int(budget / price_decimal) if price_decimal > 0 else 0
    return qty, qty*price

def apply_confirmed_fill(state: State, intent: OrderIntent, broker: BrokerOrderState,
                         trade_date: date) -> tuple[State, int, float]:
    """Apply only the newly confirmed portion of a broker fill."""
    delta_qty = max(0, broker.filled_qty - intent.filled_qty)
    delta_notional = max(0.0, broker.filled_notional_krw - intent.filled_notional_krw)
    if delta_qty <= 0 or delta_notional <= 0 or intent.side not in {"BUY", "RECOVERY"}:
        return state, 0, 0.0
    new_unit = intent.filled_qty == 0
    sequence = intent.unit_sequence or (state.units_used + (1 if new_unit else 0))
    if sequence > 40:
        raise ValueError("KR_INF_UNIT_INVARIANT")
    reserve = sequence > 30
    price = delta_notional / delta_qty
    metadata = {**state.metadata, "fill_balance_pending": True,
                "fill_balance_pending_attempts": int(state.metadata.get("fill_balance_pending_attempts") or 0)}
    return replace(
        state,
        units_used=state.units_used + (1 if new_unit else 0),
        core_units_used=state.core_units_used + (1 if new_unit and not reserve else 0),
        reserve_units_used=state.reserve_units_used + (1 if new_unit and reserve else 0),
        core_filled_notional=state.core_filled_notional + (0 if reserve else delta_notional),
        reserve_filled_notional=state.reserve_filled_notional + (delta_notional if reserve else 0),
        last_buy_date=trade_date,
        last_buy_price=price,
        recovery_probe_done=state.recovery_probe_done or intent.side == "RECOVERY",
        reserve_unlocked=state.reserve_unlocked or intent.side == "RECOVERY",
        status=Status.ACTIVE,
        metadata=metadata,
    ), delta_qty, delta_notional
