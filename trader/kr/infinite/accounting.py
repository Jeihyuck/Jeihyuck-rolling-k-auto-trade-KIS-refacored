import math
from .models import State

def validate_invariants(state: State, broker_qty: int) -> None:
    if broker_qty < 0: raise ValueError("KR_INF_BROKER_QTY_INVALID")
    if not (0 <= state.units_used <= 40 and 0 <= state.core_units_used <= 30 and 0 <= state.reserve_units_used <= 10):
        raise ValueError("KR_INF_UNIT_INVARIANT")
    if state.core_units_used + state.reserve_units_used != state.units_used: raise ValueError("KR_INF_UNIT_SUM_INVARIANT")
    if min(state.core_filled_notional, state.reserve_filled_notional) < 0 or state.filled_notional > state.allocated_capital_krw + .01:
        raise ValueError("KR_INF_CAPITAL_INVARIANT")

def buy_quantity(state: State, orderable_cash: float, price: float) -> tuple[int,float]:
    available=max(0.0,min(orderable_cash,state.allocated_capital_krw-state.filled_notional))
    budget=min(state.unit_krw,available)
    qty=math.floor(budget/price) if price > 0 else 0
    return qty, qty*price
