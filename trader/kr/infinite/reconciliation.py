from dataclasses import replace
from datetime import date
from .accounting import validate_invariants
from .models import BrokerPosition, State, Status


def reconcile_order_fill_prices(*, order_price: float, order_qty: int,
                                broker_order_no: str|None = None,
                                fill_price: float|None = None,
                                fill_qty: int|None = None,
                                broker_avg_after: float|None = None,
                                close_avg_price: float|None = None) -> dict:
    """Join KR infinite order, fill, and balance-average evidence."""
    base = float(order_price or 0)
    after = float(broker_avg_after or 0)
    mismatch = bool(base > 0 and after > 0 and abs(base - after) / base >= 0.01)
    return {
        "order_price": base,
        "order_qty": int(order_qty or 0),
        "broker_order_no": broker_order_no,
        "fill_price": fill_price,
        "fill_qty": fill_qty,
        "broker_avg_after": broker_avg_after,
        "close_avg_price": close_avg_price,
        "reconciliation_status": "KR_INF_ORDER_FILL_PRICE_MISMATCH" if mismatch else "OK",
        "price_mismatch_pct": abs(base - after) / base if mismatch else 0.0,
        "warning": "KR_INF_ORDER_FILL_PRICE_MISMATCH" if mismatch else None,
    }

def reconcile(state:State|None,position:BrokerPosition,trade_date:date,pending_sell:bool=False,
              balance_grace_attempts:int=3)->tuple[State|None,str]:
    if state is None and position.qty>0:return None,"KR_INF_UNOWNED_EXISTING_POSITION"
    if state is None:return None,"READY"
    try: validate_invariants(state,position.qty)
    except ValueError as e:return replace(state,status=Status.FROZEN),str(e)
    if position.qty>0 and position.average_price<=0:return replace(state,status=Status.FROZEN),"KR_INF_AVG_PRICE_INVALID"
    if state.status==Status.COMPLETE and position.qty>0:return replace(state,status=Status.FROZEN),"KR_INF_STATE_POSITION_MISMATCH"
    if state.status==Status.ACTIVE and position.qty==0:
        metadata=dict(state.metadata)
        if metadata.get("fill_balance_pending"):
            attempts=int(metadata.get("fill_balance_pending_attempts") or 0)+1
            metadata["fill_balance_pending_attempts"]=attempts
            if attempts <= balance_grace_attempts:
                return replace(state,metadata=metadata),"FILL_CONFIRMED_BALANCE_PENDING"
        return replace(state,status=Status.FROZEN),"KR_INF_STATE_POSITION_MISMATCH"
    if state.status==Status.ACTIVE and position.qty>0 and state.metadata.get("fill_balance_pending"):
        metadata=dict(state.metadata);metadata.pop("fill_balance_pending",None);metadata.pop("fill_balance_pending_attempts",None)
        state=replace(state,metadata=metadata)
    if state.status==Status.EXIT_PENDING:
        if position.qty>0:
            # A terminal partial-profit fill leaves the macro cycle open when
            # the freshly fetched broker balance still has a residual holding.
            # ACK/PENDING and intermediate partial fills retain EXIT_PENDING.
            if (not state.metadata.get("pending_profit_stage")
                    and str(state.metadata.get("profit_stage") or "").upper().endswith("_FILLED")):
                return replace(state,status=Status.ACTIVE),"PARTIAL_PROFIT_FILLED_RESIDUAL"
            return state,"EXIT_PENDING"
        if pending_sell:return state,"SELL_RECONCILE_PENDING"
        return replace(state,status=Status.COMPLETE,cycle_complete_date=trade_date,last_exit_date=trade_date),"CYCLE_COMPLETE"
    return state,"OK"
