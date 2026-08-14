from dataclasses import replace
from datetime import date
from .accounting import validate_invariants
from .models import BrokerPosition, State, Status

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
        if position.qty>0:return state,"EXIT_PENDING"
        if pending_sell:return state,"SELL_RECONCILE_PENDING"
        return replace(state,status=Status.COMPLETE,cycle_complete_date=trade_date,last_exit_date=trade_date),"CYCLE_COMPLETE"
    return state,"OK"
