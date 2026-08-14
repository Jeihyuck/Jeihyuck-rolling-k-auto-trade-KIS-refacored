from __future__ import annotations
from datetime import date
from .accounting import buy_quantity, validate_invariants
from .config import InfiniteConfig
from .models import Action, BrokerPosition, Decision, State, Status
from .policy_state import idempotency_key
from .risk_adapter import allows_new_cycle, buy_pause_reason

def trading_days_since(start: date|None, end: date) -> int:
    if start is None or start >= end: return 0
    from datetime import timedelta
    from trader.time_utils import is_krx_trading_day
    cursor, count = start, 0
    while cursor < end:
        cursor += timedelta(days=1)
        count += int(is_krx_trading_day(cursor))
    return count

def evaluate(*, config: InfiniteConfig, state: State|None, position: BrokerPosition, trade_date: date, market_state: str|None,
             trading_days_since_last_buy: int=10000, orderable_cash: float=0, pending_buy: bool=False, pending_sell: bool=False,
             existing_intent_keys: frozenset[str]=frozenset(), regime_data_quality: str="OK",
             allow_entry: bool=True) -> Decision:
    try: config.validate()
    except ValueError as e: return Decision(Action.BLOCK,str(e),next_status=Status.FROZEN)
    if not config.enabled: return Decision(Action.WAIT,"KR_INF_FEATURE_DISABLED")
    if position.current_price <= 0: return Decision(Action.BLOCK,"KR_INF_MARKET_DATA_UNAVAILABLE",next_status=Status.FROZEN)
    if position.qty > 0 and position.average_price <= 0: return Decision(Action.BLOCK,"KR_INF_AVG_PRICE_INVALID",next_status=Status.FROZEN)
    if state is None and position.qty > 0: return Decision(Action.BLOCK,"KR_INF_UNOWNED_EXISTING_POSITION",next_status=Status.FROZEN)
    state=state or State()
    try: validate_invariants(state,position.qty)
    except ValueError as e: return Decision(Action.BLOCK,str(e),next_status=Status.FROZEN)
    if state.status == Status.FROZEN: return Decision(Action.BLOCK,"KR_INF_STATE_FROZEN",next_status=Status.FROZEN)
    if state.status == Status.COMPLETE and position.qty > 0: return Decision(Action.BLOCK,"KR_INF_STATE_POSITION_MISMATCH",next_status=Status.FROZEN)
    if state.status == Status.ACTIVE and (not state.cycle_id or position.qty == 0): return Decision(Action.BLOCK,"KR_INF_STATE_POSITION_MISMATCH",next_status=Status.FROZEN)
    if position.qty > 0 and position.current_price + 1e-9 >= position.average_price*(1+config.take_profit_pct):
        if pending_sell: return Decision(Action.WAIT,"PENDING_SELL",next_status=Status.EXIT_PENDING)
        key=idempotency_key(state.cycle_id or "MISSING",trade_date,"SELL_ALL")
        if key in existing_intent_keys: return Decision(Action.WAIT,"DUPLICATE_INTENT",next_status=Status.EXIT_PENDING)
        return Decision(Action.SELL_ALL,"TAKE_PROFIT",position.orderable_qty,position.orderable_qty*position.current_price,key,Status.EXIT_PENDING)
    if state.status == Status.EXIT_PENDING or pending_sell: return Decision(Action.WAIT,"EXIT_PENDING",next_status=Status.EXIT_PENDING)
    if not allow_entry:return Decision(Action.WAIT,"KR_INF_ENTRY_DISABLED_BY_SESSION")
    regime_pause=buy_pause_reason(market_state,regime_data_quality)
    if regime_pause:return Decision(Action.WAIT,regime_pause)
    if state.status == Status.COMPLETE and state.last_exit_date == trade_date and not config.same_day_restart: return Decision(Action.BLOCK,"SAME_DAY_CYCLE_RESTART_BLOCK")
    if pending_buy or state.last_buy_date == trade_date: return Decision(Action.WAIT,"DAILY_BUY_LIMIT")
    new=position.qty==0
    if new and not allows_new_cycle(market_state): return Decision(Action.WAIT,"NEW_CYCLE_REGIME_BLOCK")
    if state.units_used >= config.total_units: return Decision(Action.WAIT,"TOTAL_UNITS_EXHAUSTED")
    recovery=market_state=="KR_SHOCK_REBOUND_CONFIRMED" and (new or state.crash_seen)
    if recovery and state.recovery_probe_done: return Decision(Action.WAIT,"RECOVERY_PROBE_ALREADY_DONE")
    if market_state in {"KR_DEFENSE_CRASH","KR_SHOCK_REBOUND_PENDING"}: return Decision(Action.WAIT,"BUY_PAUSED_BY_REGIME")
    if not new and not recovery:
        p,a,l=position.current_price,position.average_price,state.last_buy_price
        if state.cycle_age_trading_days>=config.long_cycle_days:
            if not (l and p<=a and p<=l*(1-config.long_cycle_step_pct) and trading_days_since_last_buy>=config.long_cycle_gap_days): return Decision(Action.WAIT,"CAPITAL_PRESERVATION_WAIT")
        elif market_state in {"KR_STRONG_RISK_ON","KR_RISK_ON"}:
            if p>a*(1+config.risk_on_premium_pct): return Decision(Action.WAIT,"PRICE_ABOVE_RISK_ON_PREMIUM")
            if trading_days_since_last_buy<config.risk_on_gap_days: return Decision(Action.WAIT,"RISK_ON_CADENCE")
        elif market_state=="KR_NORMAL":
            if p>a: return Decision(Action.WAIT,"PRICE_ABOVE_NORMAL_AVG")
            if trading_days_since_last_buy<config.normal_gap_days: return Decision(Action.WAIT,"NORMAL_CADENCE")
        elif market_state=="KR_DEFENSE_CAUTION":
            if p>a*(1-config.caution_discount_pct): return Decision(Action.WAIT,"PRICE_ABOVE_CAUTION_DISCOUNT")
            if trading_days_since_last_buy<config.caution_gap_days: return Decision(Action.WAIT,"CAUTION_CADENCE")
        elif market_state=="KR_DEFENSE_RISK_OFF":
            if not l or p>a or p>l*(1-config.risk_off_step_pct): return Decision(Action.WAIT,"RISK_OFF_PRICE_STEP")
            if trading_days_since_last_buy<config.risk_off_gap_days: return Decision(Action.WAIT,"RISK_OFF_CADENCE")
    if state.units_used>=config.core_units and not (state.reserve_unlocked or (state.crash_seen and market_state in {"KR_SHOCK_REBOUND_CONFIRMED","KR_NORMAL","KR_RISK_ON","KR_STRONG_RISK_ON"})):
        return Decision(Action.WAIT,"RESERVE_LOCKED")
    qty,notional=buy_quantity(state,orderable_cash,position.current_price)
    if qty<=0: return Decision(Action.WAIT,"INSUFFICIENT_STRATEGY_CAPITAL")
    action=Action.RECOVERY if recovery else Action.BUY
    key=idempotency_key(state.cycle_id or "NEW",trade_date,"RECOVERY" if recovery else "BUY",state.units_used+1)
    if key in existing_intent_keys: return Decision(Action.WAIT,"DUPLICATE_INTENT")
    return Decision(action,"RECOVERY_PROBE" if recovery else ("NEW_CYCLE_BUY" if new else "ADAPTIVE_ADD_BUY"),qty,notional,key,Status.ACTIVE)
