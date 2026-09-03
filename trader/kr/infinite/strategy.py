from __future__ import annotations
import os
from datetime import date
from .accounting import buy_quantity, validate_invariants
from .config import InfiniteConfig
from .models import Action, BrokerPosition, Decision, State, Status
from .policy_state import idempotency_key
from .risk_adapter import allows_new_cycle, buy_pause_reason
from trader.kr.regime import STATE_ORDER


KR_ADAPTIVE_TP_MAP: dict[str, tuple[float, float, str]] = {
    "KR_DEFENSE_CRASH": (0.05, 0.85, "TP1_DEFENSE"),
    "KR_DEFENSE_RISK_OFF": (0.05, 0.85, "TP1_DEFENSE"),
    "KR_DEFENSE_CAUTION": (0.05, 0.85, "TP1_DEFENSE"),
    "KR_SHOCK_REBOUND_PENDING": (0.05, 0.70, "TP1_REBOUND_PENDING"),
    "KR_SHOCK_REBOUND_CONFIRMED": (0.06, 0.50, "TP1_REBOUND_CONFIRMED"),
    "KR_NORMAL": (0.06, 0.50, "TP1"),
    "KR_RISK_ON": (0.07, 0.50, "TP1"),
    "KR_STRONG_RISK_ON": (0.07, 0.50, "TP1"),
}

# Import-time failure makes STATE_ORDER the enforceable source of truth in every
# runtime and CI entry point, rather than relying only on a dedicated test.
if set(KR_ADAPTIVE_TP_MAP) != set(STATE_ORDER):
    raise RuntimeError("KR adaptive TP map must exactly cover trader.kr.regime.STATE_ORDER")


def adaptive_tp_stage(*, market_state: str | None, units_used: int, cycle_age_trading_days: int) -> tuple[float, float, str]:
    """Return (threshold_pct, sell_fraction, stage) for the current adaptive TP tier.

    Capital-recovery mode (deep cycle / stale age) takes priority: a small
    +3~5% recovery exit rather than waiting for the full +10% target.
    """
    if units_used >= 30 or cycle_age_trading_days >= 30:
        return 0.04, 1.0, "CAPITAL_RECOVERY"
    state = str(market_state or "").upper()
    if state not in KR_ADAPTIVE_TP_MAP:
        raise KeyError(f"KR_INF_UNMAPPED_REGIME:{state or '<empty>'}")
    return KR_ADAPTIVE_TP_MAP[state]

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
             allow_entry: bool=True, minutes_since_open: float|None=None,
             vwap: float|None=None, intraday_return: float|None=None,
             recent_3m_return: float|None=None, quote_stale: bool=False,
             best_ask: float|None=None) -> Decision:
    try: config.validate()
    except ValueError as e: return Decision(Action.BLOCK,str(e),next_status=Status.FROZEN)
    if not config.enabled: return Decision(Action.WAIT,"KR_INF_FEATURE_DISABLED")
    if position.current_price <= 0: return Decision(Action.BLOCK,"KR_INF_MARKET_DATA_UNAVAILABLE",next_status=Status.FROZEN)
    if position.qty > 0 and position.average_price <= 0: return Decision(Action.BLOCK,"KR_INF_AVG_PRICE_INVALID",next_status=Status.FROZEN)
    adopted = ((state is None and position.qty > 0)
               or str(((state.metadata if state else {}) or {}).get("ownership_source") or "")
               == "KR_INF_EXIT_ONLY_BROKER_ADOPTION")
    state=state or State(
        status=Status.ACTIVE if adopted else Status.READY,
        cycle_id=f"BROKER_ADOPTION-{trade_date.isoformat()}" if adopted else None,
        metadata={"ownership_source": "KR_INF_EXIT_ONLY_BROKER_ADOPTION",
                  "broker_qty": position.qty, "broker_average_price": position.average_price,
                  "broker_orderable_qty": position.orderable_qty} if adopted else {},
    )
    try: validate_invariants(state,position.qty)
    except ValueError as e: return Decision(Action.BLOCK,str(e),next_status=Status.FROZEN)
    if state.status == Status.FROZEN: return Decision(Action.BLOCK,"KR_INF_STATE_FROZEN",next_status=Status.FROZEN)
    if state.status == Status.COMPLETE and position.qty > 0: return Decision(Action.BLOCK,"KR_INF_STATE_POSITION_MISMATCH",next_status=Status.FROZEN)
    if state.status == Status.ACTIVE and (not state.cycle_id or position.qty == 0): return Decision(Action.BLOCK,"KR_INF_STATE_POSITION_MISMATCH",next_status=Status.FROZEN)
    if position.qty > 0 and position.average_price > 0:
        partial_exit_pending = bool((state.metadata or {}).get("partial_exit_pending"))
        pending_status = Status.ACTIVE if partial_exit_pending else Status.EXIT_PENDING
        if pending_sell:
            return Decision(Action.WAIT, "KR_INF_PROFIT_SELL_PENDING", next_status=pending_status)
        profit_pct = position.current_price / position.average_price - 1.0
        metadata = dict(state.metadata or {})
        stage = str(metadata.get("profit_stage") or "NONE").upper()
        pending_stage = str(metadata.get("pending_profit_stage") or "").upper()
        if pending_stage in {"TP1_SUBMITTED", "TP1_DEFENSE_SUBMITTED", "TP1_REBOUND_PENDING_SUBMITTED",
                             "TP1_REBOUND_CONFIRMED_SUBMITTED", "TP2_SUBMITTED",
                             "CAPITAL_RECOVERY_SUBMITTED"}:
            return Decision(Action.WAIT, "KR_INF_PROFIT_SELL_PENDING", next_status=pending_status)
        state_name = str(market_state or "").upper()
        if state_name not in KR_ADAPTIVE_TP_MAP:
            return Decision(Action.BLOCK, "KR_INF_UNMAPPED_REGIME")
        if stage in {"NONE", "TP1_PENDING"}:
            threshold, fraction, next_stage = adaptive_tp_stage(
                market_state=state_name,
                units_used=state.units_used,
                cycle_age_trading_days=state.cycle_age_trading_days,
            )
        elif stage in {"TP1", "TP1_FILLED"}:
            threshold = 0.10 if state_name in {"KR_RISK_ON", "KR_STRONG_RISK_ON"} else 0.07
            fraction, next_stage = 1.0, "TP2"
        else:
            threshold, fraction, next_stage = 999.0, 0.0, "NONE"
        if profit_pct >= threshold and next_stage != "NONE":
            if pending_sell:
                return Decision(Action.WAIT, "KR_INF_PROFIT_SELL_PENDING", next_status=pending_status)
            orderable = int(position.orderable_qty or 0)
            if orderable <= 0:
                return Decision(Action.WAIT, "KR_INF_PROFIT_NO_ORDERABLE_QTY")
            sell_qty = min(orderable, max(1, int(orderable * fraction)))
            if next_stage == "TP2" or fraction >= 0.99:
                sell_qty = orderable
            key = idempotency_key(state.cycle_id or "MISSING", trade_date, f"SELL_{next_stage}")
            sell_action = Action.SELL_PARTIAL if sell_qty < position.qty else Action.SELL_ALL
            assert (sell_action != Action.SELL_ALL or sell_qty == position.qty)
            assert (sell_action != Action.SELL_PARTIAL or position.qty - sell_qty > 0)
            return Decision(sell_action, f"TAKE_PROFIT_{next_stage}", sell_qty,
                            sell_qty * position.current_price, key, Status.EXIT_PENDING,
                            {"desired_profit_stage": next_stage, "profit_stage": next_stage,
                             "tp_threshold_fraction": threshold, "tp_sell_fraction": fraction,
                             "adaptive_mapping_source": "capital_recovery_override" if next_stage == "CAPITAL_RECOVERY" else "explicit",
                             "raw_market_state": market_state, "normalized_state": state_name,
                             "fallback_10pct_used": 0,
                             "tp1_sold_qty": sell_qty if next_stage != "TP2" else metadata.get("tp1_sold_qty", 0),
                             "remaining_qty": max(0, position.qty - sell_qty), "return_rate_at_decision": profit_pct,
                             **({"ownership_source": "KR_INF_EXIT_ONLY_BROKER_ADOPTION",
                                 "broker_qty": position.qty, "broker_average_price": position.average_price,
                                 "broker_orderable_qty": position.orderable_qty} if adopted else {})})
    if position.qty > 0 and position.current_price + 1e-9 >= position.average_price*(1+config.take_profit_pct):
        if pending_sell: return Decision(Action.WAIT,"PENDING_SELL",next_status=Status.EXIT_PENDING)
        key=idempotency_key(state.cycle_id or "MISSING",trade_date,"SELL_ALL")
        if key in existing_intent_keys: return Decision(Action.WAIT,"DUPLICATE_INTENT",next_status=Status.EXIT_PENDING)
        return Decision(Action.SELL_ALL,"TAKE_PROFIT",position.orderable_qty,position.orderable_qty*position.current_price,key,Status.EXIT_PENDING)
    if adopted:
        return Decision(Action.WAIT, "KR_INF_EXIT_ONLY_ADOPTION_NO_ENTRY")
    if state.status == Status.EXIT_PENDING or pending_sell: return Decision(Action.WAIT,"EXIT_PENDING",next_status=Status.EXIT_PENDING)
    if not allow_entry:return Decision(Action.WAIT,"KR_INF_ENTRY_DISABLED_BY_SESSION")
    infinite_overlay_bypass = config.symbol == "122630"
    regime_pause=buy_pause_reason(market_state,regime_data_quality)
    bypassable_states = {"KR_DEFENSE_CAUTION", "KR_DEFENSE_RISK_OFF", "KR_NORMAL", "KR_RISK_ON", "KR_STRONG_RISK_ON", "KR_SHOCK_REBOUND_CONFIRMED"}
    if regime_pause and (not infinite_overlay_bypass or market_state not in bypassable_states):return Decision(Action.WAIT,regime_pause)
    if state.status == Status.COMPLETE and state.last_exit_date == trade_date and not config.same_day_restart: return Decision(Action.BLOCK,"SAME_DAY_CYCLE_RESTART_BLOCK")
    if pending_buy or state.last_buy_date == trade_date: return Decision(Action.WAIT,"DAILY_BUY_LIMIT")
    new=position.qty==0
    if new and not infinite_overlay_bypass and not allows_new_cycle(market_state): return Decision(Action.WAIT,"NEW_CYCLE_REGIME_BLOCK")
    if state.units_used >= config.total_units: return Decision(Action.WAIT,"TOTAL_UNITS_EXHAUSTED")
    recovery=market_state=="KR_SHOCK_REBOUND_CONFIRMED" and (new or state.crash_seen)
    if recovery and state.recovery_probe_done: return Decision(Action.WAIT,"RECOVERY_PROBE_ALREADY_DONE")
    if market_state in {"KR_DEFENSE_CRASH","KR_SHOCK_REBOUND_PENDING"}: return Decision(Action.WAIT,"BUY_PAUSED_BY_REGIME")
    if new and infinite_overlay_bypass and (minutes_since_open is not None or vwap is not None or recent_3m_return is not None or quote_stale):
        delay = float(os.getenv("KR_INF_FIRST_BUY_DELAY_MIN", "10"))
        if minutes_since_open is not None and minutes_since_open < delay:
            return Decision(Action.WAIT, "KR_INF_FIRST_BUY_OPENING_STABILIZATION_WAIT")
        if vwap is not None and intraday_return is not None and position.current_price < vwap and intraday_return <= -0.02:
            return Decision(Action.WAIT, "KR_INF_WAIT_BELOW_VWAP_FAST_DROP")
        if recent_3m_return is not None and recent_3m_return <= -0.01:
            return Decision(Action.WAIT, "KR_INF_WAIT_FAST_FALLING")
        if quote_stale or best_ask is None or best_ask <= 0:
            return Decision(Action.WAIT, "KR_INF_WAIT_QUOTE_NOT_STABLE")
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
