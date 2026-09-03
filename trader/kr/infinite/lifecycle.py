"""Broker-evidence-driven KR Infinite partial-profit buy-round lifecycle."""
from __future__ import annotations

from dataclasses import replace
from datetime import date

from .models import BrokerOrderState, BrokerPosition, OrderIntent, State, Status

TERMINAL = frozenset({"FILLED", "CANCELLED", "REJECTED", "EXPIRED", "STALE_RECONCILED"})


def settle_partial_exit(state: State, intent: OrderIntent, order: BrokerOrderState,
                        position: BrokerPosition, trade_date: date, *,
                        allocated_capital_krw: float, total_units: int = 40) -> State:
    """Reset only the buy round after a fully filled partial-profit order."""
    if intent.side != "SELL_PARTIAL" or intent.cycle_id != state.cycle_id:
        return state
    if state.metadata.get("last_partial_rollover_intent_key") == intent.idempotency_key:
        return state
    status = str(order.status).upper()
    if status not in TERMINAL:
        return state
    pending = str(state.metadata.get("pending_profit_stage") or "").upper()
    if not (order.filled_qty >= intent.requested_qty > 0):
        metadata = {**state.metadata, "pending_profit_stage": None, "partial_exit_pending": False,
                    "terminal_order_status": status}
        if order.filled_qty > 0:
            stage = pending.removesuffix("_SUBMITTED") or "TP1"
            metadata.update(partial_profit_stage=f"{stage}_PARTIAL",
                            partial_profit_filled_qty=order.filled_qty,
                            tp1_target_qty_at_first_decision=int(state.metadata.get("tp1_target_qty_at_first_decision") or intent.requested_qty),
                            tp1_cumulative_filled_qty=int(state.metadata.get("tp1_cumulative_filled_qty") or 0) + order.filled_qty,
                            tp1_remaining_target_qty=max(0, intent.requested_qty - order.filled_qty))
        return replace(state, status=Status.ACTIVE, metadata=metadata)
    # Rollover requires a fresh authoritative positive residual balance.
    if position.qty <= 0 or position.average_price <= 0:
        return state
    seed = position.qty * position.average_price
    allocated = max(0.0, float(allocated_capital_krw))
    if seed > allocated:
        raise ValueError("KR_INF_ROLLOVER_CAPITAL_EXCEEDED")
    free = max(0.0, allocated - seed)
    buy_round = int(state.metadata.get("buy_round") or 0) + 1
    already_filled = str(state.metadata.get("profit_stage") or "").upper()
    filled_stage = (f"{pending.removesuffix('_SUBMITTED')}_FILLED" if pending
                    else already_filled if already_filled.endswith("_FILLED") else "TP1_FILLED")
    metadata = {**state.metadata, "profit_stage": filled_stage, "pending_profit_stage": None,
                "partial_exit_pending": False, "buy_round": buy_round,
                "buy_round_id": f"{state.cycle_id}:R{buy_round}", "buy_round_units_used": 0,
                "buy_round_started_trade_date": trade_date.isoformat(),
                "rollover_seed_qty": position.qty, "rollover_seed_avg": position.average_price,
                "rollover_seed_capital_krw": seed, "free_strategy_capital_krw": free,
                "last_partial_rollover_intent_key": intent.idempotency_key,
                "terminal_order_status": status}
    return replace(state, status=Status.ACTIVE, allocated_capital_krw=allocated,
                   unit_krw=free / total_units, core_filled_notional=min(seed, allocated),
                   reserve_filled_notional=max(0.0, seed - min(seed, allocated)),
                   units_used=0, core_units_used=0, reserve_units_used=0, metadata=metadata)
