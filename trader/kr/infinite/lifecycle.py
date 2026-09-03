"""Fill-evidence-driven partial-profit cycle rollover."""
from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import date

from .models import BrokerOrderState, BrokerPosition, OrderIntent, State, Status

TERMINAL = frozenset({"FILLED", "CANCELLED", "REJECTED", "EXPIRED", "STALE_RECONCILED"})


def settle_partial_exit(state: State, intent: OrderIntent, order: BrokerOrderState,
                        position: BrokerPosition, trade_date: date) -> tuple[State, State | None]:
    """Return ``(old, child)``; ACK/open evidence can never create a child."""
    if intent.side != "SELL_PARTIAL" or str(order.status).upper() not in TERMINAL:
        return state, None
    if int(order.filled_qty or 0) <= 0:
        return replace(state, status=Status.ACTIVE,
                       metadata={**state.metadata, "pending_profit_stage": None,
                                 "partial_exit_pending": False,
                                 "terminal_order_status": order.status}), None
    if position.qty <= 0:
        return replace(state, status=Status.COMPLETE, cycle_complete_date=trade_date,
                       last_exit_date=trade_date,
                       metadata={**state.metadata, "pending_profit_stage": None,
                                 "completion_reason": "FULL_LIQUIDATION",
                                 "realized_sell_qty": order.filled_qty}), None
    child_id = f"{trade_date.isoformat()}-{uuid.uuid4().hex[:12]}"
    child = State(cycle_id=child_id, cycle_start_date=trade_date,
                  allocated_capital_krw=state.allocated_capital_krw,
                  unit_krw=max(0.0, state.allocated_capital_krw - position.qty * position.average_price) / 40,
                  status=Status.ACTIVE,
                  metadata={"parent_cycle_id": state.cycle_id, "rollover_seed_qty": position.qty,
                            "rollover_seed_avg": position.average_price,
                            "rollover_source": "PROFIT_PARTIAL_EXIT", "units_total": 40,
                            "deployed_seed_capital": position.qty * position.average_price,
                            "rollover_created_trade_date": trade_date.isoformat()})
    old = replace(state, status=Status.COMPLETE, cycle_complete_date=trade_date,
                  metadata={**state.metadata, "pending_profit_stage": None, "partial_exit_pending": False,
                            "completion_reason": "PROFIT_PARTIAL_ROLLOVER",
                            "rollover_child_cycle_id": child_id, "realized_sell_qty": order.filled_qty,
                            "realized_sell_notional": order.filled_notional_krw,
                            "terminal_order_status": order.status})
    return old, child
