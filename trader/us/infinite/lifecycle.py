"""TQQQ-specific partial-exit rollover and scoped open-order helpers."""
from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import date, datetime, timezone
from typing import Mapping, Any

from .models import InfiniteState, PositionSnapshot, Status

TERMINAL = frozenset({"FILLED", "CANCELLED", "REJECTED", "EXPIRED", "STALE_RECONCILED"})


def rollover_partial_fill(state: InfiniteState, *, order_status: str, filled_qty: int,
                          filled_notional: float, position: PositionSnapshot,
                          trading_date: date, hard_cap: float = 10_000.0) -> tuple[InfiniteState, InfiniteState | None]:
    if str(order_status).upper() not in TERMINAL:
        return state, None
    if filled_qty <= 0:
        return replace(state, status=Status.ACTIVE,
                       metadata={**state.metadata, "pending_profit_stage": None,
                                 "partial_exit_pending": False, "terminal_order_status": order_status}), None
    if position.qty <= 0:
        return replace(state, status=Status.COMPLETE, cycle_complete_date=trading_date,
                       last_exit_date=trading_date,
                       metadata={**state.metadata, "completion_reason": "FULL_LIQUIDATION",
                                 "pending_profit_stage": None}), None
    seed = min(hard_cap, max(0.0, position.qty * position.average_price))
    deployable = max(0.0, hard_cap - seed)
    child_id = str(uuid.uuid4())
    child = InfiniteState(cycle_id=child_id, cycle_start_date=trading_date,
                          core_filled_notional=min(seed, hard_cap * .75),
                          reserve_filled_notional=max(0.0, seed - hard_cap * .75), status=Status.ACTIVE,
                          metadata={"parent_cycle_id": state.cycle_id, "rollover_seed_qty": position.qty,
                                    "rollover_seed_avg": position.average_price,
                                    "rollover_source": "PROFIT_PARTIAL_EXIT", "units_used": 0,
                                    "total_units": 40, "remaining_deployable_capital_usd": deployable,
                                    "effective_unit_usd": deployable / 40})
    old = replace(state, status=Status.COMPLETE, cycle_complete_date=trading_date,
                  metadata={**state.metadata, "completion_reason": "PROFIT_PARTIAL_ROLLOVER",
                            "pending_profit_stage": None, "partial_exit_pending": False,
                            "rollover_child_cycle_id": child_id, "realized_sell_qty": filled_qty,
                            "realized_sell_notional": filled_notional,
                            "terminal_order_status": order_status})
    return old, child


def order_blocks(order: Mapping[str, Any], *, strategy_owner: str, symbol: str, side: str,
                 cycle_id: str | None = None) -> bool:
    """Known OPEN orders fence only an identical owner/symbol/side/cycle lane."""
    status = str(order.get("status") or "").upper()
    if status not in {"ACK", "OPEN", "PENDING", "PARTIALLY_FILLED"}:
        return False
    return (str(order.get("strategy_owner") or "").upper() == strategy_owner.upper()
            and str(order.get("symbol") or "").upper() == symbol.upper()
            and str(order.get("side") or "").upper() == side.upper()
            and (cycle_id is None or str(order.get("cycle_id") or "") == str(cycle_id)))


def buy_ttl_expired(order: Mapping[str, Any], *, ttl_seconds: int, now: datetime | None = None) -> bool:
    if str(order.get("side") or "").upper() != "BUY" or str(order.get("status") or "").upper() not in {"ACK", "OPEN", "PENDING", "PARTIALLY_FILLED"}:
        return False
    created = order.get("created_at")
    if isinstance(created, str):
        created = datetime.fromisoformat(created.replace("Z", "+00:00"))
    if not isinstance(created, datetime):
        return False
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return ((now or datetime.now(timezone.utc)) - created).total_seconds() >= max(1, ttl_seconds)
