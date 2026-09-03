"""Runtime lifecycle helpers for the isolated TQQQ sleeve."""
from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone
from typing import Mapping, Any

from .models import InfiniteState, PositionSnapshot, Status

TERMINAL = frozenset({"FILLED", "CANCELLED", "REJECTED", "EXPIRED", "STALE_RECONCILED"})


def rollover_partial_fill(state: InfiniteState, *, order_status: str, filled_qty: int,
                          requested_qty: int, filled_notional: float,
                          position: PositionSnapshot, trading_date: date,
                          order_key: str, hard_cap: float = 10_000.0) -> InfiniteState:
    """Idempotently reset the buy round, never the macro position cycle."""
    if str(order_status).upper() not in TERMINAL or filled_qty < requested_qty or requested_qty <= 0:
        return state
    if position.qty <= 0 or position.average_price <= 0:
        return state
    if state.metadata.get("last_partial_rollover_order_key") == order_key:
        return state
    seed = max(0.0, position.qty * position.average_price)
    remaining = max(0.0, hard_cap - seed)
    buy_round = int(state.metadata.get("buy_round") or 0) + 1
    stage = str(state.metadata.get("profit_stage") or "TP1_FILLED").upper()
    if not stage.endswith("_FILLED"):
        stage = f"{stage.removesuffix('_SUBMITTED')}_FILLED"
    metadata = {**state.metadata, "profit_stage": stage, "pending_profit_stage": None,
                "partial_exit_pending": False, "buy_round": buy_round,
                "buy_round_id": f"{state.cycle_id}:R{buy_round}", "buy_round_units_used": 0,
                "buy_round_started_trade_date": trading_date.isoformat(),
                "buy_round_effective_unit_usd": remaining / 40 if remaining > 0 else 0.0,
                "rollover_seed_qty": position.qty, "rollover_seed_avg": position.average_price,
                "rollover_seed_capital_usd": seed, "remaining_deployable_capital_usd": remaining,
                "hard_cap_exceeded": seed > hard_cap + 1e-6,
                "hard_cap_exceeded_reason": (
                    "TQQQ_INF_BROKER_DEPLOYED_EXCEEDS_HARD_CAP"
                    if seed > hard_cap + 1e-6 else None
                ),
                "broker_deployed_notional_usd": seed,
                "ownership_source": "KIS_BALANCE_AUTHORITATIVE",
                "last_partial_rollover_order_key": order_key,
                "last_partial_rollover_filled_qty": filled_qty,
                "last_partial_rollover_notional": filled_notional}
    return replace(state, status=Status.ACTIVE,
                   core_filled_notional=min(seed, hard_cap * .75),
                   reserve_filled_notional=max(0.0, seed - hard_cap * .75), metadata=metadata)


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
