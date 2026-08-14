"""Generic position-cycle reconciliation and exit-safety primitives.

These helpers deliberately have no symbol-specific behaviour.  A broker holding is
tradable only when its provenance belongs to the active epoch and open cycle.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Mapping, Any
from uuid import uuid4


STALE_META_KEYS = frozenset({
    "high_since_entry", "highest_return_pct", "holding_bars",
    "trading_days_held", "calendar_days_held", "last_trail_stop",
    "partial_exit_level", "tp1_done", "tp2_done",
    "giveback_protect_done", "pyramid_level", "last_add_price",
    "max_pnl_pct_since_entry", "trail_stop_price",
})


def new_cycle_state(*, epoch_id: str, price: float, origin: str = "SYSTEM", opened_at: datetime | None = None) -> dict[str, Any]:
    """Return clean state for a 0 -> positive transition; never copy old metadata."""
    opened_at = opened_at or datetime.now(timezone.utc)
    return {
        "position_cycle_id": str(uuid4()), "portfolio_epoch_id": epoch_id,
        "position_origin": origin, "opened_at": opened_at, "status": "OPEN",
        "entry_ts": opened_at.isoformat(), "entry_price": float(price),
        "max_price": float(price), "holding_bars": 0,
        "tp1_done": False, "tp2_done": False, "partial_exit_level": 0,
        "pyramid_level": 0, "last_trail_stop": None,
        "position_meta": {"holding_bars": 0, "trading_days_held": 0,
                          "calendar_days_held": 0, "tp1_done": False,
                          "tp2_done": False, "giveback_protect_done": False,
                          "holding_age_unknown": origin in {"IMPORTED", "RECOVERY"}},
    }


def weighted_buy_average(fills: Iterable[Mapping[str, Any]]) -> float | None:
    buys = [(float(f.get("price") or 0), int(f.get("qty") or 0)) for f in fills
            if str(f.get("side") or "").upper() == "BUY"]
    qty = sum(q for p, q in buys if p > 0 and q > 0)
    return sum(p * q for p, q in buys if p > 0 and q > 0) / qty if qty else None


@dataclass(frozen=True)
class StateValidation:
    ok: bool
    reason: str
    cycle_fill_avg: float | None = None
    difference_pct: float | None = None


def validate_active_cycle(position: Mapping[str, Any], *, epoch_id: str, kis_qty: int,
                          kis_avg: float, fills: Iterable[Mapping[str, Any]],
                          tolerance_pct: float = 2.0) -> StateValidation:
    """Fail closed when lifecycle or cost-basis provenance cannot be proven."""
    if (str(position.get("status")) != "OPEN"
            or str(position.get("portfolio_epoch_id") or "") != str(epoch_id)
            or not position.get("position_cycle_id")
            or int(kis_qty) <= 0):
        return StateValidation(False, "POSITION_STATE_MISMATCH")
    cycle_id = str(position["position_cycle_id"])
    cycle_fills = [f for f in fills if str(f.get("position_cycle_id") or "") == cycle_id
                   and str(f.get("portfolio_epoch_id") or "") == str(epoch_id)]
    fill_avg = weighted_buy_average(cycle_fills)
    if fill_avg is None:
        if str(position.get("position_origin")) in {"IMPORTED", "RECOVERY"}:
            return StateValidation(True, "IMPORTED_COST_BASIS", float(kis_avg), 0.0)
        return StateValidation(False, "POSITION_STATE_MISMATCH")
    # KIS average price represents the remaining holding.  After partial sells,
    # positions.total_cost / qty is therefore the primary comparable basis;
    # the all-BUY weighted average remains provenance/audit evidence only.
    cycle_qty = int(position.get("qty") or 0)
    remaining_basis = (
        float(position.get("total_cost") or 0) / cycle_qty
        if cycle_qty > 0 and float(position.get("total_cost") or 0) > 0 else fill_avg
    )
    diff = abs(float(kis_avg) - remaining_basis) / remaining_basis * 100 if remaining_basis > 0 else float("inf")
    if diff > float(tolerance_pct):
        return StateValidation(False, "POSITION_COST_BASIS_MISMATCH", fill_avg, diff)
    return StateValidation(True, "OK", fill_avg, diff)


def validate_long_stop(reference_entry: float, effective_stop: float) -> tuple[bool, str, float]:
    entry, stop = float(reference_entry or 0), float(effective_stop or 0)
    if entry <= 0 or stop <= 0 or stop > entry:
        return False, "INVALID_STOP", entry - stop
    risk = entry - stop
    if risk <= 0:
        return False, "INVALID_R", risk
    return True, "OK", risk
