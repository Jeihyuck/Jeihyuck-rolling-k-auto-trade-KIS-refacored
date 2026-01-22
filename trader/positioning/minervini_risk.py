from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass
class ExitOrder:
    action: str
    qty: int
    reason: str


def calc_initial_stop(
    pivot: float,
    tight_low: float | None,
    atr: float | None,
    mode: str,
    *,
    entry: float | None = None,
    atr_mult: float = 2.5,
) -> float:
    if entry is None and pivot is not None and np.isfinite(pivot):
        entry = float(pivot)
    entry = float(entry or 0.0)
    stop_candidates = []
    if mode.upper() == "TIGHTLOW" and tight_low is not None and np.isfinite(tight_low):
        stop_candidates.append(float(tight_low) * 0.998)
    if atr is not None and np.isfinite(atr) and entry > 0:
        stop_candidates.append(entry - float(atr) * float(atr_mult))
    if entry > 0:
        stop_candidates.append(entry * 0.925)
    if not stop_candidates:
        return entry * 0.925
    return min(stop_candidates)


def calc_position_size(equity: float, risk_pct: float, entry: float, stop: float, risk_mult: float) -> int:
    per_share_risk = entry - stop
    if per_share_risk <= 0:
        return 0
    max_risk = equity * (risk_pct / 100.0) * risk_mult
    return int(max_risk // per_share_risk) if max_risk > 0 else 0


def update_exits(
    state: dict,
    *,
    last_price: float,
    ma20: float | None,
    atr: float | None,
    take_profit_r1: float,
    take_profit_r2: float,
    tp1_pct: float,
    tp2_pct: float,
    trail_mode: str,
    trail_step_after_r: float,
    failed_breakout_days: int,
    failed_breakout: bool,
) -> list[ExitOrder]:
    orders: list[ExitOrder] = []
    entry_price = float(state.get("entry_price") or 0.0)
    stop_price = float(state.get("stop_price") or 0.0)
    r_value = float(state.get("r_value") or (entry_price - stop_price))
    if entry_price <= 0 or r_value <= 0:
        return orders
    tp1_done = bool(state.get("tp1_done"))
    tp2_done = bool(state.get("tp2_done"))
    qty = int(state.get("qty") or 0)

    if failed_breakout:
        orders.append(ExitOrder(action="SELL", qty=qty, reason="FAILED_BREAKOUT"))
        return orders

    if not tp1_done and last_price >= entry_price + take_profit_r1 * r_value:
        orders.append(ExitOrder(action="SELL", qty=max(1, int(qty * tp1_pct)), reason="TP1"))
    if not tp2_done and last_price >= entry_price + take_profit_r2 * r_value:
        orders.append(ExitOrder(action="SELL", qty=max(1, int(qty * tp2_pct)), reason="TP2"))

    if last_price <= stop_price:
        orders.append(ExitOrder(action="SELL", qty=qty, reason="STOP_HIT"))

    if trail_mode.upper() == "MA20" and ma20 is not None and np.isfinite(ma20):
        trail_stop = float(ma20) * 0.997
        if trail_stop > stop_price and last_price >= entry_price + trail_step_after_r * r_value:
            state["stop_price"] = trail_stop
            state["last_trail_stop"] = trail_stop

    return orders
