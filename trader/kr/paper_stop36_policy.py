"""KR PB1 practice-only -3% soft / -6% hard SWING exit policy.

This module only calculates decisions. The existing PB1 SELL order/claim,
KIS balance, account/epoch and reconciliation gates remain the sole authority
for broker submissions. No real-account or KR_INFINITE route is authorized.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import json
import math
from typing import Any, Mapping

POLICY_VERSION = "KR_PAPER_PB1_SWING_STOP_3_6_V1"
OWNER = "pb1_pullback_close"
FAMILY = "SWING_STAGED_EXIT"
SOFT_PCT = 3.0
HARD_PCT = 6.0
SOFT_CONFIRM_TICKS = 2
SOFT_SELL_FRACTION = 0.50


@dataclass(frozen=True)
class PaperStop36Decision:
    eligible: bool
    reason: str
    hard_touched: bool = False
    soft_confirmed: bool = False
    soft_breach_count: int = 0
    sell_qty: int = 0
    sell_pct: float | None = None
    effective_hard_stop: float | None = None
    update_meta: dict[str, Any] = field(default_factory=dict)


def evaluate_kr_pb1_paper_stop36(
    position: Mapping[str, Any],
    *,
    mark: float,
    env: str,
    strategy_owner: str,
    tick_id: str,
) -> PaperStop36Decision:
    """Two distinct observed breached ticks => first 50% SELL; hard => full.

    Counter and soft-completed states are scoped to a position cycle. A soft
    completion marker is *only* written by the normal broker-confirmed
    reconciliation path; this evaluator never sets it before a fill.
    """
    if env != "practice" or strategy_owner != OWNER:
        return PaperStop36Decision(False, "OWNER_OR_ENV_EXCLUDED")
    if str(position.get("strategy") or OWNER) != OWNER:
        return PaperStop36Decision(False, "POSITION_OWNER_EXCLUDED")
    if str(position.get("exit_policy_family") or "").upper() != FAMILY:
        return PaperStop36Decision(False, "EXIT_FAMILY_EXCLUDED")
    code = str(position.get("code") or "").removeprefix("A").zfill(6)
    if code == "122630":
        return PaperStop36Decision(False, "KR_INFINITE_RESERVED")
    cycle = str(position.get("position_cycle_id") or "").strip()
    epoch = str(position.get("portfolio_epoch_id") or "").strip()
    tick_id = str(tick_id or "").strip()
    if not cycle or not epoch or not tick_id:
        return PaperStop36Decision(False, "DURABLE_CYCLE_EPOCH_TICK_REQUIRED")
    plan = position.get("entry_exit_plan_json") or position.get("entry_exit_plan") or {}
    if isinstance(plan, str):
        try:
            plan = json.loads(plan)
        except (TypeError, ValueError):
            plan = {}
    if not isinstance(plan, dict) or str(plan.get("exit_policy_family") or "").upper() != FAMILY:
        return PaperStop36Decision(False, "FROZEN_SWING_PLAN_REQUIRED")
    meta = position.get("position_meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except (TypeError, ValueError):
            meta = {}
    if not isinstance(meta, dict):
        return PaperStop36Decision(False, "INVALID_POSITION_META")
    try:
        entry = float(position.get("avg_buy_price") or position.get("entry_price") or 0)
        mark_px = float(mark)
        raw_stop = float(
            meta.get("initial_stop_price") or position.get("stop_price_at_entry")
            or position.get("initial_stop") or 0
        )
        sellable = int(position.get("orderable_qty") or position.get("qty") or 0)
    except (TypeError, ValueError, OverflowError):
        return PaperStop36Decision(False, "INVALID_PRICE_OR_QUANTITY")
    if (
        not all(math.isfinite(v) for v in (entry, mark_px, raw_stop))
        or entry <= 0 or mark_px <= 0 or sellable <= 0
        or raw_stop <= 0 or raw_stop >= entry
    ):
        return PaperStop36Decision(False, "UNPROVEN_FROZEN_STOP_OR_PRICE")
    hard_stop = max(raw_stop, entry * (1.0 - HARD_PCT / 100.0))
    hard_touch = mark_px <= hard_stop
    soft_touch = mark_px <= entry * (1.0 - SOFT_PCT / 100.0)
    same_cycle = str(meta.get("kr_paper_stop36_cycle") or "") == cycle
    old_count = 0
    if same_cycle:
        try:
            old_count = max(0, min(SOFT_CONFIRM_TICKS, int(meta.get("kr_paper_stop36_breach_ticks") or 0)))
        except (TypeError, ValueError):
            old_count = 0
    last_tick = str(meta.get("kr_paper_stop36_last_tick") or "") if same_cycle else ""
    count = (
        (old_count if last_tick == tick_id else min(SOFT_CONFIRM_TICKS, old_count + 1))
        if soft_touch else 0
    )
    updates = {
        "kr_paper_stop36_policy_version": POLICY_VERSION,
        "kr_paper_stop36_cycle": cycle,
        "kr_paper_stop36_last_tick": tick_id,
        "kr_paper_stop36_breach_ticks": count,
    }
    if hard_touch:
        return PaperStop36Decision(
            True, "PAPER_HARD_STOP_6PCT_OR_TIGHTER_FROZEN",
            hard_touched=True, soft_breach_count=count,
            sell_qty=sellable, effective_hard_stop=hard_stop,
            update_meta=updates,
        )
    done = (
        same_cycle
        and bool(meta.get("kr_paper_stop36_soft_done"))
        and str(meta.get("kr_paper_stop36_soft_done_cycle") or "") == cycle
    )
    if soft_touch and not done and count >= SOFT_CONFIRM_TICKS:
        qty = min(sellable, max(1, int(sellable * SOFT_SELL_FRACTION)))
        return PaperStop36Decision(
            True, "PAPER_SOFT_STOP_3PCT_CONFIRMED",
            soft_confirmed=True, soft_breach_count=count,
            sell_qty=qty, sell_pct=SOFT_SELL_FRACTION,
            effective_hard_stop=hard_stop, update_meta=updates,
        )
    return PaperStop36Decision(
        True, ("PAPER_SOFT_ALREADY_FILLED" if done else
               "PAPER_SOFT_WAIT_SECOND_TICK" if soft_touch else "PAPER_STOP_NO_TRIGGER"),
        soft_breach_count=count, effective_hard_stop=hard_stop,
        update_meta=updates,
    )
