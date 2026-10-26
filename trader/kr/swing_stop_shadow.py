"""KR PB1 swing -3%/-6% candidate: observation-only risk policy.

This module MUST NOT submit an order or rewrite frozen EntryExitPlan,
position stop, or risk_R. Live routing is deliberately not connected: the
soft-stop confirmation/sell fraction requires policy approval and full replay.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping
import math

PB1_OWNER = "pb1_pullback_close"
SWING_FAMILY = "SWING_STAGED_EXIT"


@dataclass(frozen=True)
class KrSwingStopShadow:
    eligible: bool
    reason: str
    code: str
    entry_price: float | None
    mark_price: float | None
    soft_threshold: float | None
    hard_threshold: float | None
    soft_touched: bool
    hard_touched: bool
    raw_stop_preserved: float | None
    order_authorized: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def evaluate_kr_pb1_swing_stop_shadow(
    position: Mapping[str, Any],
    mark: float | None,
    *,
    strategy_owner: str,
    soft_pct: float = 3.0,
    hard_pct: float = 6.0,
) -> KrSwingStopShadow:
    """Evaluate *touches*, not execution intents; protect owner and contracts.

    Hard threshold is a floor on allowed stop price: the original frozen
    tighter stop is never widened. The original risk_R and TP multiples are
    not recalculated. Mark is observed price only, not executable fill price.
    """
    code = str(position.get("code") or "").zfill(6)
    empty = dict(
        eligible=False, code=code, entry_price=None, mark_price=None,
        soft_threshold=None, hard_threshold=None, soft_touched=False,
        hard_touched=False, raw_stop_preserved=None, order_authorized=False,
    )
    if strategy_owner != PB1_OWNER:
        return KrSwingStopShadow(reason="STRATEGY_OWNER_EXCLUDED", **empty)
    if str(position.get("exit_policy_family") or "").upper() != SWING_FAMILY:
        return KrSwingStopShadow(reason="EXIT_POLICY_FAMILY_EXCLUDED", **empty)
    meta = position.get("position_meta") or {}
    if not isinstance(meta, Mapping):
        meta = {}
    try:
        entry = float(position.get("entry_price") or position.get("avg_buy_price") or 0)
        mark_value = float(mark) if mark is not None else 0.0
        frozen_stop = float(
            meta.get("initial_stop_price") or position.get("stop_price_at_entry")
            or position.get("initial_stop") or 0.0
        )
        soft = float(soft_pct)
        hard = float(hard_pct)
    except (TypeError, ValueError):
        return KrSwingStopShadow(reason="INVALID_NUMERIC_EVIDENCE", **empty)
    if (
        not all(math.isfinite(x) for x in (entry, mark_value, frozen_stop, soft, hard))
        or entry <= 0 or mark_value <= 0 or not (0 < soft < hard < 100)
        or frozen_stop >= entry
    ):
        return KrSwingStopShadow(reason="INVALID_STOP_OR_PRICE_CONTRACT", **empty)
    soft_threshold = entry * (1 - soft / 100)
    hard_threshold = max(frozen_stop, entry * (1 - hard / 100))
    return KrSwingStopShadow(
        eligible=True, reason="SHADOW_ONLY_NOT_AN_ORDER",
        code=code, entry_price=entry, mark_price=mark_value,
        soft_threshold=soft_threshold, hard_threshold=hard_threshold,
        soft_touched=mark_value <= soft_threshold,
        hard_touched=mark_value <= hard_threshold,
        raw_stop_preserved=(frozen_stop if frozen_stop > 0 else None),
        order_authorized=False,
    )
