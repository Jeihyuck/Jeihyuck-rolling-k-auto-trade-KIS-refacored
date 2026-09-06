"""Cross-engine KR PB1 order stability contracts.

The helpers are deliberately independent of PB1's client-order-key.  They model
an economic order attempt and explicit same-day re-entry evidence.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from trader.kr.pb1.sell_fence import NO_SELLABLE_STICKY, NoSellableStickyFence

SEMANTIC_SELL_FAMILIES = (
    "SWING_STAGED_EXIT", "TRAIL_STOP_HIT", "TIME_STOP", "HARD_STOP",
    "DEFENSE_TRIM", "CLUSTER_TRIM", "PROFIT_CAPTURE", "FORCE_EOD",
)
ATTEMPT_STATUSES = frozenset({
    "CREATED", "SUBMITTED", "ACK", "ACKED", "ACCEPTED", "FILLED",
    "PARTIAL", "PARTIALLY_FILLED", "PARTIAL_FILLED", "CANCELED",
    "CANCELLED", "MANUAL_CANCELED", "MANUAL_CANCELLED", "REJECTED",
})
RECOVERY_REGIMES = frozenset({
    "KR_RISK_ON", "KR_STRONG_RISK_ON", "KR_SHOCK_REBOUND_CONFIRMED",
})


def normalize_sell_reason_family(reason: Any) -> str:
    value = str(reason or "").upper()
    aliases = {
        "EXIT_HARD_STOP": "HARD_STOP", "EXIT_CORE_HARD_STOP": "HARD_STOP",
        "TRAILING_STOP": "TRAIL_STOP_HIT", "EXIT_TRAILING_STOP": "TRAIL_STOP_HIT",
        "EXIT_TRAIL": "TRAIL_STOP_HIT", "EXIT_TIME": "TIME_STOP", "EXIT_TIME_STOP": "TIME_STOP",
        "DEFENSE_RISK_OFF_TRIM": "DEFENSE_TRIM", "CLUSTER_EXPOSURE_TRIM": "CLUSTER_TRIM",
        "KR_PROFIT_CAPTURE": "PROFIT_CAPTURE", "PROFIT_PROTECT_8PCT": "PROFIT_CAPTURE",
        "ABS_TP1_10PCT": "PROFIT_CAPTURE", "SWING_PROFIT_PROTECT_GIVEBACK": "PROFIT_CAPTURE",
        "MOMENTUM_PROFIT_PROTECT_GIVEBACK": "PROFIT_CAPTURE", "FORCE_CLOSE": "FORCE_EOD",
    }
    if value in aliases:
        return aliases[value]
    return next((family for family in SEMANTIC_SELL_FAMILIES if family in value), value)


def same_day_semantic_sell_exists(*, rows: Iterable[dict], symbol: str, strategy_owner: str,
                                  reason_family: str, lifecycle_id: str) -> bool:
    if os.getenv("KR_ALLOW_REPEAT_SEMANTIC_SELL", "0") == "1":
        return False
    wanted = (str(symbol).zfill(6), str(strategy_owner or "KR_STANDARD").upper(),
              normalize_sell_reason_family(reason_family), str(lifecycle_id or ""))
    for row in rows:
        meta = row.get("request_json") or row.get("meta") or {}
        if not isinstance(meta, dict):
            meta = {}
        status = str(row.get("status") or "").upper()
        actual = (str(row.get("code") or row.get("symbol") or "").zfill(6),
                  str(row.get("strategy_owner") or meta.get("strategy_owner") or "KR_STANDARD").upper(),
                  normalize_sell_reason_family(row.get("reason_family") or row.get("stage") or meta.get("reason_family") or meta.get("reasons")),
                  str(row.get("position_lifecycle_id") or meta.get("position_lifecycle_id") or meta.get("lifecycle_id") or ""))
        if status in ATTEMPT_STATUSES and actual == wanted:
            return True
    return False


@dataclass(frozen=True)
class ReentryDecision:
    allowed: bool
    reason: str


def evaluate_same_day_reentry(*, sell_exists: bool, sell_confirmed: bool, pending_sell: bool,
                              no_sellable_sticky: bool, prior_reason_family: str,
                              cooldown_elapsed_min: float, market_state: str,
                              entry_score_strong: bool, fresh_entry_signal: bool,
                              reentry_count: int) -> ReentryDecision:
    if not sell_exists:
        return ReentryDecision(True, "NO_SAME_DAY_SELL")
    if not sell_confirmed or pending_sell or no_sellable_sticky:
        return ReentryDecision(False, "BUYABLE_TODAY_SELL_REBUY_BLOCKED")
    if normalize_sell_reason_family(prior_reason_family) == "HARD_STOP":
        return ReentryDecision(False, "BUYABLE_TODAY_SELL_REBUY_BLOCKED")
    if os.getenv("KR_ALLOW_SAME_DAY_REBUY_AFTER_SELL", "0") != "1":
        return ReentryDecision(False, "BUYABLE_TODAY_SELL_REBUY_BLOCKED")
    required_cooldown = max(0, int(os.getenv("KR_SAME_DAY_REBUY_COOLDOWN_MIN", "60")))
    max_count = max(0, int(os.getenv("KR_SAME_DAY_REBUY_MAX_PER_SYMBOL", "1")))
    recovered = market_state in RECOVERY_REGIMES or (market_state == "KR_NORMAL" and entry_score_strong)
    if cooldown_elapsed_min < required_cooldown or not recovered or not fresh_entry_signal or reentry_count >= max_count:
        return ReentryDecision(False, "BUYABLE_TODAY_SELL_REBUY_BLOCKED")
    return ReentryDecision(True, "BUYABLE_TODAY_REBUY_ALLOWED_BY_RECOVERY")
