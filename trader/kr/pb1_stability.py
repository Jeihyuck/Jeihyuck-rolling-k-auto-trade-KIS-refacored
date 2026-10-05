"""Cross-engine KR PB1 order stability contracts.

The helpers are deliberately independent of PB1's client-order-key.  They model
an economic order attempt and explicit same-day re-entry evidence.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import os
from typing import Any, Iterable

SEMANTIC_SELL_FAMILIES = (
    "SWING_STAGED_EXIT", "TRAIL_STOP_HIT", "TIME_STOP", "HARD_STOP",
    "DEFENSE_TRIM", "CLUSTER_TRIM", "PROFIT_CAPTURE", "FORCE_EOD",
)
_OPEN_SELL_ATTEMPT_STATUSES = frozenset({
    "SUBMITTED", "ACK", "ACKED", "ACCEPTED", "PARTIAL", "PARTIALLY_FILLED",
    "PARTIAL_FILLED", "UNRESOLVED_ACK", "RECONCILE_ERROR",
})
_SATISFIED_SELL_ATTEMPT_STATUSES = frozenset({"FILLED"})
_CANCELLED_SELL_STATUSES = frozenset({
    "CANCELED", "CANCELLED", "MANUAL_CANCELED", "MANUAL_CANCELLED",
    "CANCELLED_ZERO_FILL", "CANCELLED_PARTIAL_FILL", "EXPIRED",
})
_AUTHORITATIVE_CANCEL_STATUSES = frozenset({"CANCELLED_ZERO_FILL", "CANCELLED_PARTIAL_FILL"})
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


def is_retryable_semantic_sell_attempt(row: dict) -> bool:
    data = row if isinstance(row, dict) else {}
    status = str(data.get("status") or "").upper()
    response = data.get("response_json")
    if not isinstance(response, dict):
        response = {}
    if status in {"REJECTED", "REJECTED_EXPLICIT"}:
        return True
    if status in {"CREATED", "INTENT", "SKIP"}:
        return not any(
            data.get(field)
            for field in ("submitted_at", "acked_at", "kis_odno", "broker_order_id")
        )
    if status == "ERROR":
        rt_cd = str(response.get("rt_cd") or "").strip()
        if response.get("broker_submit") is False or (
            rt_cd and rt_cd not in {"0", "UNRESOLVED_ACK"}
        ):
            return True
        return not any(
            data.get(field)
            for field in ("submitted_at", "acked_at", "kis_odno", "broker_order_id")
        )
    broker_row = response.get("kis_row")
    if not isinstance(broker_row, dict):
        broker_row = {}
    cumulative_fills = []
    for payload in (response, broker_row):
        for field in ("cumulative_filled_qty", "tot_ccld_qty"):
            value = payload.get(field)
            if value is None or value == "":
                continue
            try:
                cumulative = float(value)
            except (TypeError, ValueError):
                continue
            if cumulative >= 0 and cumulative.is_integer():
                cumulative_fills.append(int(cumulative))
    request = data.get("request_json") or data.get("meta") or {}
    if not isinstance(request, dict):
        request = {}
    requested_qty = None
    for value in (
        data.get("requested_qty"),
        data.get("qty"),
        request.get("requested_qty"),
        request.get("submitted_qty"),
    ):
        if value is None or value == "":
            continue
        try:
            requested = float(value)
        except (TypeError, ValueError):
            continue
        if requested > 0 and requested.is_integer():
            requested_qty = int(requested)
            break
    if requested_qty is not None and any(fill >= requested_qty for fill in cumulative_fills):
        return False
    if status in _AUTHORITATIVE_CANCEL_STATUSES:
        return True
    if status not in _CANCELLED_SELL_STATUSES:
        return False
    return bool(cumulative_fills)


def same_day_semantic_sell_exists(*, rows: Iterable[dict], symbol: str, strategy_owner: str,
                                  reason_family: str, lifecycle_id: str,
                                  action_stage: str | None = None) -> bool:
    if os.getenv("KR_ALLOW_REPEAT_SEMANTIC_SELL", "0") == "1":
        return False
    wanted = (str(symbol).zfill(6), str(strategy_owner or "KR_STANDARD").upper(),
              normalize_sell_reason_family(reason_family), str(lifecycle_id or ""),
              str(action_stage or "").strip().upper())
    for row in rows:
        meta = row.get("request_json") or row.get("meta") or {}
        if not isinstance(meta, dict):
            meta = {}
        status = str(row.get("status") or "").upper()
        response = row.get("response_json")
        if not isinstance(response, dict):
            response = {}
        actual = (str(row.get("code") or row.get("symbol") or "").zfill(6),
                  str(row.get("strategy_owner") or meta.get("strategy_owner") or "KR_STANDARD").upper(),
                  normalize_sell_reason_family(row.get("reason_family") or meta.get("reason_family") or meta.get("reasons") or row.get("stage")),
                  str(row.get("position_lifecycle_id") or meta.get("position_lifecycle_id") or meta.get("lifecycle_id") or ""),
                  str(meta.get("exit_stage") or meta.get("semantic_action_stage")
                      or meta.get("profit_capture_stage") or row.get("stage") or "").strip().upper())
        if actual[:4] != wanted[:4] or (wanted[4] and actual[4] != wanted[4]):
            continue
        if is_retryable_semantic_sell_attempt(row):
            continue
        if status in _OPEN_SELL_ATTEMPT_STATUSES | _SATISFIED_SELL_ATTEMPT_STATUSES:
            return True
        if status == "ERROR":
            if any(row.get(field) for field in ("submitted_at", "acked_at", "kis_odno", "broker_order_id")):
                return True
            continue
        if status in _CANCELLED_SELL_STATUSES:
            return True
        if status == "CREATED" and any(
            row.get(field) for field in ("submitted_at", "acked_at", "kis_odno", "broker_order_id")
        ):
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


class NoSellableStickyFence:
    def __init__(self) -> None:
        self._blocked: dict[tuple[str, str, str], str] = {}

    def mark(self, *, trade_date: str, symbol: str, snapshot_version: str) -> None:
        self._blocked[(trade_date, str(symbol).zfill(6), "SELL")] = str(snapshot_version)

    def blocked(self, *, trade_date: str, symbol: str, snapshot_version: str,
                orderable_qty: int) -> bool:
        key = (trade_date, str(symbol).zfill(6), "SELL")
        old_version = self._blocked.get(key)
        if orderable_qty > 0 and old_version != str(snapshot_version):
            self._blocked.pop(key, None)
            return False
        return old_version == str(snapshot_version)


NO_SELLABLE_STICKY = NoSellableStickyFence()
