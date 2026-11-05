"""US_STANDARD v3 swing exit *pure policy* (no broker/order/DB I/O).

Not a standalone live-order engine. The authoritative router must attach a
signed v3 BUY-time contract, durable stage state, min-hold, broker reservation
and Stage4 proof before acting on a decision. UNKNOWN inputs never become
synthetic HEALTHY or zero ATR.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timezone
from typing import Any


V3_POLICY_NAME = "US_SWING_TP_GB_ATR_RUNNER_V3"
V3_PARTIAL = "PROFIT_DEFENSE_PARTIAL"
V3_FINAL = "RUNNER_FINAL"
_TERMINAL_OR_ACTIVE = frozenset({
    "DONE", "ACK", "ACKED", "PARTIAL_FILL", "PARTIALLY_FILLED",
    "CLAIMED", "SUBMIT_STARTED", "HTTP_STARTED", "BROKER_UNKNOWN",
    "BROKER_UNKNOWN_RECONCILE_REQUIRED", "RECONCILE_REQUIRED",
    "ACTIVE_ORDER", "BLOCKED", "ERROR", "PENDING", "SIGNALLED",
})


def _positive(value: Any) -> float | None:
    try:
        x = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return x if math.isfinite(x) and x > 0 else None


def chandelier_from_verified_completed_bars(
    bars: list[dict], *, decision_date: str, atr_period: int = 14,
    lookback: int = 22, multiplier: float = 3.0, ratchet: bool = True,
) -> dict:
    """Wilder ATR14 and two *individual completed-bar* Chandelier stops.

    Each bar needs a strictly past ET trade date, consistent adjustment basis,
    independently verified source and completed=True. No last live candle.
    """
    try:
        signal_date = date.fromisoformat(str(decision_date))
    except ValueError:
        return {"valid": False, "reason": "DECISION_DATE_UNVERIFIED"}
    if not isinstance(bars, list) or len(bars) < max(atr_period + 2, lookback + 1):
        return {"valid": False, "reason": "INSUFFICIENT_COMPLETED_DAILY_BARS"}
    known_sources = {"KIS_DAILY_VERIFIED", "INDEPENDENT_DAILY_VERIFIED"}
    prev = None
    seen_date = None
    basis = None
    highs, lows, closes, tr = [], [], [], []
    for row in bars:
        if not isinstance(row, dict) or row.get("complete") is not True:
            return {"valid": False, "reason": "INCOMPLETE_BAR"}
        if row.get("source") not in known_sources:
            return {"valid": False, "reason": "BAR_SOURCE_UNVERIFIED"}
        this_basis = row.get("adjustment_basis")
        if not this_basis or (basis is not None and basis != this_basis):
            return {"valid": False, "reason": "ADJUSTMENT_BASIS_UNVERIFIED"}
        basis = this_basis
        try:
            at = date.fromisoformat(str(row["trade_date"]))
        except (KeyError, ValueError, TypeError):
            return {"valid": False, "reason": "BAR_DATE_UNVERIFIED"}
        if at >= signal_date or (seen_date is not None and at <= seen_date):
            return {"valid": False, "reason": "FUTURE_OR_UNSORTED_BAR"}
        seen_date = at
        h, l, c = _positive(row.get("high")), _positive(row.get("low")), _positive(row.get("close"))
        if h is None or l is None or c is None or not l <= c <= h:
            return {"valid": False, "reason": "INVALID_OHLC"}
        highs.append(h)
        lows.append(l)
        closes.append(c)
        if prev is not None:
            tr.append(max(h - l, abs(h - prev), abs(l - prev)))
        prev = c
    if len(tr) < atr_period or len(highs) < lookback + 1:
        return {"valid": False, "reason": "INSUFFICIENT_ATR_WINDOW"}
    atr = sum(tr[:atr_period]) / atr_period
    atr_by_index = {atr_period: atr}  # TR idx i corresponds bar idx i+1
    for j in range(atr_period, len(tr)):
        atr = ((atr * (atr_period - 1)) + tr[j]) / atr_period
        atr_by_index[j + 1] = atr
    stops: dict[int, float] = {}
    last_stop = None
    for i in range(max(lookback - 1, atr_period), len(highs)):
        current_atr = atr_by_index.get(i)
        if current_atr is None or current_atr <= 0:
            continue
        highest = max(highs[i - lookback + 1:i + 1])
        raw_stop = highest - multiplier * current_atr
        stop = max(last_stop, raw_stop) if last_stop is not None and ratchet else raw_stop
        stops[i] = stop
        last_stop = stop
    n = len(highs)
    if n - 2 not in stops or n - 1 not in stops:
        return {"valid": False, "reason": "MISSING_TWO_COMPLETED_STOPS"}
    return {
        "valid": True, "source": bars[-1]["source"], "adjustment_basis": basis,
        "as_of_date": str(seen_date), "atr14": atr_by_index[n - 1],
        "highest_high_22": max(highs[-lookback:]),
        "stop": stops[n - 1], "previous_stop": stops[n - 2],
        "two_completed_below_stop": closes[-2] < stops[n - 2] and closes[-1] < stops[n - 1],
    }


def evaluate_v3_runner(
    *, entry_price: float, verified_high: float, current_price: float,
    holding_qty: int, orderable_qty: int, trend_state: str,
    trend_verified: bool, high_verified: bool,
    atr_evidence: dict | None, partial_stage_status: str,
    final_stage_status: str, policy: dict,
) -> dict:
    """Return HOLD, BLOCK or exactly ONE partial/final stage intent proposal."""
    def hold(reason: str, **detail) -> dict:
        return {"action": "HOLD", "reason": reason, **detail}
    if str(policy.get("name") or "") != V3_POLICY_NAME:
        return hold("V3_POLICY_UNVERIFIED")
    ep, high, px = _positive(entry_price), _positive(verified_high), _positive(current_price)
    if ep is None or high is None or px is None or not high_verified or high < ep:
        return hold("LIFECYCLE_PRICE_EVIDENCE_UNVERIFIED")
    try:
        held, available = int(holding_qty), int(orderable_qty)
    except (ValueError, TypeError):
        return hold("HOLDING_QTY_UNVERIFIED")
    if held <= 0 or available <= 0:
        return hold("NO_VERIFIED_ORDERABLE_QTY")
    peak_gain = (high - ep) / ep
    current_gain = (px - ep) / ep
    giveback = (peak_gain - current_gain) / peak_gain if peak_gain > 0 else 0.0
    trend = str(trend_state or "UNKNOWN").upper()
    verified_trend = bool(trend_verified) and trend in {"HEALTHY", "WARNING", "TRIM", "EXIT"}
    bars = dict(atr_evidence or {})
    atr = _positive(bars.get("atr14")) if bars.get("valid") is True else None
    atr_confirm = atr is not None and high - px >= float(policy["atr_confirm_mult"]) * atr
    final_active = str(final_stage_status or "UNKNOWN").upper()
    partial_active = str(partial_stage_status or "UNKNOWN").upper()
    evidence = {"peak_gain": peak_gain, "current_gain": current_gain, "giveback": giveback,
                "trend_state": trend, "atr14": atr,
                "chandelier_stop": bars.get("stop") if bars.get("valid") else None}
    if final_active != "NOT_TRIGGERED":
        return hold("FINAL_STAGE_ALREADY_ACTIVE_OR_UNVERIFIED", **evidence)
    is_final = (
        (verified_trend and trend == "EXIT", "R1_TREND_EXIT"),
        (peak_gain >= float(policy["peak_activation"])
         and giveback >= float(policy["final_trim_giveback"])
         and verified_trend and trend == "TRIM", "R2_GIVEBACK_TREND"),
        (bars.get("valid") is True and bars.get("two_completed_below_stop") is True
         and px < float(bars["stop"]), "R3_TWO_CLOSES_CHANDELIER"),
        (peak_gain >= float(policy["peak_activation"])
         and giveback >= float(policy["max_giveback"]), "R4_PROFIT_GIVEBACK_CAP"),
    )
    for triggered, reason in is_final:
        if triggered:
            return {"action": "SELL", "stage": V3_FINAL,
                    "exit_type": "swing_runner_final_v3",
                    "qty": min(held, available), "reason": reason, **evidence}
    if partial_active != "NOT_TRIGGERED":
        return hold("PARTIAL_STAGE_ALREADY_ACTIVE_OR_UNVERIFIED", **evidence)
    if held <= 1:
        return hold("ONE_SHARE_RUNNER", **evidence)
    gb = peak_gain >= float(policy["peak_activation"]) and giveback >= float(policy["partial_giveback"]) and (atr_confirm or (verified_trend and trend == "TRIM"))
    old_trailing = (peak_gain >= float(policy["trailing_activation"])
                    and px <= high * (1.0 - float(policy["trailing_high_drawdown"])))
    if gb or old_trailing:
        qty = min(held - 1, available, int(held * float(policy["partial_fraction"])))
        if qty >= 1:
            return {"action": "SELL", "stage": V3_PARTIAL,
                    "exit_type": "swing_giveback_partial_v3", "qty": qty,
                    "reason": "GB1_PROFIT_GIVEBACK" if gb else "PROFIT_TRAILING_SHARED_BUDGET",
                    **evidence}
    return hold("V3_RUNNER_HOLD", **evidence)


V3_RUNNER_POLICY = {
    "name": V3_POLICY_NAME,
    "peak_activation": 0.08,
    "partial_giveback": 0.20,
    "partial_fraction": 0.50,
    "atr_confirm_mult": 1.2,
    "trailing_activation": 0.03,
    "trailing_high_drawdown": 0.05,
    "final_trim_giveback": 0.40,
    "max_giveback": 0.60,
    "atr_period": 14,
    "chandelier_lookback": 22,
    "chandelier_atr_mult": 3.0,
    "chandelier_ratchet": True,
}
