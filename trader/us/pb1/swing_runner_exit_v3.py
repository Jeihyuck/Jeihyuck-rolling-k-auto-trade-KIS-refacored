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
    # A pending/unknown partial has reserved inventory. A later Runner FINAL
    # must not be submitted over it without authoritative cancel/fill proof.
    # Existing hard/persistent soft stops are evaluated outside v3 first.
    if partial_active not in {"NOT_TRIGGERED", "DONE"}:
        return hold("PARTIAL_ORDER_UNSETTLED_FINAL_REQUIRES_RECONCILE", **evidence)
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


def evaluate_v3_from_position(*, position: dict, current_price: float,
                              now: datetime, contract: dict) -> dict:
    """Translate the running position snapshot to strict evidence and stage truth.

    No fallback to an unverified max_price, inferred old trade cycle, or DB
    failure. ATR is accepted only from an explicitly supplied independently
    verified *completed* daily feed, never unverified price_daily.
    """
    from zoneinfo import ZoneInfo
    from trader.us.execution.v3_stage_truth import read_v3_lifecycle_stage_states

    meta = position.get("meta") if isinstance(position.get("meta"), dict) else {}
    risk = position.get("risk_state") if isinstance(position.get("risk_state"), dict) else {}
    risk_state = risk.get("state") if isinstance(risk.get("state"), dict) else {}
    life = risk_state.get("lifecycle") if isinstance(risk_state.get("lifecycle"), dict) else {}
    lid = str(position.get("position_lifecycle_id") or meta.get("position_lifecycle_id") or "")
    cycle = str(position.get("position_cycle_id") or meta.get("position_cycle_id") or life.get("position_cycle_id") or "")
    portfolio = str(position.get("portfolio_epoch_id") or meta.get("portfolio_epoch_id") or life.get("portfolio_epoch_id") or "")
    entry = _positive(position.get("entry_price") or position.get("avg_price_usd") or position.get("avg_cost"))
    holding = int(position.get("qty") or position.get("holding_qty") or 0)
    orderable = int(position.get("orderable_qty") or position.get("sellable_qty") or holding)
    observed = life.get("high_watermark_at")
    high_proof = bool(
        lid and str(life.get("lifecycle_id") or "") == lid
        and life.get("high_watermark_source") == "us_position_risk_state"
        and observed
    )
    try:
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        at = datetime.fromisoformat(str(observed).replace("Z", "+00:00"))
        high_proof = high_proof and at.tzinfo is not None and at <= now
    except (TypeError, ValueError):
        high_proof = False
    trend = position.get("trend") if isinstance(position.get("trend"), dict) else {}
    decision_date = now.astimezone(ZoneInfo("America/New_York")).date().isoformat()
    trend_ok = (
        lid and str(trend.get("lifecycle_id") or "") == lid
        and trend.get("last_daily_update_trade_date") == decision_date
        and trend.get("trend_data_quality") == "ok"
        and trend.get("daily_history_quality") in {"OK", "DEGRADED_NO_MA200"}
    )

    daily = position.get("v3_completed_daily_bars")
    atr_evidence = None
    if isinstance(daily, list):
        atr_evidence = chandelier_from_verified_completed_bars(
            daily, decision_date=decision_date,
            atr_period=int(contract["management"]["runner_exit"]["atr_period"]),
            lookback=int(contract["management"]["runner_exit"]["chandelier_lookback"]),
            multiplier=float(contract["management"]["runner_exit"]["chandelier_atr_mult"]),
            ratchet=bool(contract["management"]["runner_exit"]["chandelier_ratchet"]),
        )
        if atr_evidence.get("valid"):
            try:
                from trader.us.market_calendar import previous_completed_us_session
                if str(atr_evidence["as_of_date"]) != previous_completed_us_session(decision_date).isoformat():
                    atr_evidence = {"valid": False, "reason": "STALE_COMPLETED_DAILY_BAR"}
            except Exception:
                atr_evidence = {"valid": False, "reason": "DAILY_CALENDAR_UNVERIFIED"}
    stages = read_v3_lifecycle_stage_states(
        lifecycle_id=lid, position_cycle_id=cycle,
        portfolio_epoch_id=portfolio,
        contract_sha=str(contract.get("sha256") or ""),
    )
    return evaluate_v3_runner(
        entry_price=entry or 0, verified_high=float(life.get("high_watermark") or 0),
        current_price=current_price, holding_qty=holding, orderable_qty=orderable,
        trend_state=trend.get("trend_state") or "UNKNOWN",
        trend_verified=bool(trend_ok), high_verified=high_proof,
        atr_evidence=atr_evidence,
        partial_stage_status=stages["PROFIT_DEFENSE_PARTIAL"],
        final_stage_status=stages["RUNNER_FINAL"],
        policy=dict(contract.get("management", {}).get("runner_exit") or {}),
    )
