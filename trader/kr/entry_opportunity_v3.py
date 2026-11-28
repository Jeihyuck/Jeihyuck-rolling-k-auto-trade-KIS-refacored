"""Candidate geometry for KR PB1 Adaptive Entry V3.

This module is deliberately NOT an order authority. It consumes only
caller-verified native completed bars, never replaces Final30/owner/risk,
and is consumed only through the practice PB1 enforcement adapter.
"""
from __future__ import annotations

from datetime import datetime, time
from math import isfinite

from trader.kr.entry_opportunity_v2 import (
    MIN_DEPTH, MIN_VOLUME_RATIO, MIN_RR, MAX_RISK, MAX_DEVIATION,
)
from trader.kr_price_utils import normalize_kr_order_price


def candidate_geometry(*, family, opening, post, now, independent=False,
                       original_setup_ok=False, owner_breakout=False):
    """Signal and price interval independently of a proposed order price.

    Inputs must already be KIS-native and scoped to the account/session.
    A local high must precede the pullback; never use a future or current
    rebound high to manufacture reward. All consumed minutes are contiguous.
    """
    def result(state, reason, **facts):
        return {"state": state, "reason": reason, "entry_policy": "ADAPTIVE_V3",
                "not_order_authorization": True, **facts}

    if family not in {"PULLBACK", "MOMENTUM"} or owner_breakout:
        return result("WAIT", "OWNER_PATH_REQUIRED")
    if not original_setup_ok:
        return result("WAIT", "ORIGINAL_SETUP_FAILED")
    if now.tzinfo is None or not time(9, 30) <= now.time() <= time(15, 5):
        return result("UNKNOWN", "INVALID_SESSION")
    window = post[-8:] if independent else post[-30:]
    if len(window) < (8 if independent else 4):
        return result("WAIT", "POST_OPENING_CONFIRMATION_PENDING")
    try:
        stamps = [datetime.fromisoformat(b["timestamp"]) for b in window]
        if any(t.tzinfo is None or t.date() != now.date() for t in stamps):
            raise ValueError("session")
        if any((b-a).total_seconds() != 60 for a,b in zip(stamps, stamps[1:])):
            return result("UNKNOWN", "NATIVE_WINDOW_GAP")
        if not 0 <= (now-stamps[-1]).total_seconds() <= 180:
            return result("UNKNOWN", "COMPLETED_BAR_STALE")
        if any((now-t).total_seconds() < 60 or t.time() < time(9, 30)
               for t in stamps):
            return result("UNKNOWN", "INCOMPLETE_OR_OUT_OF_SESSION_BAR")
        if not independent:
            if not opening:
                return result("UNKNOWN", "OPENING_PROOF_UNAVAILABLE")
            opening_stamps = [datetime.fromisoformat(b["timestamp"]) for b in opening]
            if any(t.tzinfo is None or t.date() != now.date()
                   or not time(9, 0) <= t.time() < time(9, 30)
                   or (now-t).total_seconds() < 60 for t in opening_stamps):
                return result("UNKNOWN", "OPENING_PROOF_UNAVAILABLE")
        for b in window + ([] if independent else opening):
            hi, lo, close, vol = (float(b[k]) for k in ("high", "low", "close", "volume"))
            if not all(isfinite(x) for x in (hi, lo, close, vol)) or not 0 < lo <= close <= hi or vol < 0:
                raise ValueError("bar")
        tail = window[-4:]
        closes = [float(b["close"]) for b in tail[-3:]]
        lows = [float(b["low"]) for b in tail[-3:]]
        prior_vol = sum(float(b["volume"]) for b in tail[:2])
        recent_vol = sum(float(b["volume"]) for b in tail[2:])
        if prior_vol <= 0 or recent_vol <= 0:
            return result("WAIT", "NO_POSITIVE_VOLUME")
        ratio = recent_vol / prior_vol
        pattern = "STRICT_RISE" if closes[0] < closes[1] < closes[2] else (
            "RISE_AND_HOLD" if closes[0] < closes[1] == closes[2] else None)
        if not pattern or not lows[0] <= lows[1] <= lows[2] or ratio < MIN_VOLUME_RATIO:
            return result("WAIT", "REBOUND_NOT_CONFIRMED", pattern=pattern, volume_ratio=ratio)
        # Opening high remains the fallback. A later local peak is eligible
        # only when it is followed by a lower low before the final rebound.
        if independent:
            peak_index = max(range(len(window)-3), key=lambda i: float(window[i]["high"]))
            high = float(window[peak_index]["high"])
            trough = min(float(b["low"]) for b in window[peak_index+1:-2])
        else:
            high = max(float(b["high"]) for b in opening)
            trough = min(float(b["low"]) for b in window[-15:-2])
        source = "INDEPENDENT_WINDOW" if independent else "OPENING_HIGH"
        for i in range(len(window)-4, 0, -1):
            peak = float(window[i]["high"])
            if (peak >= float(window[i-1]["high"])
                    and peak > float(window[i+1]["high"])):
                later = min(float(b["low"]) for b in window[i+1:-2])
                if later < peak and (peak-later)/peak >= MIN_DEPTH:
                    # Do not replace a valid opening target just to enlarge RR.
                    if independent or peak > high:
                        high, trough, source = peak, later, "CONFIRMED_LOCAL_PEAK"
                        break
        depth = (high-trough)/high
        if depth < MIN_DEPTH:
            return result("WAIT", "NO_VALID_PULLBACK", depth=depth, target=high)
        stop = min(lows)
        close = closes[-1]
        raw = min((high+MIN_RR*stop)/(1+MIN_RR),
                  stop/(1-MAX_RISK), close*(1+MAX_DEVIATION))
        cap, _ = normalize_kr_order_price(raw, side="SELL")
        floor, tick = normalize_kr_order_price(max(stop, close*(1-MAX_DEVIATION)), side="BUY")
        if floor <= stop:
            floor, _ = normalize_kr_order_price(stop+tick, side="BUY")
        if not stop < floor <= cap < high:
            return result("WAIT", "NO_FEASIBLE_PRICE_INTERVAL", target=high, stop=stop)
        return result("SIGNAL", "CANDIDATE_PRICE_INTERVAL", pattern=pattern,
                      reference_source=source, target=high, stop=stop,
                      depth=depth, volume_ratio=ratio, price_floor=floor,
                      price_ceiling=cap, reference_close=close,
                      latest_bar_ts=stamps[-1].isoformat())
    except (KeyError, TypeError, ValueError, OverflowError, ZeroDivisionError):
        return result("UNKNOWN", "INVALID_NATIVE_BAR_INPUT")


def _finite_positive(value):
    try:
        number = float(value)
        return number if isfinite(number) and number > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def evaluate_entry(*, family, opening, post, now, order_price, independent,
                   payload, conflict_keys, setup_ok, trigger_ok):
    """Single V3 verdict: retain strict reclaim as an internal signal variant.

    The old public US_ALIGNED_V2 proof schema/reason strings stay unchanged
    for quote verification, telemetry and persisted order compatibility.
    Neither variant submits an order or bypasses a hard veto.
    """
    from trader.kr.entry_opportunity_v2 import evaluate_reclaim
    baseline = evaluate_reclaim(
        family=family, opening=opening, post=post, now=now,
        order_price=order_price, independent=independent,
        vwap=payload.get("vwap"),
        vwap_verified=payload.get("vwap_source") == "KIS_NATIVE_WEIGHTED_AVG",
        trigger_ok=trigger_ok,
    )
    baseline = {**baseline, "entry_engine_revision": "V3", "signal_variant": "STRICT_RECLAIM"}
    # Preserve strict-reclaim success and all hard vetoes within the one
    # V3 decision engine. The public V2 proof contract remains compatible.
    if baseline.get("state") != "WAIT" or baseline.get("reason") not in {
        "NO_CONFIRMED_MOMENTUM_RECLAIM", "PULLBACK_SUPPORT_REBOUND_NOT_CONFIRMED",
        "INSUFFICIENT_ROOM_VS_STOP", "PULLBACK_NO_ROOM_VS_STOP",
    }:
        return baseline
    if family == "MOMENTUM" and trigger_ok:
        return baseline
    # Use a contiguous, unquarantined suffix; never introduce a 30-bar
    # universal blocker or consume a conflicting historical local peak.
    suffix = []
    for bar in reversed(post[-(8 if independent else 30):]):
        stamp = datetime.fromisoformat(bar["timestamp"])
        if bar["timestamp"] in conflict_keys:
            break
        if suffix and (datetime.fromisoformat(suffix[-1]["timestamp"])-stamp).total_seconds() != 60:
            break
        suffix.append(bar)
    suffix.reverse()
    proposal = candidate_geometry(
        family=family, opening=opening, post=suffix, now=now,
        independent=independent, original_setup_ok=setup_ok,
    )
    if proposal.get("state") != "SIGNAL":
        return baseline
    price = _finite_positive(order_price)
    floor, cap = proposal["price_floor"], proposal["price_ceiling"]
    stop, high = proposal["stop"], proposal["target"]
    facts = {
        **baseline,
        "entry_policy": "US_ALIGNED_V2",
        "adaptive_revision": "V3",
        "signal_variant": proposal["pattern"],
        "reference_source": "KIS_NATIVE_ADAPTIVE_V3_" + proposal["reference_source"],
        "rebound_pattern": proposal["pattern"],
        "intraday_pullback_depth": proposal["depth"],
        "structural_stop_reference": stop,
        "structural_target_reference": high,
        "diagnostic_entry_ceiling": cap,
        "diagnostic_entry_floor": floor,
        "reference_price": proposal["reference_close"],
        "latest_bar_ts": proposal["latest_bar_ts"],
        "max_entry_price": None,
        "risk_reference_is_live_stop_order": False,
        "not_order_authorization": True,
    }
    if price is None or not floor <= price <= cap:
        return {**facts, "state": "WAIT", "reason": "ADAPTIVE_V3_PRICE_OUTSIDE_INTERVAL"}
    # The tick cap is a necessary, not sufficient, condition. Recompute
    # the risk geometry at the actual proposed price, never at a fake ask.
    from trader.kr.entry_opportunity_v2 import MIN_RR, MAX_RISK, MAX_DEVIATION
    risk = price-stop
    rr = (high-price)/risk if risk > 0 else 0
    if (risk <= 0 or rr < MIN_RR or risk/price > MAX_RISK
            or abs(price/proposal["reference_close"]-1) > MAX_DEVIATION):
        return {**facts, "state": "WAIT", "reason": "ADAPTIVE_V3_PRICE_OUTSIDE_INTERVAL"}
    return {**facts, "state": "READY_SHADOW",
            "reason": "ADAPTIVE_V3_" + proposal["pattern"] + "_CONFIRMED",
            "max_entry_price": cap, "risk_reward_reference": rr,
            "stop_distance_pct": risk/price*100}

