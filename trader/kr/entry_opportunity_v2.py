"""KR practice PB1 US-aligned entry price opportunity, version 2.

Only native completed bars; no order or position policy mutation. The KR
independent eight-minute path remains available when opening proof is sparse.
The caller retains its existing quote, account, sizing and broker fences.
"""
from __future__ import annotations

from datetime import datetime, time
import math

from trader.kr_price_utils import normalize_kr_order_price

POLICY = "US_ALIGNED_V2"
MAX_DEVIATION = 0.01
MIN_DEPTH = 0.005
MIN_VOLUME_RATIO = 0.8
MIN_RR = 1.5
MAX_RISK = 0.025


def evaluate_reclaim(*, family, opening, post, now, order_price, independent,
                     vwap=None, vwap_verified=False, trigger_ok=False):
    """The final proposed LIMIT, not an assumed executable quote, is evaluated.

    The US local four-bar recovery and risk geometry apply to KR. In the
    independent path the prior five-bar high is a local target, never an
    opening high. Neither a missing opening nor a prior conflict is fabricated.
    """
    facts = {
        "version": "kr_opening_bar_opportunity_v2",
        "entry_policy": POLICY, "family": family,
        "opening_count": len(opening), "opening_range_complete": not independent,
        "reference_source": ("KIS_NATIVE_POST_OPENING_5_PLUS_3" if independent
                             else "KIS_NATIVE_OPENING_AND_RECENT_15"),
        "not_order_authorization": True,
        "max_order_price_deviation": MAX_DEVIATION,
        "min_rr": MIN_RR, "max_stop_distance": MAX_RISK,
    }

    def result(state, reason, **extra):
        return {**facts, "state": state, "reason": reason, **extra}

    if family not in {"PULLBACK", "MOMENTUM"}:
        return result("UNKNOWN", "UNSUPPORTED_RECLAIM_FAMILY")
    if not time(9, 30) <= now.time() <= time(15, 5):
        return result("WAIT", "OUTSIDE_ENTRY_WINDOW")
    if len(post) < (8 if independent else 4):
        return result("WAIT", "POST_OPENING_CONFIRMATION_PENDING")
    tail = post[-4:]
    stamps = [datetime.fromisoformat(b["timestamp"]) for b in tail]
    if independent:
        all_stamps = [datetime.fromisoformat(b["timestamp"]) for b in post[-8:]]
    else:
        all_stamps = stamps
    if any((b-a).total_seconds() != 60 for a,b in zip(all_stamps, all_stamps[1:])):
        return result("UNKNOWN", "POST_OPENING_NATIVE_WINDOW_GAP" if independent
                      else "RECENT_MINUTE_GAP")
    if not 0 <= (now-stamps[-1]).total_seconds() <= 180:
        return result("UNKNOWN", "COMPLETED_BAR_STALE")
    if not independent and not opening:
        return result("UNKNOWN", "OPENING_BARS_MISSING_OR_INCOMPLETE")
    high = max(b["high"] for b in (post[-8:-3] if independent else opening))
    trough = min(b["low"] for b in post[-(8 if independent else 15):])
    close = tail[-1]["close"]
    try:
        price = float(order_price)
    except (TypeError, ValueError, OverflowError):
        return result("UNKNOWN", "INVALID_ORDER_PRICE")
    if not math.isfinite(price) or price <= 0:
        return result("UNKNOWN", "INVALID_ORDER_PRICE")
    prior_vol = sum(b["volume"] for b in tail[:2])
    recent_vol = sum(b["volume"] for b in tail[2:])
    ratio = recent_vol / prior_vol if prior_vol > 0 else 0
    rising = tail[-3]["close"] < tail[-2]["close"] < tail[-1]["close"]
    higher_lows = tail[-3]["low"] <= tail[-2]["low"] <= tail[-1]["low"]
    depth = (high-trough)/high
    facts.update(
        reference_price=close, latest_bar_ts=tail[-1]["timestamp"],
        recent_closes=[b["close"] for b in tail],
        recent_lows=[b["low"] for b in tail],
        prior_2m_volume=prior_vol, recent_2m_volume=recent_vol,
        observed_volume_ratio=round(ratio,5), volume_reference="PREVIOUS_2_MINUTES",
        intraday_pullback_depth=depth, increasing_close=rising,
        higher_lows=higher_lows, opening_high=None if independent else high,
        intraday_reference_high=high, local_trough=trough,
        vwap=vwap if vwap_verified else None, vwap_proven=bool(vwap_verified and vwap),
    )
    if abs(price/close-1) > MAX_DEVIATION:
        return result("WAIT", "ORDER_PRICE_MOVED_FROM_CONFIRMED_BAR")
    # Reclaim does not stand in for a breakout (including momentum breakout).
    if family == "MOMENTUM" and trigger_ok and close >= high:
        return result("WAIT", "MOMENTUM_BREAKOUT_REQUIRES_OWNER_PATH")
    if not (depth >= MIN_DEPTH and rising and higher_lows
            and recent_vol > 0 and prior_vol > 0 and ratio >= MIN_VOLUME_RATIO):
        return result("WAIT", "PULLBACK_SUPPORT_REBOUND_NOT_CONFIRMED" if family=="PULLBACK"
                      else "NO_CONFIRMED_MOMENTUM_RECLAIM")
    stop = min(b["low"] for b in tail[-3:])
    risk = price-stop
    facts.update(structural_stop_reference=stop, structural_target_reference=high,
                 risk_reference_is_live_stop_order=False)
    if risk <= 0:
        return result("WAIT", "NO_VALID_STRUCTURAL_RISK")
    rr = (high-price)/risk
    risk_pct = risk/price
    facts.update(risk_reward_reference=rr, stop_distance_pct=risk_pct*100)
    if risk_pct > MAX_RISK:
        return result("WAIT", "STRUCTURAL_STOP_DISTANCE_TOO_FAR")
    if rr < MIN_RR:
        return result("WAIT", "PULLBACK_NO_ROOM_VS_STOP" if family=="PULLBACK"
                      else "INSUFFICIENT_ROOM_VS_STOP")
    rr_cap = (high+MIN_RR*stop)/(1+MIN_RR)
    risk_cap = stop/(1-MAX_RISK)
    raw_cap = min(rr_cap, risk_cap, close*(1+MAX_DEVIATION))
    cap, _ = normalize_kr_order_price(raw_cap, side="SELL")
    facts.update(rr_price_ceiling=rr_cap, max_risk_price_ceiling=risk_cap)
    if cap < price or not stop < cap < high:
        return result("WAIT", "ORDER_PRICE_ABOVE_CONFIRMED_CAP")
    return result("READY_SHADOW", "US_ALIGNED_RECLAIM_CONFIRMED",
                  max_entry_price=cap)
