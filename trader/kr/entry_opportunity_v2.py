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
        "opening_count": len(opening),
        "opening_range_complete": not independent and len(opening) == 30,
        "opening_reference_available": not independent,
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
    # Compute the feasible structural interval even if the proposed ask
    # fails. Diagnostic only: WAIT still has no max_entry_price/authority.
    rr_cap = (high+MIN_RR*stop)/(1+MIN_RR)
    risk_cap = stop/(1-MAX_RISK)
    raw_cap = min(rr_cap, risk_cap, close*(1+MAX_DEVIATION))
    cap, _ = normalize_kr_order_price(raw_cap, side="SELL")
    facts.update(rr_price_ceiling=rr_cap, max_risk_price_ceiling=risk_cap,
                 diagnostic_entry_ceiling=cap if stop < cap < high else None)
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
    if cap < price or not stop < cap < high:
        return result("WAIT", "ORDER_PRICE_ABOVE_CONFIRMED_CAP")
    return result("READY_SHADOW", "US_ALIGNED_RECLAIM_CONFIRMED",
                  max_entry_price=cap)


def validate_live_quote(*, code, timing, quote, now, order_price, allow_passive=False):
    """Fresh authenticated quote guard for an already READY V2 LIMIT.

    A passive LIMIT is allowed only on the pre-existing no-ask fallback path.
    This is not an invented bid/ask, an upward repricing, or order authority.
    """
    def positive(value):
        try:
            value = float(value)
            return value if math.isfinite(value) and value > 0 else None
        except (TypeError, ValueError, OverflowError):
            return None

    def verdict(state, reason, **facts):
        return {"state": state, "reason": reason, **facts}

    if timing.get("state") != "READY_SHADOW" or timing.get("entry_policy") != POLICY:
        return verdict("UNKNOWN", "V2_TIMING_PROOF_UNAVAILABLE")
    if not isinstance(quote, dict) or now.tzinfo is None:
        return verdict("UNKNOWN", "V2_NATIVE_QUOTE_UNAVAILABLE")
    source = str(quote.get("source") or "").upper()
    identity = {
        "KIS_REST_INQUIRE_PRICE": "AUTHENTICATED_REST_REQUEST_BOUND",
        "KIS_WEBSOCKET": "KIS_WS_PARSED_SYMBOL",
    }.get(source)
    symbols = [str(quote.get(key) or "").removeprefix("A")
               for key in ("requested_symbol", "symbol") if quote.get(key)]
    expected = str(code).removeprefix("A").zfill(6)
    if (not identity or quote.get("source_identity") != identity
            or str(quote.get("market") or "").upper() != "KR"
            or not symbols or any(not s.isascii() or not s.isdigit()
                                  or s.zfill(6) != expected for s in symbols)):
        return verdict("UNKNOWN", "V2_NATIVE_QUOTE_IDENTITY_UNVERIFIED")
    receipt = positive(quote.get("received_at"))
    if receipt is None:
        return verdict("UNKNOWN", "V2_NATIVE_QUOTE_STALE")
    age = now.timestamp()-receipt
    quote_day = str(quote.get("stck_bsop_date") or "").replace("-", "")
    if not 0 <= age <= 15 or quote.get("stale") or (
            quote_day and quote_day != now.strftime("%Y%m%d")):
        return verdict("UNKNOWN", "V2_NATIVE_QUOTE_STALE", quote_age_sec=age)
    try:
        bar_age = (now-datetime.fromisoformat(timing["latest_bar_ts"])).total_seconds()
    except (ValueError, TypeError, KeyError):
        return verdict("UNKNOWN", "COMPLETED_BAR_STALE")
    if not 0 <= bar_age <= 180:
        return verdict("UNKNOWN", "COMPLETED_BAR_STALE")
    last = positive(quote.get("last") or quote.get("prpr") or quote.get("stck_prpr"))
    close = positive(timing.get("reference_price"))
    cap = positive(timing.get("max_entry_price"))
    price = positive(order_price)
    if None in (last, close, cap, price):
        return verdict("UNKNOWN", "V2_NATIVE_PRICE_UNVERIFIED")
    facts = dict(quote_age_sec=age, quote_source=source, quote_last=last,
                 latest_bar_age_sec=bar_age)
    if abs(last/close-1) > MAX_DEVIATION:
        return verdict("WAIT", "ORDER_PRICE_MOVED_FROM_CONFIRMED_BAR", **facts)
    if price > cap:
        return verdict("WAIT", "ORDER_PRICE_ABOVE_CONFIRMED_CAP", **facts)
    from trader.kr.executable_quote import valid_book_supplement
    if not valid_book_supplement(quote, expected, now=now.timestamp()):
        return verdict("UNKNOWN", "V2_EXECUTABLE_BID_ASK_UNVERIFIED", **facts)
    ask, bid = positive(quote.get("ask")), positive(quote.get("bid"))
    facts.update(quote_ask=ask, quote_bid=bid)
    if ask is not None and ask > price:
        return verdict("WAIT", "V2_ASK_ABOVE_PLANNED_LIMIT", **facts)
    if ask is None or bid is None:
        if not allow_passive or ask is not None:
            return verdict("UNKNOWN", "V2_EXECUTABLE_BID_ASK_UNVERIFIED", **facts)
        return verdict("READY", "V2_AUTHENTICATED_PASSIVE_LIMIT", **facts)
    if ask < bid:
        return verdict("UNKNOWN", "V2_EXECUTABLE_BID_ASK_UNVERIFIED", **facts)
    spread = (ask-bid)/((ask+bid)/2)
    facts["spread_pct"] = spread*100
    if spread > 0.003:
        return verdict("WAIT", "V2_SPREAD_EXCEEDS_LIMIT", **facts)
    return verdict("READY", "V2_EXECUTABLE_LIMIT_CONFIRMED", **facts)
