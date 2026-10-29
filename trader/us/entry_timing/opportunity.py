"""US_STANDARD price-aware timing: verified 1-minute KIS bars, never tick pseudo-bars.

The entry candidate stays independent of the clock gate. Trial thresholds are
parameters, not assumed profitable values. A READY decision is never broker
permission unless the existing risk router also allows an order.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
BAR_SOURCE = "KIS_OVERSEAS_1M"
FAMILIES = {"ENTRY_PULLBACK", "ENTRY_MOMENTUM", "ENTRY_BREAKOUT"}
CONFIG_VERSION = "US_OPPORTUNITY_TRIAL_V1"


def finite(value: Any) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError, OverflowError):
        return None


def normalize_bars(rows: list[dict], *, trade_date: str, now_et: datetime) -> list[dict]:
    """Only complete native KIS 1m bars; never synthesize from quote snapshots.

    KIS overseas minute response: xymd/xhms, open/high/low/last,
    evol (minute volume), eamt (minute turnover). Missing volume is unknown.
    """
    now_et = now_et.astimezone(NY)
    cleaned: dict[str, dict] = {}
    for raw in rows or []:
        if not isinstance(raw, dict):
            continue
        xymd = str(raw.get("xymd") or "").replace("-", "").strip()
        hhmmss = str(raw.get("xhms") or "").replace(":", "").strip().zfill(6)
        if xymd != trade_date.replace("-", "") or len(hhmmss) != 6 or not hhmmss.isdigit():
            continue
        try:
            at = datetime.strptime(xymd + hhmmss, "%Y%m%d%H%M%S").replace(tzinfo=NY)
        except ValueError:
            continue
        # Explicitly require the minute to finish before using it.
        if at + timedelta(minutes=1) > now_et:
            continue
        if not (at.replace(hour=9, minute=30, second=0) <= at <
                at.replace(hour=16, minute=0, second=0)):
            continue
        o, h, l, c = (finite(raw.get(k)) for k in ("open", "high", "low", "last"))
        v = finite(raw.get("evol"))
        turnover = finite(raw.get("eamt"))
        if any(x is None for x in (o, h, l, c, v)):
            continue
        if l <= 0 or v < 0 or not (l <= min(o, c) <= max(o, c) <= h):
            continue
        cleaned[at.isoformat()] = {
            "time_et": at.isoformat(),
            "open": o, "high": h, "low": l, "close": c,
            "volume": v, "turnover": turnover if turnover is not None and turnover >= 0 else None,
        }
    return [cleaned[key] for key in sorted(cleaned)]


def opening_coverage(bars: list[dict], trade_date: str) -> dict:
    opening = [b for b in bars if
               b["time_et"][:10] == trade_date and
               "T09:30:00" <= b["time_et"][10:19] < "T10:00:00"]
    minutes = {b["time_et"][11:16] for b in opening}
    return {
        "observed_1m": len(minutes),
        "missing_1m": max(0, 30 - len(minutes)),
        "first_bar": opening[0]["time_et"] if opening else None,
        "last_bar": opening[-1]["time_et"] if opening else None,
        "opening_high": max((b["high"] for b in opening), default=None),
        "opening_low": min((b["low"] for b in opening), default=None),
    }


def _decision(state: str, reason: str, *, style: str, facts: dict,
              price: float | None = None, max_entry_price: float | None = None,
              stop_reference: float | None = None, target_reference: float | None = None,
              rr: float | None = None) -> dict:
    result = {
        "schema": CONFIG_VERSION, "strategy_owner": "US_STANDARD",
        "entry_family": style, "state": state, "reason": reason,
        "facts": facts, "decision_price": price, "max_entry_price": max_entry_price,
        "structural_stop_reference": stop_reference,
        "structural_target_reference": target_reference,
        "risk_reward_reference": rr,
        "risk_reference_is_live_stop_order": False,
        "broker_authorized": False,
    }
    encoded = json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False)
    result["evidence_sha256"] = hashlib.sha256(encoded.encode()).hexdigest()
    return result


def evaluate_price_opportunity(
    *, style: str, trade_date: str, now_et: datetime,
    verified_bars: list[dict], quote: dict,
    prep: dict | None = None, min_opening_bars: int = 30,
    min_rr: float = 1.5, max_stop_distance: float = 0.025,
    max_spread: float = 0.003, max_chase: float = 0.008,
) -> dict:
    """Evaluate whether the *current entry price* deserves a BUY candidate.

    This is a trial signal, NOT an alteration of a frozen position's exit
    policy, nor proof of future profits. Incomplete or stale evidence => WAIT.
    """
    style = str(style or "").upper()
    prep = prep or {}
    facts: dict = {"trade_date": trade_date, "bar_source": BAR_SOURCE,
                   "bar_count": len(verified_bars), "thresholds": {
                       "min_opening_bars": min_opening_bars, "min_rr": min_rr,
                       "max_stop_distance": max_stop_distance,
                       "max_spread": max_spread, "max_chase": max_chase,
                   }}
    if style not in FAMILIES:
        return _decision("NOT_APPLICABLE", "non_standard_or_vcp", style=style, facts=facts)
    if not (
        isinstance(min_opening_bars, int) and min_opening_bars == 30
        and all(math.isfinite(v) for v in (min_rr, max_stop_distance, max_spread, max_chase))
        and min_rr >= 1.0
        and 0 < max_stop_distance <= 0.10
        and 0 < max_spread <= 0.03
        and 0 < max_chase <= 0.05
    ):
        return _decision("WAIT", "trial_policy_parameters_invalid", style=style, facts=facts)
    if now_et.tzinfo is None:
        return _decision("WAIT", "timezone_missing", style=style, facts=facts)
    now_et = now_et.astimezone(NY)
    # The observation sidecar may have advanced while the Tick was working.
    # Ignore bars that did not exist at the Tick's own decision timestamp.
    _asof_bars = []
    for _bar in verified_bars:
        try:
            _bar_at = datetime.fromisoformat(str(_bar["time_et"])).astimezone(NY)
        except (KeyError, ValueError, TypeError):
            continue
        if _bar_at.date().isoformat() == trade_date and _bar_at + timedelta(minutes=1) <= now_et:
            _asof_bars.append(_bar)
    verified_bars = sorted(_asof_bars, key=lambda b: b["time_et"])
    facts["bar_count"] = len(verified_bars)
    if now_et.date().isoformat() != trade_date:
        return _decision("WAIT", "trade_date_mismatch", style=style, facts=facts)
    if (now_et.hour, now_et.minute) < (10, 0):
        return _decision("WATCH", "opening_window_collecting", style=style, facts=facts)

    coverage = opening_coverage(verified_bars, trade_date)
    facts["opening"] = coverage
    if coverage["observed_1m"] < min_opening_bars:
        return _decision("WAIT", "opening_minute_bars_insufficient", style=style, facts=facts)

    # Native KIS minute turnover/volume only. Sparse last-price samples,
    # missing turnover, or anomalous value units do not produce fake VWAP.
    _open_bars = [
        b for b in verified_bars
        if "T09:30:00" <= b["time_et"][10:19] < "T10:00:00"
    ]
    _volume = sum(b["volume"] for b in _open_bars)
    _have_amount = (len(_open_bars) >= min_opening_bars
                    and all(b.get("turnover") is not None for b in _open_bars))
    _vwap = (sum(b["turnover"] for b in _open_bars) / _volume
             if _have_amount and _volume > 0 else None)
    _mid = sum(b["close"] for b in _open_bars) / len(_open_bars) if _open_bars else None
    if _vwap is not None and _mid is not None and not (0.75 <= _vwap / _mid <= 1.25):
        _vwap = None
    facts["opening_vwap"] = round(_vwap, 6) if _vwap is not None else None
    facts["vwap_provenance"] = "KIS_NATIVE_MINUTE_AMOUNT_AND_VOLUME" if _vwap is not None else "UNKNOWN"

    fresh_price = finite(quote.get("last"))
    age = finite(quote.get("age_sec"))
    source = str(quote.get("source") or "").upper()
    if (fresh_price is None or fresh_price <= 0 or age is None
            or age < 0 or age > 15
            or source not in {"KIS_LIVE", "KIS_CACHE", "KIS_WEBSOCKET",
                              "KIS_WEBSOCKET_COLLECTOR"}):
        return _decision("WAIT", "fresh_broker_quote_missing", style=style, facts=facts)
    # A fresh last price alone does not establish valid intraday volume or VWAP.
    facts["quote_age_sec"] = age
    bid, ask = finite(quote.get("bid")), finite(quote.get("ask"))
    if bid is None or ask is None or bid <= 0 or ask < bid:
        return _decision("WAIT", "executable_bid_ask_missing", style=style, facts=facts,
                         price=fresh_price)
    spread = (ask - bid) / ((ask + bid) / 2)
    facts["spread_pct"] = round(spread * 100, 4)
    if spread > max_spread:
        return _decision("WAIT", "spread_exceeds_limit", style=style, facts=facts,
                         price=fresh_price)

    if not verified_bars:
        return _decision("WAIT", "recent_minute_bars_missing", style=style, facts=facts)
    tail = verified_bars[-4:]
    if len(tail) < 4:
        return _decision("WAIT", "recent_minute_bars_insufficient", style=style, facts=facts)
    parsed_times = [datetime.fromisoformat(b["time_et"]) for b in tail]
    if (now_et - parsed_times[-1]).total_seconds() > 180:
        return _decision("WAIT", "recent_minute_bars_stale", style=style, facts=facts)
    if any((b - a).total_seconds() != 60 for a, b in zip(parsed_times, parsed_times[1:])):
        return _decision("WAIT", "recent_minute_bars_not_contiguous", style=style, facts=facts)
    facts["last_completed_bar"] = tail[-1]["time_et"]
    facts["recent_closes"] = [b["close"] for b in tail]
    facts["recent_lows"] = [b["low"] for b in tail]
    recent_volume = sum(b["volume"] for b in tail[-2:])
    prior_volume = sum(b["volume"] for b in tail[:2])
    facts["recent_2m_volume"] = recent_volume
    facts["prior_2m_volume"] = prior_volume
    last_close = tail[-1]["close"]
    if abs(fresh_price / last_close - 1) > 0.01:
        return _decision("WAIT", "live_price_far_from_completed_bar", style=style,
                         facts=facts, price=fresh_price)

    rising_closes = tail[-1]["close"] > tail[-2]["close"] > tail[-3]["close"]
    higher_lows = tail[-1]["low"] >= tail[-2]["low"] >= tail[-3]["low"]
    volume_confirmed = recent_volume > 0 and prior_volume > 0 and recent_volume >= 0.8 * prior_volume
    facts.update(rising_closes=rising_closes, higher_lows=higher_lows,
                 volume_confirmed=volume_confirmed)
    opening_high = coverage["opening_high"]
    opening_low = coverage["opening_low"]
    # The high/low are from actual broker bars; NOT tick-sample extrema.
    local_trough = min(b["low"] for b in verified_bars[-15:])
    facts["local_trough"] = local_trough

    if style == "ENTRY_BREAKOUT":
        pivot = finite(prep.get("breakout_pivot_price") or prep.get("pivot_price"))
        if pivot is None or pivot <= 0:
            return _decision("WAIT", "prep_pivot_missing", style=style, facts=facts, price=fresh_price)
        confirmed = last_close >= pivot * 1.0005 and tail[-2]["close"] >= pivot
        if not confirmed or not volume_confirmed:
            return _decision("WAIT", "breakout_not_confirmed", style=style, facts=facts, price=fresh_price)
        if ask > pivot * (1 + max_chase):
            return _decision("WAIT", "breakout_chase_too_large", style=style, facts=facts, price=fresh_price)
        target = finite(prep.get("timing_target_reference"))
    else:
        # Momentum may use either the pullback-reclaim or separately validated
        # pivot path. No daily PB1 signal is silently converted into day-trade.
        recovered = rising_closes and higher_lows and volume_confirmed
        pullback_depth = (opening_high - local_trough) / opening_high if opening_high else 0
        facts["intraday_pullback_depth"] = round(pullback_depth, 5)
        if not recovered or pullback_depth < 0.005:
            # Distinguish a watched setup from an actual completed-bar
            # confirmation; a falling knife never becomes READY.
            facts["phase"] = "PULLBACK" if pullback_depth >= 0.005 else "WATCH"
            return _decision("WAIT", "reversal_not_confirmed", style=style, facts=facts, price=fresh_price)
        if style == "ENTRY_MOMENTUM":
            facts["momentum_subtype"] = "PULLBACK_RECLAIM"
        facts["phase"] = "CONFIRM"
        target = opening_high

    stop = min(b["low"] for b in tail[-3:])
    entry = ask
    if entry <= stop:
        return _decision("WAIT", "stop_geometry_invalid", style=style, facts=facts, price=fresh_price)
    risk = (entry - stop) / entry
    reward = (target - entry) / entry if target is not None else None
    rr = reward / risk if reward is not None and risk > 0 else None
    facts.update(stop_distance_pct=round(risk * 100, 4),
                 reward_distance_pct=round(reward * 100, 4) if reward is not None else None)
    if risk > max_stop_distance:
        return _decision("WAIT", "stop_reference_too_far", style=style, facts=facts, price=fresh_price,
                         stop_reference=stop, target_reference=target, rr=rr)
    if rr is None or rr < min_rr:
        return _decision("WAIT", "unfavorable_reference_reward_risk", style=style, facts=facts,
                         price=fresh_price, stop_reference=stop, target_reference=target, rr=rr)
    # Solve the price ceiling using the FINAL executable limit, not today's
    # ask: (target - limit)/(limit - stop) >= required reference R:R.
    # Also re-evaluate structural distance at the final order limit.
    rr_ceiling = (target + min_rr * stop) / (1.0 + min_rr)
    risk_ceiling = stop / (1.0 - max_stop_distance)
    ceiling = min(ask * (1 + 0.001), rr_ceiling, risk_ceiling)
    # Conservative tick-independent 4dp floor: rounding UP could violate R:R.
    import math as _math
    ceiling = _math.floor(ceiling * 10000.0) / 10000.0
    facts["rr_price_ceiling"] = round(rr_ceiling, 6)
    facts["max_risk_price_ceiling"] = round(risk_ceiling, 6)
    if ceiling < ask:
        return _decision("WAIT", "entry_ceiling_below_ask", style=style, facts=facts,
                         price=fresh_price, stop_reference=stop, target_reference=target, rr=rr)
    facts["phase"] = "READY"
    return _decision("READY", "confirmed_rebound_or_breakout_with_price_room",
                     style=style, facts=facts, price=fresh_price,
                     max_entry_price=ceiling,
                     stop_reference=stop, target_reference=target, rr=rr)


def effective_mode(environ: dict | None = None) -> str:
    """Policy approval and practice guard before any BUY permission change."""
    import os
    env = environ if environ is not None else os.environ
    requested = str(env.get("US_ENTRY_OPPORTUNITY_MODE", "SHADOW")).upper()
    if requested == "OFF":
        return "OFF"
    if requested != "ENFORCE":
        return "SHADOW"
    if (str(env.get("KIS_ENV", "practice")).lower() != "practice"
            or str(env.get("US_ENTRY_OPPORTUNITY_APPROVED", "0")) != "1"):
        return "ENFORCE_NOT_APPROVED"
    return "ENFORCE"


def attach_opportunity_proof(intent: dict, decision: dict, *, mode: str,
                             decision_epoch: float) -> None:
    """Freeze the exact observed signal before order sizing/route."""
    if not isinstance(intent.get("meta"), dict):
        intent["meta"] = {}
    intent["meta"]["us_entry_opportunity_v1"] = {
        **decision, "mode": mode, "decision_epoch": float(decision_epoch),
        "max_age_sec": 120.0,
    }


def pre_submit_timing_check(intent: dict, *, now_epoch: float, fresh_ws_quote: dict | None,
                            max_quote_age: float = 5.0) -> tuple[bool, str]:
    """Pure BUY-only native HTTP-boundary check; no broker I/O here.

    Caller gets a fresh WS snapshot via bounded read-only collector IPC.
    The full entry/risk router remains authoritative for order permission.
    """
    if (str(intent.get("side") or "").upper() != "BUY"
            or str(intent.get("position_action") or "") != "NEW_POSITION_BUY"
            or str(intent.get("strategy_owner") or "US_STANDARD").upper() != "US_STANDARD"):
        return True, "NOT_APPLICABLE"
    meta = intent.get("meta") if isinstance(intent.get("meta"), dict) else {}
    proof = meta.get("us_entry_opportunity_v1")
    if not isinstance(proof, dict) or proof.get("mode") != "ENFORCE":
        return False, "TIMING_PROOF_MISSING"
    signed = {k: v for k, v in proof.items()
              if k not in {"mode", "decision_epoch", "max_age_sec", "evidence_sha256"}}
    try:
        digest = hashlib.sha256(
            json.dumps(signed, sort_keys=True, separators=(",", ":"),
                       allow_nan=False).encode()
        ).hexdigest()
    except (TypeError, ValueError):
        return False, "TIMING_PROOF_INVALID"
    if digest != proof.get("evidence_sha256") or proof.get("state") != "READY":
        return False, "TIMING_PROOF_INVALID"
    observed = finite(proof.get("decision_epoch"))
    if observed is None or not (0 <= now_epoch - observed <= 120):
        return False, "TIMING_SIGNAL_EXPIRED"
    cap = finite(proof.get("max_entry_price"))
    price = finite(intent.get("limit_price"))
    if cap is None or cap <= 0 or price is None or price <= 0 or price > cap + 1e-6:
        return False, "TIMING_PRICE_CAP_VIOLATED"
    if not isinstance(fresh_ws_quote, dict):
        return False, "TIMING_WS_QUOTE_MISSING"
    received = finite(fresh_ws_quote.get("received_at"))
    last = finite(fresh_ws_quote.get("last"))
    ask = finite(fresh_ws_quote.get("ask"))
    bid = finite(fresh_ws_quote.get("bid"))
    if (received is None or last is None or ask is None or bid is None or
            received > now_epoch + 1 or now_epoch - received > max_quote_age or
            last <= 0 or ask < bid or bid <= 0):
        return False, "TIMING_WS_QUOTE_STALE"
    if ask > cap + 1e-6 or (ask - bid) / ((ask + bid) / 2) > 0.003:
        return False, "TIMING_BROKER_PRICE_MOVED"
    decision_price = finite(proof.get("decision_price"))
    stop_ref = finite(proof.get("structural_stop_reference"))
    if (decision_price is None or decision_price <= 0
            or abs(last / decision_price - 1.0) > 0.004
            or (stop_ref is not None and last <= stop_ref)):
        # A new sharp downswing invalidates a previously valid rebound,
        # even when the ask still fits the old maximum entry price.
        return False, "TIMING_REBOUND_INVALIDATED"
    return True, "TIMING_FRESH_PRICE_CONFIRMED"
