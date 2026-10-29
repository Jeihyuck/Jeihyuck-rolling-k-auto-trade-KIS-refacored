"""KR PB1 entry-timing shadow: observe 09:00-09:30, NEVER approve or block orders.

This module deliberately does not connect to brokers, mutate any order, or infer
BUY readiness from the clock, prior-day setup, invalid VWAP, or unadjusted daily RVOL.
Evidence is scoped by trade date, account hash and symbol across scheduled ticks.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from datetime import datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from trader.runtime_paths import runtime_path

KST = ZoneInfo("Asia/Seoul")
PROOF_VERSION = "kr_entry_timing_shadow_v1"
_SUPPORTED = {"ENTRY_MOMENTUM", "ENTRY_MOMENTUM_CONTINUATION", "ENTRY_PULLBACK", "ENTRY_BREAKOUT"}


def _number(value: Any) -> float | None:
    try:
        value = float(value)
        return value if math.isfinite(value) and value > 0 else None
    except (ValueError, TypeError):
        return None


def _quote_exchange_ts(quote: dict[str, Any], trade_date: str) -> datetime | None:
    raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else {}
    clock = quote.get("stck_cntg_hour") or raw.get("stck_cntg_hour")
    quote_day = quote.get("stck_bsop_date") or raw.get("stck_bsop_date")
    if quote_day and str(quote_day).replace("-", "") != trade_date.replace("-", ""):
        return None
    if clock is None:
        return None
    clock = str(clock).strip().zfill(6)
    if len(clock) != 6 or not clock.isdigit():
        return None
    try:
        return datetime.strptime(trade_date + clock, "%Y-%m-%d%H%M%S").replace(tzinfo=KST)
    except ValueError:
        return None


def evaluate_kr_timing_shadow(
    *,
    style: str,
    samples: list[dict[str, Any]],
    observed_at: datetime,
    trigger_ok: bool,
    volume_proof: dict[str, Any] | None = None,
    setup_ok: bool = True,
) -> dict[str, Any]:
    """Explain a hypothetical timing decision; NEVER return an order authorization.

    No interpolation: sparse snapshots cannot certify completed one/five-minute
    candles, true session high/low, breakout volume, or a profitable entry.
    """
    observed_at = observed_at.astimezone(KST)
    base = {"version": PROOF_VERSION, "state": "WAIT", "reason": "",
            "opening_samples": 0, "opening_observed_high": None,
            "opening_observed_low": None, "not_order_authorization": True}
    if style not in _SUPPORTED:
        return {**base, "state": "NOT_APPLICABLE", "reason": "OTHER_OWNER_OR_STYLE"}
    if not setup_ok:
        return {**base, "reason": "SETUP_NOT_ELIGIBLE"}
    now_clock = observed_at.time()
    if now_clock < time(9, 30):
        return {**base, "reason": "OPENING_COLLECTION_WINDOW"}
    today = observed_at.date()
    filtered: list[tuple[datetime, float, dict[str, Any]]] = []
    for row in samples:
        try:
            ts = datetime.fromisoformat(str(row.get("exchange_ts") or ""))
        except ValueError:
            continue
        if ts.tzinfo is None:
            continue
        ts = ts.astimezone(KST)
        px = _number(row.get("price"))
        if ts.date() == today and time(9, 0) <= ts.time() <= now_clock and px is not None:
            filtered.append((ts, px, row))
    # Same exchange second is one observation, regardless of repeated scheduler ticks.
    dedup: dict[datetime, tuple[datetime, float, dict[str, Any]]] = {r[0]: r for r in filtered}
    ordered = sorted(dedup.values(), key=lambda r: r[0])
    opening = [r for r in ordered if time(9, 0) <= r[0].time() < time(9, 30)]
    base["opening_samples"] = len(opening)
    if opening:
        base["opening_observed_high"] = max(r[1] for r in opening)
        base["opening_observed_low"] = min(r[1] for r in opening)
    # Need evidence from both ends of the opening period, not merely N ticks.
    if (len(opening) < 6
            or opening[0][0].time() > time(9, 5)
            or opening[-1][0].time() < time(9, 25)):
        return {**base, "reason": "INSUFFICIENT_OPENING_COVERAGE"}
    if not ordered or (observed_at - ordered[-1][0]).total_seconds() > 120:
        return {**base, "state": "UNKNOWN", "reason": "QUOTE_STALE_OR_MISSING"}
    current = ordered[-1]
    if not bool(current[2].get("quote_fresh")):
        return {**base, "state": "UNKNOWN", "reason": "QUOTE_UNVERIFIED"}
    if current[0].time() < time(9, 30):
        return {**base, "reason": "AWAIT_POST_OPENING_QUOTE"}
    if len(ordered) < 3:
        return {**base, "reason": "NOT_ENOUGH_PRICE_OBSERVATIONS"}
    prices = [r[1] for r in ordered[-3:]]
    rising = prices[0] < prices[1] < prices[2]
    vwap = _number(current[2].get("vwap"))
    proof = volume_proof if isinstance(volume_proof, dict) else {}
    rvol = _number(proof.get("relative_volume_at_time")) if proof.get("confirmation_state") == "VERIFIED" else None
    detail = {**base, "last_price": current[1], "vwap": vwap, "rvol_at_time": rvol,
              "three_observations_rising": rising}

    if style in {"ENTRY_MOMENTUM", "ENTRY_MOMENTUM_CONTINUATION"}:
        if vwap is None or rvol is None:
            return {**detail, "reason": "VWAP_OR_TIME_ADJUSTED_RVOL_UNKNOWN"}
        if current[1] <= vwap:
            return {**detail, "reason": "NOT_ABOVE_VWAP"}
        if rvol < 1.2:
            return {**detail, "reason": "RVOL_BELOW_EXPLORATORY_THRESHOLD"}
        if (rising and trigger_ok and current[1] > base["opening_observed_high"]):
            return {**detail, "state": "READY_SHADOW", "reason": "MOMENTUM_OPENING_HIGH_BREAKOUT"}
        if rising and all(r[1] > vwap for r in ordered[-2:]):
            return {**detail, "state": "READY_SHADOW", "reason": "MOMENTUM_SUSTAINED_RECLAIM"}
        return {**detail, "reason": "NO_CONFIRMED_MOMENTUM_RESUMPTION"}
    if style == "ENTRY_BREAKOUT":
        if (trigger_ok and proof.get("confirmation_state") == "VERIFIED"
                and proof.get("breakout_volume_confirmed") is True
                and rising and current[1] > base["opening_observed_high"]):
            return {**detail, "state": "READY_SHADOW", "reason": "BREAKOUT_CONFIRMED"}
        return {**detail, "reason": "BREAKOUT_PRICE_AND_VOLUME_NOT_BOTH_CONFIRMED"}
    # Pullback needs a proven support level and completed intraday candles,
    # neither is available from sparse candidate-tick snapshots alone.
    return {**detail, "state": "UNKNOWN", "reason": "PULLBACK_NEEDS_COMPLETED_BARS_AND_SUPPORT"}


def observe_kr_timing_shadow(
    *, code: str, env: str, account_scope: str, observed_at: datetime,
    price: Any, selected_style: str, quote: dict[str, Any] | None,
    trigger_ok: bool, volume_proof: dict[str, Any] | None = None,
    setup_ok: bool = True,
) -> dict[str, Any]:
    """Append one bounded evidence row, replay that day's rows and classify."""
    now = observed_at.astimezone(KST)
    normalized = str(code).zfill(6)
    if len(normalized) != 6 or not normalized.isdecimal():
        raise ValueError("invalid KR symbol")
    if not (time(9, 0) <= now.time() <= time(15, 30)):
        return {"version": PROOF_VERSION, "state": "NOT_APPLICABLE", "reason": "OUTSIDE_KR_SESSION",
                "not_order_authorization": True}
    quote = quote if isinstance(quote, dict) else {}
    exchange_ts = _quote_exchange_ts(quote, now.date().isoformat())
    freshness_seconds = ((now - exchange_ts).total_seconds() if exchange_ts else None)
    fresh = bool(freshness_seconds is not None and -10 <= freshness_seconds <= 120)
    vwap = _number(quote.get("vwap") or quote.get("wghn_avrg_stck_prc"))
    raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else {}
    if vwap is None:
        vwap = _number(raw.get("vwap") or raw.get("wghn_avrg_stck_prc"))
    digest = hashlib.sha256(str(account_scope).encode("utf-8")).hexdigest()[:20]
    file_path = runtime_path("runtime", "kr_entry_timing_shadow", str(env).lower(),
                             digest, now.date().isoformat(), normalized + ".jsonl")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    observation = {
        "exchange_ts": exchange_ts.isoformat() if exchange_ts else None,
        "observed_at": now.isoformat(), "price": _number(price),
        "vwap": vwap, "quote_fresh": fresh, "style": selected_style,
    }
    # One append() call per tick, no read/write of order or fill tables.
    with file_path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(observation, ensure_ascii=False, separators=(",", ":")) + "\n")
    rows: list[dict[str, Any]] = []
    # One trading session is bounded; do not accidentally consume prior days.
    with file_path.open("r", encoding="utf-8") as source:
        for line in source:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    decision = evaluate_kr_timing_shadow(
        style=selected_style, samples=rows, observed_at=now,
        trigger_ok=trigger_ok, volume_proof=volume_proof, setup_ok=setup_ok,
    )
    return {**decision, "history_rows": len(rows), "quote_fresh": fresh,
            "quote_exchange_ts": exchange_ts.isoformat() if exchange_ts else None}
