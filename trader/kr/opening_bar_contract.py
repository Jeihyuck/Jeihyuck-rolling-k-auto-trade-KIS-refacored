"""Price-aware entry timing from genuine *completed* KIS KR minute bars.

Independent of the Final30 daily selection/ranking and existing frozen exit
contracts. Output is a hypothesis-only observation by default, not an order.
"""
from __future__ import annotations

from datetime import datetime, timedelta, time as dtime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo
import hashlib
import json
import math
import os

from trader.runtime_paths import runtime_path

KST = ZoneInfo("Asia/Seoul")
VERSION = "kr_opening_bar_opportunity_v1"
STYLES = {
    "ENTRY_MOMENTUM": "MOMENTUM",
    "ENTRY_MOMENTUM_CONTINUATION": "MOMENTUM",
    "ENTRY_MOMENTUM_SWING": "MOMENTUM",
    "ENTRY_PULLBACK": "PULLBACK",
    "ENTRY_PULLBACK_OVERRIDE": "PULLBACK",
    "ENTRY_BREAKOUT": "BREAKOUT",
    "ENTRY_BREAKOUT_SWING": "BREAKOUT",
}


def _positive(x):
    try:
        v = float(x)
        return v if math.isfinite(v) and v > 0 else None
    except (TypeError, ValueError):
        return None


def _source_time(raw: dict, date_str: str) -> datetime | None:
    if str(raw.get("date") or "").replace("-", "") != date_str.replace("-", ""):
        return None
    hhmmss = str(raw.get("time") or "").strip().zfill(6)
    if len(hhmmss) != 6 or not hhmmss.isdigit():
        return None
    try:
        return datetime.strptime(f"{date_str}T{hhmmss}", "%Y-%m-%dT%H%M%S").replace(tzinfo=KST)
    except ValueError:
        return None


def clean_completed_bars(rows: list[dict], now: datetime) -> list[dict]:
    """No invented OHLC, no previous date, future minute or missing volume."""
    now = now.astimezone(KST)
    current_minute = now.replace(second=0, microsecond=0)
    observed: dict[datetime, dict] = {}
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        stamp = _source_time(raw, now.date().isoformat())
        if stamp is None or not dtime(9, 0) <= stamp.time() < dtime(15, 30):
            continue
        # KIS minute time is the minute's opening timestamp. Exclude that
        # whole minute until closed; never use unfinished candle or future.
        if stamp + timedelta(minutes=1) > current_minute:
            continue
        o, h, l, c, v = [_positive(raw.get(k)) for k in
                          ("open", "high", "low", "close", "volume")]
        if None in (o, h, l, c, v) or not l <= o <= h or not l <= c <= h:
            continue
        observed[stamp] = {
            "timestamp": stamp.isoformat(), "open": o, "high": h,
            "low": l, "close": c, "volume": v,
        }
    return [observed[k] for k in sorted(observed)]


def _result(state: str, reason: str, **extra) -> dict:
    return {"version": VERSION, "state": state, "reason": reason,
            "not_order_authorization": True, **extra}


def evaluate_price_aware_entry(
    *, style: str, rows: list[dict], now: datetime,
    vwap: float | None, vwap_verified: bool, pivot: float | None = None,
    trigger_ok: bool = False, setup_ok: bool = True,
    order_price: float | None = None,
) -> dict:
    """Pure diagnostic states; threshold assumptions must be replay-tested."""
    family = STYLES.get(str(style or "").strip().upper())
    if family is None:
        return _result("NOT_APPLICABLE", "OTHER_OWNER_OR_STYLE")
    now = now.astimezone(KST)
    if now.time() < dtime(9, 30):
        return _result("WAIT", "OPENING_OBSERVATION")
    if now.time() > dtime(15, 5):
        return _result("INVALID", "ENTRY_WINDOW_EXPIRED")
    if not setup_ok:
        return _result("INVALID", "ORIGINAL_SETUP_FAILED")
    bars = clean_completed_bars(rows, now)
    opening = [b for b in bars if dtime(9, 0) <=
               datetime.fromisoformat(b["timestamp"]).time() < dtime(9, 30)]
    if (len(opening) < 25
            or datetime.fromisoformat(opening[0]["timestamp"]).time() > dtime(9, 3)
            or datetime.fromisoformat(opening[-1]["timestamp"]).time() < dtime(9, 26)):
        return _result("UNKNOWN", "OPENING_BARS_MISSING_OR_INCOMPLETE",
                       opening_count=len(opening))
    high = max(b["high"] for b in opening)
    low = min(b["low"] for b in opening)
    current = bars[-1] if bars else None
    if current is None:
        return _result("UNKNOWN", "NO_CURRENT_BAR")
    at = datetime.fromisoformat(current["timestamp"])
    if (now - at).total_seconds() > 180:
        return _result("UNKNOWN", "COMPLETED_BAR_STALE",
                       opening_high=high, opening_low=low)
    after_open = [b for b in bars if datetime.fromisoformat(b["timestamp"]).time() >= dtime(9, 30)]
    if len(after_open) < 3:
        return _result("WAIT", "POST_OPENING_CONFIRMATION_PENDING",
                       opening_high=high, opening_low=low)
    recent = after_open[-3:]
    stamps = [datetime.fromisoformat(b["timestamp"]) for b in recent]
    if any((b-a).total_seconds() != 60 for a,b in zip(stamps, stamps[1:])):
        return _result("UNKNOWN", "RECENT_MINUTE_GAP", opening_high=high, opening_low=low)
    close = current["close"]
    ordered_price = _positive(order_price) or close
    if ordered_price <= 0:
        return _result("UNKNOWN", "INVALID_ORDER_PRICE")
    # This is a within-day volume comparison, NOT time-of-day historical RVOL.
    base_minute_volume = sum(b["volume"] for b in opening) / len(opening)
    vol_ratio = (
        sum(b["volume"] for b in recent[-2:]) / (2 * base_minute_volume)
        if base_minute_volume > 0 else 0.0
    )
    rising = recent[0]["close"] < recent[1]["close"] < recent[2]["close"]
    higher_lows = recent[0]["low"] < recent[1]["low"] < recent[2]["low"]
    vwap_val = _positive(vwap) if vwap_verified else None
    proof = dict(
        family=family, opening_count=len(opening), opening_high=high,
        opening_low=low, reference_price=close, latest_bar_ts=current["timestamp"],
        observed_volume_ratio=round(vol_ratio, 5),
        vwap=vwap_val, vwap_proven=bool(vwap_val is not None),
        increasing_close=rising, higher_lows=higher_lows,
    )
    if abs(ordered_price / close - 1.0) > 0.004:
        return _result("WAIT", "ORDER_PRICE_MOVED_FROM_CONFIRMED_BAR", **proof)

    # Structural risk/room are entry-quality references, not live stop/TP.
    stop = min(b["low"] for b in recent[-5:])
    risk = ordered_price - stop
    if risk <= 0:
        return _result("WAIT", "NO_VALID_STRUCTURAL_RISK", **proof)
    if family == "MOMENTUM":
        if vwap_val is None:
            return _result("UNKNOWN", "NO_VERIFIED_INTRADAY_VWAP", **proof)
        if not (rising and higher_lows and close > vwap_val and vol_ratio >= 1.2):
            return _result("WAIT", "NO_CONFIRMED_MOMENTUM_RECLAIM", **proof)
        if close >= high and trigger_ok:
            cap = high * 1.005
            if ordered_price > cap:
                return _result("WAIT", "BREAKOUT_CHASE_TOO_EXPENSIVE", **proof)
            return _result("READY_SHADOW", "MOMENTUM_BREAKOUT_RECONFIRMED",
                           max_entry_price=cap, **proof)
        # Reclaim before opening range high: enough space to the resistance
        # is required or 09:30 chasing a rebound is adverse risk/reward.
        room = high - ordered_price
        if room / risk < 1.5:
            return _result("WAIT", "INSUFFICIENT_ROOM_VS_STOP",
                           room_to_opening_high=room, risk_to_recent_low=risk, **proof)
        return _result("READY_SHADOW", "MOMENTUM_PULLBACK_RECLAIM",
                       max_entry_price=min(close * 1.0025, high), **proof)

    if family == "PULLBACK":
        # Do NOT borrow the momentum VWAP/price-breakout requirement.
        earlier = after_open[-8:]
        had_dip = any(b["low"] <= high * 0.99 for b in earlier)
        if not (had_dip and higher_lows and rising
                and close > recent[-2]["high"] and vol_ratio >= 1.0):
            return _result("WAIT", "PULLBACK_SUPPORT_REBOUND_NOT_CONFIRMED", **proof)
        if (high - ordered_price) / risk < 1.5:
            return _result("WAIT", "PULLBACK_NO_ROOM_VS_STOP", **proof)
        return _result("READY_SHADOW", "PULLBACK_REBOUND_CONFIRMED",
                       max_entry_price=min(close * 1.0025, high), **proof)

    # BREAKOUT requires actual owner-approved trigger and sufficient live
    # minute volume, never the setup-only OR bypass.
    pivot_val = _positive(pivot)
    if pivot_val is None or not trigger_ok:
        return _result("WAIT", "ORIGINAL_PIVOT_TRIGGER_UNCONFIRMED", **proof)
    breakout_level = max(high, pivot_val)
    if close <= breakout_level or not rising or vol_ratio < 1.5:
        return _result("WAIT", "BREAKOUT_PRICE_OR_VOLUME_NOT_CONFIRMED", **proof)
    cap = breakout_level * 1.005
    if ordered_price > cap:
        return _result("WAIT", "BREAKOUT_CHASE_TOO_EXPENSIVE", **proof)
    return _result("READY_SHADOW", "BREAKOUT_PRICE_VOLUME_CONFIRMED",
                   max_entry_price=cap, **proof)


def _evidence_file(env: str, account_scope: str, code: str, now: datetime) -> Path:
    digest = hashlib.sha256(str(account_scope).encode("utf-8")).hexdigest()[:24]
    return runtime_path("runtime", "kr_opening_bars", str(env).lower(),
                        digest, now.date().isoformat(), f"{str(code).zfill(6)}.json")


def save_opening_proof(*, env: str, account_scope: str, code: str,
                       now: datetime, raw_bars: list[dict], vwap: float | None,
                       vwap_source: str) -> dict:
    now = now.astimezone(KST)
    cleaned = clean_completed_bars(raw_bars, now)
    payload = {
        "version": VERSION, "env": str(env).lower(), "code": str(code).zfill(6),
        "as_of": now.isoformat(), "trade_date": now.date().isoformat(),
        "source": "KIS_FHKST03010200", "bars": [
            {**bar, "date": now.date().isoformat(), "time": datetime.fromisoformat(bar["timestamp"]).strftime("%H%M%S")}
            for bar in cleaned
        ],
        "vwap": _positive(vwap),
        "vwap_source": vwap_source if _positive(vwap) else "UNKNOWN",
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    payload["sha256"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    path = _evidence_file(env, account_scope, code, now)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)
    return {"path": str(path), "bars": len(cleaned), "sha256": payload["sha256"]}


def load_opening_proof(*, env: str, account_scope: str, code: str, now: datetime) -> dict | None:
    now = now.astimezone(KST)
    path = _evidence_file(env, account_scope, code, now)
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        stamp = datetime.fromisoformat(payload.get("as_of") or "").astimezone(KST)
        if (payload.get("version") != VERSION
                or payload.get("code") != str(code).zfill(6)
                or payload.get("env") != str(env).lower()
                or payload.get("trade_date") != now.date().isoformat()
                or stamp > now or (now - stamp).total_seconds() > 180):
            return None
        digest = payload.pop("sha256", "")
        canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if digest != hashlib.sha256(canonical.encode("utf-8")).hexdigest():
            return None
        payload["sha256"] = digest
        return payload
    except (ValueError, TypeError, OSError):
        return None


def evaluate_proof_for_candidate(*, env: str, account_scope: str, code: str,
                                 now: datetime, style: str, setup_ok: bool,
                                 trigger_ok: bool, pivot: float | None,
                                 order_price: float) -> dict:
    payload = load_opening_proof(
        env=env, account_scope=account_scope, code=code, now=now,
    )
    if not payload:
        return _result("UNKNOWN", "NO_FRESH_VERIFIED_KIS_MINUTE_BAR_PROOF")
    return evaluate_price_aware_entry(
        style=style, rows=payload["bars"], now=now,
        vwap=payload["vwap"],
        vwap_verified=payload["vwap_source"] == "KIS_NATIVE_WEIGHTED_AVG",
        pivot=pivot, trigger_ok=trigger_ok, setup_ok=setup_ok,
        order_price=order_price,
    )
