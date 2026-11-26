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
# One symmetric limit for both signal evaluation and broker-bound fallback.
# It is an existing 0.4% policy, not a new / relaxed threshold.
MAX_ORDER_PRICE_DEVIATION = 0.004
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


def diagnose_native_bar_rows(rows: list[dict], now: datetime) -> dict:
    """Count exact reasons KIS minute source rows are NOT usable as proof.

    Diagnostic only: no synthetic candles, volume, VWAP, timestamps or entry
    authorization. Labels distinguish absent exchange dates from KIS's
    genuine no-trade minute buckets and from strict OHLC rejection.
    """
    from collections import Counter

    now = now.astimezone(KST)
    now_minute = now.replace(second=0, microsecond=0)
    counts: Counter[str] = Counter()
    valid_minutes: set[str] = set()
    duplicates = 0
    for row in rows:
        if not isinstance(row, dict):
            counts["ROW_NOT_MAPPING"] += 1
            continue
        raw_day = str(row.get("date") or "").replace("-", "").strip()
        if not raw_day:
            counts["SOURCE_DATE_MISSING"] += 1
            continue
        if raw_day != now.strftime("%Y%m%d"):
            counts["SOURCE_DATE_MISMATCH"] += 1
            continue
        stamp = _source_time(row, now.date().isoformat())
        if stamp is None:
            counts["SOURCE_CLOCK_INVALID"] += 1
            continue
        if not dtime(9, 0) <= stamp.time() < dtime(15, 30):
            counts["OUTSIDE_KRX_SESSION"] += 1
            continue
        if stamp + timedelta(minutes=1) > now_minute:
            counts["CURRENT_OR_FUTURE_MINUTE"] += 1
            continue
        o, h, l, c, vol = [_positive(row.get(k)) for k in
                            ("open", "high", "low", "close", "volume")]
        if None in (o, h, l, c, vol):
            counts["OHLCV_MISSING_OR_NONPOSITIVE"] += 1
            continue
        if not (l <= o <= h and l <= c <= h):
            counts["OHLC_INCONSISTENT"] += 1
            continue
        key = stamp.isoformat()
        if key in valid_minutes:
            duplicates += 1
        valid_minutes.add(key)
        counts["VALID_COMPLETED_NATIVE_ROW"] += 1
    return {
        "total_rows": len(rows),
        "valid_unique_minutes": len(valid_minutes),
        "duplicate_valid_rows": duplicates,
        "excluded_by_reason": {
            k: int(v) for k, v in sorted(counts.items())
            if k != "VALID_COMPLETED_NATIVE_ROW"
        },
        "first_valid_native": min(valid_minutes) if valid_minutes else None,
        "last_valid_native": max(valid_minutes) if valid_minutes else None,
        "no_trade_minutes_proven": False,
    }


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



def opening_range_ready(opening: list[dict]) -> bool:
    """Legacy opening-reference quality criterion, not a universal BUY gate."""
    return bool(len(opening) >= 25
                and datetime.fromisoformat(opening[0]["timestamp"]).time() <= dtime(9, 3)
                and datetime.fromisoformat(opening[-1]["timestamp"]).time() >= dtime(9, 26))


def _completed_bar_structure(bars: list[dict], now: datetime) -> tuple[str, str, list[dict], list[dict]]:
    """One shared structural contract for observer health and BUY evaluator.

    No volume/momentum/pullback/breakout thresholds are modified here.
    """
    now = now.astimezone(KST)
    opening = [bar for bar in bars if dtime(9, 0) <=
               datetime.fromisoformat(bar["timestamp"]).time() < dtime(9, 30)]
    after_open = [bar for bar in bars if
                  datetime.fromisoformat(bar["timestamp"]).time() >= dtime(9, 30)]
    if now.time() < dtime(9, 30):
        return "WAIT", "OPENING_OBSERVATION", opening, after_open
    if not opening_range_ready(opening):
        return "UNKNOWN", "OPENING_BARS_MISSING_OR_INCOMPLETE", opening, after_open
    if not bars:
        return "UNKNOWN", "NO_CURRENT_BAR", opening, after_open
    at = datetime.fromisoformat(bars[-1]["timestamp"])
    if (now - at).total_seconds() > 180:
        return "UNKNOWN", "COMPLETED_BAR_STALE", opening, after_open
    if len(after_open) < 3:
        return "WAIT", "POST_OPENING_CONFIRMATION_PENDING", opening, after_open
    stamps = [datetime.fromisoformat(bar["timestamp"]) for bar in after_open[-3:]]
    if any((b - a).total_seconds() != 60 for a, b in zip(stamps, stamps[1:])):
        return "UNKNOWN", "RECENT_MINUTE_GAP", opening, after_open
    return "COVERED", "STRUCTURAL_1M_BARS_AVAILABLE", opening, after_open


def _native_window_structure(after_open: list[dict], now: datetime) -> tuple[str, str]:
    """One structural check shared by collection and the 5+3 evaluator."""
    stamps = [datetime.fromisoformat(bar["timestamp"]) for bar in after_open[-8:]]
    if any((right - left).total_seconds() != 60 for left, right in zip(stamps, stamps[1:])):
        return "UNKNOWN", "POST_OPENING_NATIVE_WINDOW_GAP"
    if len(stamps) < 8:
        return "WAIT", "POST_OPENING_NATIVE_WINDOW_PENDING"
    if not 0 <= (now.astimezone(KST) - stamps[-1]).total_seconds() <= 180:
        return "UNKNOWN", "COMPLETED_BAR_STALE"
    return "COVERED", "POST_OPENING_NATIVE_WINDOW_AVAILABLE"


def inspect_completed_bar_coverage(*, rows: list[dict], now: datetime,
                                   source_conflicts: list | None = None) -> dict:
    """Read-only structural proof; COVERED never grants BUY authorization."""
    now = now.astimezone(KST)
    bars = clean_completed_bars(rows, now)
    state, reason, opening, after_open = _completed_bar_structure(bars, now)
    opening_minutes = sorted({
        datetime.fromisoformat(bar["timestamp"]).strftime("%H:%M")
        for bar in opening
    })
    observed = set(opening_minutes)
    unobserved = [
        f"09:{i:02d}" for i in range(30) if f"09:{i:02d}" not in observed
    ]
    native_window_state, native_window_reason = _native_window_structure(after_open, now)
    recent_native_window_ready = native_window_state == "COVERED"
    # Retain historical opening diagnostics, but expose the independent
    # admissible data path separately. No structural verdict is authority.
    scope = _conflict_scope(bars=bars, conflicts=source_conflicts or [],
                            now=now, family="PULLBACK")
    independent_ready = bool(recent_native_window_ready
                             and scope["independent_window_ready"]
                             and not scope["invalid"])
    opening_ready = bool(opening_range_ready(opening)
                         and not scope["opening_conflicts"] and not scope["invalid"])
    required_recent = 4 if active_entry_policy(os.getenv("KIS_ENV", "practice")) == "US_ALIGNED_V2" else 3
    if state == "COVERED" and required_recent == 4:
        recent = after_open[-4:]
        if len(recent) < 4:
            state, reason = "WAIT", "POST_OPENING_CONFIRMATION_PENDING"
        elif any((datetime.fromisoformat(b["timestamp"]) -
                  datetime.fromisoformat(a["timestamp"])).total_seconds() != 60
                 for a, b in zip(recent, recent[1:])):
            state, reason = "UNKNOWN", "RECENT_MINUTE_GAP"
    return {
        "required_recent_minutes": required_recent,
        "state": state, "reason": reason,
        "opening_range_ready": opening_ready,
        "native_post_open_window_ready": independent_ready,
        "native_post_open_window_state": native_window_state,
        "native_post_open_window_reason": native_window_reason,
        "available_native_paths": [
            *([ "OPENING_RANGE" ] if state == "COVERED" and opening_ready
               and not scope["full_conflicts"] else []),
            *([ "POST_OPENING_5_PLUS_3" ] if independent_ready else []),
        ],
        "source_conflicts": list(source_conflicts or []),
        "window_conflicts": scope["window_conflicts"],
        "bars": len(bars), "opening_count": len(opening),
        # Unobserved does NOT prove a broker data gap: some minutes may have
        # no genuine trades. Never synthesize their OHLC or volume.
        "opening_valid_minutes": opening_minutes,
        "opening_unobserved_minutes": unobserved,
        "post_open_count": len(after_open),
        "latest_bar_ts": bars[-1]["timestamp"] if bars else None,
        "latest_bar_age_sec": (
            round((now - datetime.fromisoformat(bars[-1]["timestamp"])).total_seconds(), 3)
            if bars else None
        ),
    }


def _result(state: str, reason: str, **extra) -> dict:
    return {"version": VERSION, "state": state, "reason": reason,
            "not_order_authorization": True, **extra}


def _evaluate_verified_intraday_window(
    *, family: str, opening: list[dict], after_open: list[dict],
    now: datetime, vwap: float | None, vwap_verified: bool,
    pivot: float | None, trigger_ok: bool, order_price: float | None,
) -> dict:
    """Use independent native post-open candles when the opening range is incomplete.

    The previous five and latest three *consecutive completed minutes* are
    precisely the lookbacks needed by the existing rebound / risk checks.
    Their high and mean volume are local verified references, NEVER represented
    as the full 09:00-09:29 opening high or 30-minute volume.
    An incomplete native opening interval remains visible in every verdict.
    """
    proof = {
        "family": family, "opening_count": len(opening),
        "opening_range_complete": False,
        "reference_source": "KIS_NATIVE_POST_OPENING_5_PLUS_3",
    }
    window = after_open[-8:]
    native_state, native_reason = _native_window_structure(after_open, now)
    if native_state != "COVERED":
        return _result(native_state, native_reason, **proof)
    current = window[-1]

    earlier, recent = window[:5], window[-3:]
    high = max(bar["high"] for bar in earlier)
    low = min(bar["low"] for bar in earlier)
    close = current["close"]
    price = _positive(order_price) or close
    if price <= 0:
        return _result("UNKNOWN", "INVALID_ORDER_PRICE", **proof)
    base_volume = sum(bar["volume"] for bar in earlier) / len(earlier)
    vol_ratio = (sum(bar["volume"] for bar in recent[-2:])
                 / (2 * base_volume)) if base_volume > 0 else 0.0
    rising = recent[0]["close"] < recent[1]["close"] < recent[2]["close"]
    higher_lows = recent[0]["low"] < recent[1]["low"] < recent[2]["low"]
    native_vwap = _positive(vwap) if vwap_verified else None
    proof.update(
        reference_price=close, intraday_reference_high=high,
        intraday_reference_low=low, latest_bar_ts=current["timestamp"],
        observed_volume_ratio=round(vol_ratio, 5),
        vwap=native_vwap, vwap_proven=bool(native_vwap is not None),
        increasing_close=rising, higher_lows=higher_lows,
    )
    if abs(price / close - 1.0) > MAX_ORDER_PRICE_DEVIATION:
        return _result("WAIT", "ORDER_PRICE_MOVED_FROM_CONFIRMED_BAR", **proof)

    stop = min(bar["low"] for bar in recent)
    risk = price - stop
    if risk <= 0:
        return _result("WAIT", "NO_VALID_STRUCTURAL_RISK", **proof)

    if family == "PULLBACK":
        had_dip = any(bar["low"] <= high * 0.99 for bar in earlier)
        if not (had_dip and rising and higher_lows
                and close > recent[-2]["high"] and vol_ratio >= 1.0):
            return _result("WAIT", "PULLBACK_SUPPORT_REBOUND_NOT_CONFIRMED", **proof)
        if (high - price) / risk < 1.5:
            return _result("WAIT", "PULLBACK_NO_ROOM_VS_STOP", **proof)
        return _result("READY_SHADOW", "PULLBACK_INTRADAY_NATIVE_REBOUND_CONFIRMED",
                       max_entry_price=min(close * 1.0025, high), **proof)

    if family == "MOMENTUM":
        if native_vwap is None:
            return _result("UNKNOWN", "NO_VERIFIED_INTRADAY_VWAP", **proof)
        if not (rising and higher_lows and close > native_vwap
                and vol_ratio >= 1.2):
            return _result("WAIT", "NO_CONFIRMED_MOMENTUM_RECLAIM", **proof)
        if close >= high and trigger_ok:
            # The five recent post-open bars are NOT a substitute for a
            # verified 09:00-09:29 opening-range high. Retain owner-approved
            # breakout uncertainty instead of authorizing an easier entry.
            return _result("UNKNOWN",
                           "OPENING_RANGE_HIGH_UNVERIFIED_FOR_MOMENTUM_BREAKOUT",
                           **proof)
        if (high - price) / risk < 1.5:
            return _result("WAIT", "INSUFFICIENT_ROOM_VS_STOP", **proof)
        return _result("READY_SHADOW", "MOMENTUM_INTRADAY_NATIVE_RECLAIM",
                       max_entry_price=min(close * 1.0025, high), **proof)

    # Original BREAKOUT owner approval and a verified pivot are mandatory.
    # A verified daily pivot cannot by itself replace the *opening high* in
    # the existing max(opening_high,pivot) contract. Incomplete native 09:00
    # coverage means that operand is unknown; continue read-only observation.
    pivot_val = _positive(pivot)
    if pivot_val is None or not trigger_ok:
        return _result("WAIT", "ORIGINAL_PIVOT_TRIGGER_UNCONFIRMED", **proof)
    return _result("UNKNOWN", "OPENING_RANGE_HIGH_UNVERIFIED_FOR_BREAKOUT",
                   verified_pivot=pivot_val, **proof)


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
    structure_state, structure_reason, opening, after_open = _completed_bar_structure(bars, now)
    if structure_state != "COVERED":
        if structure_reason == "OPENING_BARS_MISSING_OR_INCOMPLETE":
            if not after_open:
                # No post-opening market observation exists to evaluate.
                return _result("UNKNOWN", structure_reason, opening_count=len(opening))
            # An opening completeness diagnostic is not a *universal* BUY
            # veto. Only use a separately confirmed post-opening calculation
            # whose data dependency is fully satisfied.
            return _evaluate_verified_intraday_window(
                family=family, opening=opening, after_open=after_open,
                now=now, vwap=vwap, vwap_verified=vwap_verified,
                pivot=pivot, trigger_ok=trigger_ok, order_price=order_price,
            )
        meta = {"opening_count": len(opening)}
        if opening:
            meta.update(observed_opening_high=max(b["high"] for b in opening),
                        observed_opening_low=min(b["low"] for b in opening))
        return _result(structure_state, structure_reason, **meta)
    high = max(b["high"] for b in opening)
    low = min(b["low"] for b in opening)
    current = bars[-1]
    recent = after_open[-3:]
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
    if abs(ordered_price / close - 1.0) > MAX_ORDER_PRICE_DEVIATION:
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


class OpeningProofLockTimeout(TimeoutError):
    """The read-only evidence writer could not acquire its per-symbol lock."""


def _acquire_opening_proof_lock(lock_file, *, timeout_seconds: float = 2.0) -> None:
    """Never let an evidence-writer lock stall a collector indefinitely."""
    import fcntl
    import time

    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError as exc:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise OpeningProofLockTimeout("OPENING_PROOF_LOCK_TIMEOUT") from exc
            time.sleep(min(0.05, remaining))


def save_opening_proof(*, env: str, account_scope: str, code: str,
                       now: datetime, raw_bars: list[dict], vwap: float | None,
                       vwap_source: str) -> dict:
    """Merge attested native complete minutes with durable conflict quarantine.

    Previously stored opening bars can be reused, but never synthesized.
    A revision to a completed minute is quarantined until THREE matching
    KIS-native completed-minute observations at least 60 seconds apart
    confirm a single value over >=120 seconds.  Intra-response conflicts
    cannot satisfy that proof.  Every resolution retains its audit trail.
    """
    import tempfile
    import fcntl

    now = now.astimezone(KST)
    new_bars = clean_completed_bars(raw_bars, now)
    # Detect contradictory native OHLCV versions in a *single* REST collection
    # pass. clean_completed_bars alone uses the last row for each minute.
    seen_this_pass: dict[str, dict] = {}
    intra_pass_conflicts: set[str] = set()
    # These IDs are generated at the successful authenticated KIS HTTP
    # response boundary, not from OHLCV contents or by this persistence layer.
    # The same cached/native-format response cannot advance a quarantine
    # multiple times, even across process restarts or spaced calls.
    http_observation_ids: dict[str, set[str]] = {}
    for raw in raw_bars:
        for one in clean_completed_bars([raw], now):
            key = one["timestamp"]
            if key in seen_this_pass and seen_this_pass[key] != one:
                intra_pass_conflicts.add(key)
            seen_this_pass[key] = one
            response_id = raw.get("_kis_http_observation_id")
            if (isinstance(response_id, str) and len(response_id) == 32
                    and all(char in "0123456789abcdef" for char in response_id)):
                http_observation_ids.setdefault(key, set()).add(response_id)

    path = _evidence_file(env, account_scope, code, now)
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(".lock")
    with lock_path.open("a+b") as lock_file:
        _acquire_opening_proof_lock(lock_file)
        try:
            previous = load_opening_proof(
                env=env, account_scope=account_scope, code=code, now=now,
                max_age_seconds=None,
            )
            old_bars = clean_completed_bars((previous or {}).get("bars") or [], now)
            merged = {bar["timestamp"]: bar for bar in old_bars}
            conflicts = set((previous or {}).get("source_conflicts") or [])
            observations = dict((previous or {}).get("conflict_observations") or {})
            resolved_history = list((previous or {}).get("resolved_source_conflicts") or [])
            resolved_this_pass: list[str] = []
            added = 0

            for bar in new_bars:
                key = bar["timestamp"]
                current = merged.get(key)
                changed = current is not None and current != bar
                quarantined = changed or key in conflicts or key in intra_pass_conflicts
                if quarantined:
                    conflicts.add(key)
                    prev = observations.get(key) or {}
                    identical = prev.get("candidate") == bar
                    last_stamp = None
                    try:
                        last_stamp = datetime.fromisoformat(
                            str(prev.get("last_counted_at") or "")
                        )
                    except (ValueError, TypeError):
                        pass
                    separated = (last_stamp is None or
                                 (now - last_stamp).total_seconds() >= 60)
                    # Pre-upgrade counters with no authenticated HTTP receipt
                    # are not independent proof and must be revalidated.
                    counted_ids = list(prev.get("counted_http_observation_ids") or []) if identical else []
                    distinct_ids = sorted(
                        candidate_id for candidate_id in http_observation_ids.get(key, set())
                        if candidate_id not in counted_ids
                    )
                    first_at = str(prev.get("first_candidate_seen") or "") if identical else now.isoformat()
                    if separated and key not in intra_pass_conflicts and distinct_ids:
                        # At most one confirmation per store pass, and only
                        # one confirmed ID for each separate KIS HTTP response.
                        counted_ids.append(distinct_ids[0])
                        last_counted = now.isoformat()
                    else:
                        last_counted = str(prev.get("last_counted_at") or "") if identical else ""
                    count = len(counted_ids)
                    observations[key] = {
                        "original": prev.get("original") or current or bar,
                        "candidate": bar, "first_candidate_seen": first_at,
                        "last_observed_at": now.isoformat(),
                        "last_counted_at": last_counted,
                        "stable_native_reads": count,
                        "counted_http_observation_ids": counted_ids,
                        "unverified_native_replay": not bool(distinct_ids),
                        "source": "KIS_FHKST03010200",
                        "intra_pass_conflict": key in intra_pass_conflicts,
                    }
                    try:
                        span = (now - datetime.fromisoformat(first_at)).total_seconds()
                    except (TypeError, ValueError):
                        span = 0
                    if (count >= 3 and span >= 120
                            and key not in intra_pass_conflicts):
                        resolved_history.append({
                            "minute": key, "original": observations[key]["original"],
                            "accepted": bar, "confirmed_native_reads": count,
                            "first_seen": first_at, "resolved_at": now.isoformat(),
                            "distinct_http_responses": len(counted_ids),
                            "resolution": "THREE_SPACED_KIS_NATIVE_CONFIRMATIONS",
                        })
                        merged[key] = bar
                        conflicts.discard(key)
                        observations.pop(key, None)
                        resolved_this_pass.append(key)
                    elif current is None:
                        # Retain an untrusted copy only for forensic diagnosis.
                        # A conflict flag still prohibits BUY with this proof.
                        merged[key] = bar
                    continue
                if current is None:
                    added += 1
                merged[key] = bar

            # An unresolved conflicting native minute remains a hard block
            # through process restarts, even if no new data arrived this pass.
            bars = [merged[key] for key in sorted(merged)]
            payload = {
                "version": VERSION, "env": str(env).lower(),
                "code": str(code).zfill(6),
                "as_of": now.isoformat(), "trade_date": now.date().isoformat(),
                "source": "KIS_FHKST03010200",
                "bars": [
                    {**bar, "date": now.date().isoformat(),
                     "time": datetime.fromisoformat(bar["timestamp"]).strftime("%H%M%S")}
                    for bar in bars
                ],
                "vwap": _positive(vwap),
                "vwap_source": vwap_source if _positive(vwap) else "UNKNOWN",
                "source_conflicts": sorted(conflicts),
                "conflict_observations": observations,
                "resolved_source_conflicts": resolved_history[-100:],
            }
            canonical = json.dumps(
                payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            )
            payload["sha256"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
            temp_name = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w", encoding="utf-8", dir=path.parent,
                    prefix=path.stem + ".", suffix=".tmp", delete=False,
                ) as temp_file:
                    temp_name = temp_file.name
                    json.dump(payload, temp_file, ensure_ascii=False)
                    temp_file.flush()
                    os.fsync(temp_file.fileno())
                os.replace(temp_name, path)
            finally:
                if temp_name and os.path.exists(temp_name):
                    os.unlink(temp_name)
            return {
                "path": str(path), "bars": len(bars),
                "new_bars": len(new_bars), "reused_bars": len(old_bars),
                "added_bars": added, "source_conflicts": sorted(conflicts),
                "resolved_conflicts": resolved_this_pass,
                "intra_pass_conflicts": sorted(intra_pass_conflicts),
                "sha256": payload["sha256"],
            }
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)



def load_opening_proof(*, env: str, account_scope: str, code: str, now: datetime,
                       max_age_seconds: float | None = 180) -> dict | None:
    now = now.astimezone(KST)
    path = _evidence_file(env, account_scope, code, now)
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return None
        stamp = datetime.fromisoformat(payload.get("as_of") or "")
        if stamp.tzinfo is None:
            return None  # Never reinterpret a naive WSL timestamp as KST.
        stamp = stamp.astimezone(KST)
        if (payload.get("version") != VERSION
                or payload.get("source") != "KIS_FHKST03010200"
                or not isinstance(payload.get("bars"), list)
                or not isinstance(payload.get("source_conflicts", []), list)
                or payload.get("code") != str(code).zfill(6)
                or payload.get("env") != str(env).lower()
                or payload.get("trade_date") != now.date().isoformat()
                or stamp > now
                or (max_age_seconds is not None
                    and (now - stamp).total_seconds() > max_age_seconds)):
            return None
        digest = payload.pop("sha256", "")
        canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if digest != hashlib.sha256(canonical.encode("utf-8")).hexdigest():
            return None
        payload["sha256"] = digest
        return payload
    except (ValueError, TypeError, OSError):
        return None


def _conflict_scope(*, bars: list[dict], conflicts: list, now: datetime, family: str) -> dict:
    """Use the actual dependency window; never remove quarantined source rows.

    Opening references depend on every observed opening minute. Pullback uses
    the last eight post-open bars; momentum/breakout use the last three.
    An independent 5+3 path may not consume ANY quarantined window minute.
    """
    opening = [b for b in bars if datetime.fromisoformat(b["timestamp"]).time() < dtime(9, 30)]
    post = [b for b in bars if datetime.fromisoformat(b["timestamp"]).time() >= dtime(9, 30)]
    keys = {b["timestamp"] for b in bars}
    invalid = False
    normalized = set()
    for raw in conflicts:
        try:
            stamp = datetime.fromisoformat(raw)
            if stamp.tzinfo is None:
                raise ValueError("naive conflict")
            key = stamp.astimezone(KST).isoformat()
            if key not in keys:
                raise ValueError("unmatched conflict")
            normalized.add(key)
        except (TypeError, ValueError, OverflowError):
            invalid = True
    open_keys = {b["timestamp"] for b in opening}
    full_recent_keys = {b["timestamp"] for b in post[-(8 if family == "PULLBACK" else 3):]}
    window = post[-8:]
    window_keys = {b["timestamp"] for b in window}
    stamps = [datetime.fromisoformat(b["timestamp"]) for b in window]
    independent = (len(stamps) == 8
                   and all((b - a).total_seconds() == 60 for a, b in zip(stamps, stamps[1:]))
                   and 0 <= (now.astimezone(KST) - stamps[-1]).total_seconds() <= 180
                   and not (normalized & window_keys))
    return {"invalid": invalid, "opening_conflicts": sorted(normalized & open_keys),
            "full_conflicts": sorted(normalized & (open_keys | full_recent_keys)),
            "window_conflicts": sorted(normalized & window_keys),
            "independent_window_ready": independent,
            "opening": opening, "post": post}


def active_entry_policy(env: str) -> str:
    """Explicit WSL profile selects V2; legacy remains a reversible ENFORCE policy."""
    if str(env).lower() != "practice":
        return "LEGACY_V1"
    return str(os.getenv("KR_PB1_ENTRY_OPPORTUNITY_POLICY", "LEGACY_V1")).strip().upper()


def _evaluate_us_aligned_reclaim(*, payload, now, family, setup_ok, trigger_ok, order_price):
    from trader.kr.entry_opportunity_v2 import evaluate_reclaim
    if not setup_ok:
        return _result("INVALID", "ORIGINAL_SETUP_FAILED")
    now = now.astimezone(KST)
    if not dtime(9, 30) <= now.time() <= dtime(15, 5):
        return _result("WAIT", "OUTSIDE_ENTRY_WINDOW")
    bars = clean_completed_bars(payload["bars"], now)
    conflicts = payload.get("source_conflicts") or []
    scope = _conflict_scope(bars=bars, conflicts=conflicts, now=now, family=family)
    if scope["invalid"]:
        return _result("UNKNOWN", "CONFLICTING_NATIVE_COMPLETED_BAR")
    opening, post = scope["opening"], scope["post"]
    # V2 reads a longer 15-minute trough; include every operand in conflict
    # scope even if V1 would have ignored that historical conflict.
    conflict_keys = {datetime.fromisoformat(raw).astimezone(KST).isoformat()
                     for raw in conflicts}
    recent_keys = {b["timestamp"] for b in post[-15:]}
    full_ok = opening_range_ready(opening) and not (
        scope["opening_conflicts"] or conflict_keys & recent_keys)
    if full_ok:
        independent = False
    elif scope["independent_window_ready"]:
        independent = True
    else:
        if conflicts and (scope["opening_conflicts"] or conflict_keys & recent_keys):
            return _result("UNKNOWN", "CONFLICTING_NATIVE_COMPLETED_BAR",
                           source_conflicts=list(conflicts))
        state, reason = _native_window_structure(post, now)
        return _result(state if state != "COVERED" else "UNKNOWN", reason,
                       opening_count=len(opening))
    if os.getenv("KR_PB1_ADAPTIVE_ENTRY_V3_ENABLED", "0") == "1":
        from trader.kr.entry_opportunity_v3 import evaluate_entry
        return evaluate_entry(
            family=family, opening=opening, post=post, now=now,
            order_price=order_price, independent=independent,
            payload=payload, conflict_keys=conflict_keys,
            setup_ok=setup_ok, trigger_ok=trigger_ok,
        )
    # Explicit rollback configuration keeps the original strict-only path.
    return evaluate_reclaim(
        family=family, opening=opening, post=post, now=now,
        order_price=order_price, independent=independent,
        vwap=payload.get("vwap"),
        vwap_verified=payload.get("vwap_source") == "KIS_NATIVE_WEIGHTED_AVG",
        trigger_ok=trigger_ok,
    )



def evaluate_proof_for_candidate(*, env: str, account_scope: str, code: str,
                                 now: datetime, style: str, setup_ok: bool,
                                 trigger_ok: bool, pivot: float | None,
                                 order_price: float) -> dict:
    policy = active_entry_policy(env)
    family = STYLES.get(str(style or "").strip().upper())
    if policy not in {"LEGACY_V1", "US_ALIGNED_V2"}:
        return _result("UNKNOWN", "ENTRY_OPPORTUNITY_POLICY_INVALID",
                       entry_policy=policy)
    payload = load_opening_proof(
        env=env, account_scope=account_scope, code=code, now=now,
    )
    if not payload:
        return _result("UNKNOWN", "NO_FRESH_VERIFIED_KIS_MINUTE_BAR_PROOF",
                       entry_policy=policy if family in {"PULLBACK", "MOMENTUM"} else "LEGACY_V1")
    if policy == "US_ALIGNED_V2" and family in {"PULLBACK", "MOMENTUM"}:
        # An owner-confirmed momentum breakout remains on its separate V1
        # opening/pivot contract, not an easier reclaim substitution.
        result = _evaluate_us_aligned_reclaim(
            payload=payload, now=now, family=family, setup_ok=setup_ok,
            trigger_ok=trigger_ok, order_price=order_price,
        )
        if result.get("reason") != "MOMENTUM_BREAKOUT_REQUIRES_OWNER_PATH":
            # Early UNKNOWN/WAIT returns must not be mislabeled as legacy.
            # Preserve the owner breakout fallback below.
            return {**result, "entry_policy": policy}
    conflicts = payload.get("source_conflicts") or []
    if conflicts:
        family = STYLES.get(str(style or "").strip().upper())
        bars = clean_completed_bars(payload["bars"], now)
        scope = _conflict_scope(bars=bars, conflicts=conflicts, now=now, family=family or "")
        diagnostic = {"source_conflicts": list(conflicts),
                      "relevant_source_conflicts": scope["full_conflicts"],
                      "native_window_conflicts": scope["window_conflicts"]}
        if scope["invalid"] or not family:
            return _result("UNKNOWN", "CONFLICTING_NATIVE_COMPLETED_BAR", **diagnostic)
        structure, _, _, _ = _completed_bar_structure(bars, now)
        if structure == "COVERED" and not scope["full_conflicts"]:
            # Historical conflict outside every operand of this full proof.
            verdict = evaluate_price_aware_entry(
                style=style, rows=payload["bars"], now=now, vwap=payload["vwap"],
                vwap_verified=payload["vwap_source"] == "KIS_NATIVE_WEIGHTED_AVG",
                pivot=pivot, trigger_ok=trigger_ok, setup_ok=setup_ok,
                order_price=order_price,
            )
            return {**verdict, **diagnostic, "conflict_scope": "OUTSIDE_FULL_DEPENDENCIES"}
        if (family in {"PULLBACK", "MOMENTUM"} and scope["independent_window_ready"]
                and setup_ok and dtime(9, 30) <= now.astimezone(KST).time() <= dtime(15, 5)):
            # Do not use an opening high/volume contaminated by a conflict.
            # The existing independent evaluator consumes only the clean
            # latest eight bars, never an invented opening range.
            verdict = _evaluate_verified_intraday_window(
                family=family, opening=scope["opening"], after_open=scope["post"],
                now=now.astimezone(KST), vwap=payload["vwap"],
                vwap_verified=payload["vwap_source"] == "KIS_NATIVE_WEIGHTED_AVG",
                pivot=pivot, trigger_ok=trigger_ok, order_price=order_price,
            )
            return {**verdict, **diagnostic, "conflict_scope": "INDEPENDENT_5_PLUS_3"}
        return _result("UNKNOWN", "CONFLICTING_NATIVE_COMPLETED_BAR", **diagnostic)
    return evaluate_price_aware_entry(
        style=style, rows=payload["bars"], now=now,
        vwap=payload["vwap"],
        vwap_verified=payload["vwap_source"] == "KIS_NATIVE_WEIGHTED_AVG",
        pivot=pivot, trigger_ok=trigger_ok, setup_ok=setup_ok,
        order_price=order_price,
    )
