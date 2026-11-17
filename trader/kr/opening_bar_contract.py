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
    if (len(opening) < 25
            or datetime.fromisoformat(opening[0]["timestamp"]).time() > dtime(9, 3)
            or datetime.fromisoformat(opening[-1]["timestamp"]).time() < dtime(9, 26)):
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


def inspect_completed_bar_coverage(*, rows: list[dict], now: datetime) -> dict:
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
    return {
        "state": state, "reason": reason,
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
        meta = {"opening_count": len(opening)}
        if opening:
            meta.update(opening_high=max(b["high"] for b in opening),
                        opening_low=min(b["low"] for b in opening))
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
    for raw in raw_bars:
        for one in clean_completed_bars([raw], now):
            key = one["timestamp"]
            if key in seen_this_pass and seen_this_pass[key] != one:
                intra_pass_conflicts.add(key)
            seen_this_pass[key] = one

    path = _evidence_file(env, account_scope, code, now)
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(".lock")
    with lock_path.open("a+b") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
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
                    count = int(prev.get("stable_native_reads") or 0) if identical else 0
                    first_at = str(prev.get("first_candidate_seen") or "") if identical else now.isoformat()
                    if separated and key not in intra_pass_conflicts:
                        count += 1
                        last_counted = now.isoformat()
                    else:
                        last_counted = str(prev.get("last_counted_at") or "") if identical else ""
                    observations[key] = {
                        "original": prev.get("original") or current or bar,
                        "candidate": bar, "first_candidate_seen": first_at,
                        "last_observed_at": now.isoformat(),
                        "last_counted_at": last_counted,
                        "stable_native_reads": count,
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


def evaluate_proof_for_candidate(*, env: str, account_scope: str, code: str,
                                 now: datetime, style: str, setup_ok: bool,
                                 trigger_ok: bool, pivot: float | None,
                                 order_price: float) -> dict:
    payload = load_opening_proof(
        env=env, account_scope=account_scope, code=code, now=now,
    )
    if not payload:
        return _result("UNKNOWN", "NO_FRESH_VERIFIED_KIS_MINUTE_BAR_PROOF")
    if payload.get("source_conflicts"):
        return _result("UNKNOWN", "CONFLICTING_NATIVE_COMPLETED_BAR",
                       source_conflicts=list(payload["source_conflicts"]))
    return evaluate_price_aware_entry(
        style=style, rows=payload["bars"], now=now,
        vwap=payload["vwap"],
        vwap_verified=payload["vwap_source"] == "KIS_NATIVE_WEIGHTED_AVG",
        pivot=pivot, trigger_ok=trigger_ok, setup_ok=setup_ok,
        order_price=order_price,
    )
