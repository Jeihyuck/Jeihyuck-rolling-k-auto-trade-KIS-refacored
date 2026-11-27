"""KR PB1 read-only opening geometry. Never an order approval or stop plan."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from functools import lru_cache
from math import isfinite
import os

from trader.kr.opening_bar_contract import KST, clean_completed_bars
from trader.kr.entry_opportunity_v2 import MIN_RR, MAX_RISK, MIN_DEPTH
from trader.kr_price_utils import normalize_kr_order_price


@dataclass(frozen=True)
class Bar:
    stamp: datetime
    high: float
    low: float
    close: float
    volume: float


@lru_cache(maxsize=512)
def _opening_features(bars: tuple[Bar, ...]) -> tuple:
    """Pure immutable content-keyed features; corrected bars invalidate the key."""
    peak = max(bars, key=lambda bar: bar.high)
    trough = min(bars, key=lambda bar: bar.low)
    after_peak = tuple(b for b in bars if b.stamp > peak.stamp)
    support = min(after_peak, key=lambda bar: bar.low) if after_peak else None
    return peak, trough, support


def analyze_opening(*, proof: dict, now: datetime, prep_style: str | None = None) -> dict:
    """Caller supplies a checksum-validated native proof from load_opening_proof."""
    now = now.astimezone(KST)
    result = {"is_order_authorization": False, "state": "INSUFFICIENT_DATA",
              "provisional_entry_ceiling": None, "opening_count": 0,
              "observed_at": now.isoformat(), "proof_sha": proof.get("sha256"),
              "prep_style": prep_style, "strategy_is_provisional": True}
    if proof.get("source") != "KIS_FHKST03010200" or proof.get("trade_date") != now.date().isoformat():
        return {**result, "reason": "UNVERIFIED_SCOPE"}
    if proof.get("source_conflicts"):
        return {**result, "state": "INVALIDATED", "reason": "NATIVE_CONFLICT"}
    try:
        received = datetime.fromisoformat(proof["as_of"])
        if received.tzinfo is None or not 0 <= (now-received).total_seconds() <= 180:
            return {**result, "state": "EXPIRED", "reason": "STALE_PROOF"}
    except (KeyError, ValueError, TypeError):
        return {**result, "reason": "INVALID_RECEIPT"}
    bars = tuple(Bar(datetime.fromisoformat(b["timestamp"]), b["high"], b["low"],
                     b["close"], b["volume"])
                 for b in clean_completed_bars(proof.get("bars", []), now))
    opening = tuple(b for b in bars if time(9, 0) <= b.stamp.time() < time(9, 30))
    if not opening or not bars:
        return {**result, "reason": "NO_COMPLETED_OPENING_BAR"}
    peak, low, opening_support = _opening_features(opening)
    expected = max(0, min(30, int((now-now.replace(hour=9, minute=0, second=0, microsecond=0)).total_seconds()//60)))
    result.update(opening_count=len(opening), opening_expected=expected,
                  opening_missing=max(0, expected-len(opening)),
                  opening_high=peak.high, opening_high_time=peak.stamp.isoformat(),
                  opening_low=low.low, opening_low_time=low.stamp.isoformat(),
                  opening_range_complete=(expected == 30 and len(opening) == 30),
                  latest_completed_bar=bars[-1].stamp.isoformat(),
                  latest_bar_age_sec=(now-bars[-1].stamp-timedelta(minutes=1)).total_seconds(),
                  observed_close=bars[-1].close,
                  vwap=proof.get("vwap") if proof.get("vwap_source") == "KIS_NATIVE_WEIGHTED_AVG" else None)
    style = str(prep_style or "").upper().removeprefix("ENTRY_")
    if not any(style == f or style.startswith(f + "_") for f in ("PULLBACK", "MOMENTUM")):
        return {**result, "state": "OBSERVING", "reason": "OWNER_SPECIFIC_GEOMETRY_REQUIRED"}
    # Require an actual later minute, not the unknowable high/low sequence in one bar.
    if opening_support is None:
        return {**result, "state": "OBSERVING", "reason": "NO_LATER_OPENING_SUPPORT"}
    tail = bars[-3:]
    if len(tail) < 3 or tail[0].stamp <= peak.stamp or any(
            (b.stamp-a.stamp).total_seconds() != 60 for a,b in zip(tail, tail[1:])):
        return {**result, "state": "OBSERVING", "reason": "NO_CONTIGUOUS_REBOUND_WINDOW"}
    if result["latest_bar_age_sec"] > 120:
        return {**result, "state": "EXPIRED", "reason": "STALE_COMPLETED_BAR"}
    lows = [b.low for b in tail]
    closes = [b.close for b in tail]
    support = min(lows)
    depth = (peak.high-opening_support.low)/peak.high
    result.update(opening_pullback_depth=depth, structural_support=support,
                  higher_lows=lows[0] <= lows[1] <= lows[2],
                  lower_lows=lows[0] > lows[1] > lows[2],
                  rebound=(closes[0] < closes[1] <= closes[2]))
    if result["lower_lows"] or support < opening_support.low:
        return {**result, "state": "INVALIDATED", "reason": "NEW_LOWER_SUPPORT"}
    if depth < MIN_DEPTH or not result["higher_lows"] or not result["rebound"]:
        return {**result, "state": "OBSERVING", "reason": "REBOUND_NOT_CONFIRMED"}
    raw = min((peak.high+MIN_RR*support)/(1+MIN_RR), support/(1-MAX_RISK))
    if not isfinite(raw) or raw <= support:
        return {**result, "state": "OBSERVING", "reason": "NO_FEASIBLE_RANGE"}
    cap, _ = normalize_kr_order_price(raw, side="SELL")
    if not support < cap < peak.high:
        return {**result, "state": "OBSERVING", "reason": "NO_FEASIBLE_RANGE"}
    # Geometry only, no executable ask, original setup, risk or strategy approval.
    return {**result, "state": "PROVISIONAL_RANGE", "reason": "OBSERVATION_GEOMETRY_ONLY",
            "provisional_entry_ceiling": cap, "target_reference": peak.high,
            "price_near_range": support < closes[-1] <= cap}


class ObservationPromotion:
    """Bounded, session-local HOT/WARM swaps for PREP observation only."""
    def __init__(self, min_hold_sec=180):
        self.min_hold_sec = min_hold_sec
        self.last_swap = None
        self.snapshot_key = None
        self.hot = []
        self.warm = []
        self.cold = []
        self.facts = {}

    def observe(self, code, facts):
        if isinstance(facts, dict):
            self.facts[code] = facts

    def apply(self, priority, now):
        if priority.get("state") != "PREP_COLLECTION_ACTIVE":
            self.snapshot_key = None
            self.facts.clear()
            return priority
        key = (priority.get("code_sha"), priority.get("final30_digest"),
               tuple(priority["hot"]), tuple(priority["warm"]), tuple(priority.get("cold", [])), now.date())
        if self.snapshot_key != key:
            self.snapshot_key = key
            self.hot, self.warm = list(priority["hot"]), list(priority["warm"])
            self.cold = list(priority.get("cold", []))
            self.last_swap = None
            # Even an overlapping symbol may have different owner geometry
            # after a PREP snapshot or code revision changes. Do not carry
            # prior observation facts across that identity boundary.
            self.facts.clear()
        def fresh_near(code):
            f = self.facts.get(code, {})
            try:
                age = (now-datetime.fromisoformat(f["observed_at"])).total_seconds()
            except (KeyError, TypeError, ValueError):
                return False
            return (0 <= age <= 180 and f.get("state") == "PROVISIONAL_RANGE"
                    and f.get("price_near_range") is True
                    and f.get("is_order_authorization") is False)
        if self.last_swap is None or (now-self.last_swap).total_seconds() >= self.min_hold_sec:
            incoming = next((c for c in self.warm if fresh_near(c)), None)
            outgoing = next((c for c in reversed(self.hot) if not fresh_near(c)), None)
            if incoming and outgoing:
                self.hot[self.hot.index(outgoing)] = incoming
                self.warm[self.warm.index(incoming)] = outgoing
                self.last_swap = now
            elif os.getenv("KR_COLD_DISCOVERY_ENABLED", "1") == "1":
                incoming = next((c for c in self.cold
                                 if c != "122630" and fresh_near(c)), None)
                outgoing = next((c for c in reversed(self.warm) if not fresh_near(c)), None)
                if incoming and (outgoing or len(self.warm) < 5):
                    self.cold.remove(incoming)
                    if outgoing:
                        self.warm[self.warm.index(outgoing)] = incoming
                        self.cold.append(outgoing)
                    else:
                        self.warm.append(incoming)
                    self.last_swap = now
        return {**priority, "hot": list(self.hot), "warm": list(self.warm),
                "cold": list(self.cold)}


class ColdDiscovery:
    """At most one reclaim-style PREP-only discovery every three minutes.

    Never takes an already-due HOT/WARM slot; the actual HTTP calls must
    still pass the existing observer and cross-process broker priority budgets.
    The result is observation evidence only, never an executable candidate.
    """
    def __init__(self):
        self.last_probe = None
        self.last_attempt = {}
        self.scope = None

    def select(self, priority, schedule, now):
        if (priority.get("state") != "PREP_COLLECTION_ACTIVE"
                or priority.get("is_order_authorization") is not False
                or not (priority.get("hot") or priority.get("warm"))):
            return None
        scope = (now.date(), priority.get("code_sha"), priority.get("final30_digest"))
        if self.scope != scope:
            self.scope = scope
            self.last_attempt.clear()
            # Keep the global rate bound even across snapshot replacements.
        if self.last_probe is not None and (now-self.last_probe).total_seconds() < 180:
            return None
        for tier in ("hot", "warm"):
            for code in priority.get(tier, []):
                last = schedule.last_attempt.get(code)
                if (last is None or (now-last).total_seconds() >
                        schedule.periods[tier] - 20):
                    return None
        styles = priority.get("observation_styles") or {}
        eligible = []
        for code in priority.get("cold", []):
            style = str(styles.get(code) or "").upper().removeprefix("ENTRY_")
            if (code == "122630" or not any(style == f or style.startswith(f+"_")
                                            for f in ("PULLBACK", "MOMENTUM"))):
                continue
            last = self.last_attempt.get(code)
            if last is None or (now-last).total_seconds() >= 1800:
                eligible.append(code)
        if not eligible:
            return None
        chosen = min(eligible, key=lambda c: (
            c in self.last_attempt, self.last_attempt.get(c, now), c))
        self.last_attempt[chosen] = self.last_probe = now
        return chosen
