"""Independent read-only KIS KR completed-minute observer for standard PB1.

Usage (practice only, no broker orders):
python -m scripts.kr_opening_observer --once --final30-file PATH
python -m scripts.kr_opening_observer --loop --final30-file PATH

Must run as an independent WSL process; never called synchronously in the
PB1 BUY/SELL hot path. Not enabled by scheduler changes in this PR.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, time as dtime
import json
import logging
import os
from pathlib import Path
import time

from trader.account_state import get_account_key
from trader.kr.opening_bar_contract import (
    KST, save_opening_proof, load_opening_proof, inspect_completed_bar_coverage,
    diagnose_native_bar_rows, _source_time, clean_completed_bars,
)
from trader.runtime_paths import repo_root
from trader.time_utils import now_kst
from trader.kr.entry_priority_queue import CandidateSchedule, observer_collection_priorities
from trader.kr.opening_intelligence import analyze_opening, ObservationPromotion, ColdDiscovery

logger = logging.getLogger(__name__)


def final30_codes(*, trade_date: str, filename: str | None = None) -> list[str]:
    """Read the exact PREP canonical Final30 with its sibling dated contract.

    The canonical Final30 payload may be an array, or a dict with items/rows.
    The source file is not itself the authority for the trade date/market/env:
    `prep_contract.json` is. Do not infer scope from the file's directory.
    """
    # PREP writes the canonical mirrored file under the REPOSITORY runtime
    # directory. The generic ephemeral runtime_path() defaults to
    # /tmp/trader_runtime, which has no Final30 in actual WSL AM sessions.
    # Do not silently accept a different artifact root/date.
    file = (
        Path(filename) if filename else
        repo_root() / "runtime" / "kr" / "watchlist" / trade_date / "final30_scored.json"
    )
    # PREP's production canonical artifact is sometimes mirrored under an
    # extra trade-date directory. The 2026-11-10 AM archive proves:
    #   runtime/kr/watchlist/YYYY-MM-DD/final30_scored.json
    #   runtime/kr/watchlist/YYYY-MM-DD/YYYY-MM-DD/prep_contract.json
    # Search only these two same-day canonical locations, never stale latest
    # aliases. If both exist, reject conflicting date/as_of/contract hash.
    contract_paths = (
        file.with_name("prep_contract.json"),
        file.parent / trade_date / "prep_contract.json",
    )
    existing = [p for p in contract_paths if p.is_file()]
    if not existing:
        raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_SCOPE_MISMATCH")
    try:
        payload = json.loads(file.read_text(encoding="utf-8"))
        contracts = [json.loads(p.read_text(encoding="utf-8")) for p in existing]
        contract = contracts[0]
        for other in contracts[1:]:
            for field in ("trade_date", "market", "env", "contract_hash"):
                if (contract.get(field) is not None and other.get(field) is not None
                        and str(contract[field]) != str(other[field])):
                    raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_CONFLICTING_CONTRACTS")
            left_asof = str(contract.get("actual_as_of") or contract.get("as_of") or contract.get("expected_as_of") or "")[:10]
            right_asof = str(other.get("actual_as_of") or other.get("as_of") or other.get("expected_as_of") or "")[:10]
            if not left_asof or left_asof != right_asof:
                raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_CONFLICTING_CONTRACTS")
    except (ValueError, OSError, TypeError) as exc:
        raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_SCOPE_MISMATCH") from exc
    if (
        not isinstance(contract, dict)
        or str(contract.get("trade_date") or "")[:10] != trade_date
        or str(contract.get("market") or "").upper() != "KR"
        or str(contract.get("env") or "").lower() != "practice"
        or not bool(contract.get("contract_ok"))
        or not bool(contract.get("trade_can_proceed"))
        or int(contract.get("final30_rows") or 0) != 30
    ):
        raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_SCOPE_MISMATCH")
    contract_asof = str(
        contract.get("actual_as_of") or contract.get("as_of")
        or contract.get("expected_as_of") or ""
    )[:10]
    if not contract_asof:
        raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_SCOPE_MISMATCH")
    # PREP contract can have 'actual_as_of' instead of 'as_of'; do not reject
    # valid production artifacts or silently allow a mismatched scoring date.
    expected_asof = str(contract.get("expected_as_of") or "")[:10]
    if expected_asof and expected_asof != contract_asof:
        raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_SCOPE_MISMATCH")
    if isinstance(payload, dict):
        for field, expected in (
            ("trade_date", trade_date),
            ("market", "KR"),
            ("env", "practice"),
            ("as_of", contract_asof),
            ("expected_as_of", contract_asof),
        ):
            if payload.get(field) is not None and str(payload[field])[:10] != expected:
                raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_SCOPE_MISMATCH")
        rows = (payload.get("rows") or payload.get("items")
                or payload.get("data") or payload.get("final30") or [])
    elif isinstance(payload, list):
        rows = payload
    else:
        raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_BAD_UNIVERSE")
    if not isinstance(rows, list) or len(rows) != 30:
        raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_BAD_UNIVERSE")
    codes = []
    for row in rows:
        if not isinstance(row, dict):
            raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_BAD_UNIVERSE")
        code = str(row.get("code") or "").strip()
        if not code.isdigit() or len(code) > 6 or not code:
            raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_BAD_UNIVERSE")
        codes.append(code.zfill(6))
    if len(set(codes)) != 30:
        raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_BAD_UNIVERSE")
    return sorted(codes)

def verified_native_kr_vwap(quote: dict, *, code: str, observed_at: datetime) -> tuple[float | None, str]:
    """A source-bound native KIS VWAP, never a guessed intraday average.

    Provenance, symbol, native session date, exchange clock and receipt
    freshness must all agree. A price-only WS quote cannot supply VWAP.
    """
    if not isinstance(quote, dict):
        return None, "QUOTE_NOT_MAPPING"
    raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else {}
    source = str(quote.get("source") or raw.get("source") or "").upper()
    norm = str(code).zfill(6)
    if source == "KIS_REST_INQUIRE_PRICE":
        if (quote.get("source_identity") != "AUTHENTICATED_REST_REQUEST_BOUND"
                or str(quote.get("requested_symbol") or "").lstrip("A").zfill(6) != norm):
            return None, "REST_REQUEST_IDENTITY_UNVERIFIED"
    elif source == "KIS_WEBSOCKET":
        if str(quote.get("symbol") or "").lstrip("A").zfill(6) != norm:
            return None, "WS_SUBSCRIPTION_IDENTITY_UNVERIFIED"
    else:
        return None, "QUOTE_SOURCE_UNVERIFIED"
    def field(key: str):
        # An explicit invalid top-level field is never repaired by a cache.
        return quote[key] if key in quote else raw.get(key)
    day = str(field("stck_bsop_date") or "").replace("-", "")
    clock_value = field("stck_cntg_hour")
    if not day or day != observed_at.astimezone(KST).strftime("%Y%m%d"):
        return None, "NATIVE_QUOTE_DATE_UNVERIFIED"
    if clock_value is None or not str(clock_value).strip():
        return None, "NATIVE_QUOTE_CLOCK_MISSING"
    clock = str(clock_value).strip()
    if not clock.isdigit() or len(clock) > 6:
        return None, "NATIVE_QUOTE_CLOCK_INVALID"
    clock = clock.zfill(6)
    if clock == "000000":
        return None, "NATIVE_QUOTE_CLOCK_INVALID"
    try:
        native_at = datetime.strptime(day + clock, "%Y%m%d%H%M%S").replace(tzinfo=KST)
    except ValueError:
        return None, "NATIVE_QUOTE_CLOCK_INVALID"
    age = (observed_at.astimezone(KST) - native_at).total_seconds()
    if not -10 <= age <= 120:
        return None, "NATIVE_QUOTE_STALE_OR_FUTURE"
    received = quote.get("received_at")
    if received is None:
        return None, "QUOTE_RECEIPT_NOT_ATTESTED"
    try:
        if isinstance(received, (int, float)):
            received_at = datetime.fromtimestamp(float(received), tz=KST)
        elif isinstance(received, datetime):
            received_at = received
        else:
            received_at = datetime.fromisoformat(str(received).replace("Z", "+00:00"))
        if received_at.tzinfo is None:
            return None, "QUOTE_RECEIPT_NOT_ATTESTED"
        age = (observed_at.astimezone(KST) - received_at.astimezone(KST)).total_seconds()
        if not -10 <= age <= 120:
            return None, "QUOTE_RECEIPT_STALE_OR_FUTURE"
    except (TypeError, ValueError, OverflowError, OSError):
        return None, "QUOTE_RECEIPT_NOT_ATTESTED"
    try:
        value = float(field("wghn_avrg_stck_prc"))
        if not (value > 0 and value < float("inf")):
            raise ValueError("nonpositive-or-infinite")
    except (TypeError, ValueError, OverflowError):
        return None, "NATIVE_VWAP_MISSING_OR_INVALID"
    return value, "VERIFIED_NATIVE_KIS_VWAP"


# Per-process bounded backoff. Historical candles may need several sessions
# of REST recovery; never block PB1's time-critical SELL route.
_OPENING_REPAIR_NEXT: dict[str, tuple[float, int]] = {}


def _repair_opening_due(code: str) -> bool:
    now = time.monotonic()
    next_at, attempt = _OPENING_REPAIR_NEXT.get(code, (0.0, 0))
    if now < next_at:
        return False
    attempt = min(attempt + 1, 7)
    _OPENING_REPAIR_NEXT[code] = (now + min(900.0, 20.0 * (2 ** attempt)), attempt)
    return True


# Conflict revalidation has its own bounded cadence: three independent reads
# spaced >=60s must not inherit the opening-range 15-minute backoff.
_CONFLICT_REPAIR_NEXT: dict[str, float] = {}
_CONFLICT_REPAIR_CURSOR: dict[str, int] = {}


def _repair_conflict_due(code: str) -> bool:
    now = time.monotonic()
    if now < _CONFLICT_REPAIR_NEXT.get(code, 0.0):
        return False
    _CONFLICT_REPAIR_NEXT[code] = now + 60.0
    return True


def _conflict_repair_anchors(code: str, unresolved: list, observed_at: datetime) -> list[str]:
    """Rotate a bounded pair of exact historical anchors; never starve later conflicts."""
    valid = []
    for minute in unresolved:
        try:
            stamp = datetime.fromisoformat(str(minute))
            if stamp.tzinfo is None:
                continue
            stamp = stamp.astimezone(KST)
        except (ValueError, TypeError, OverflowError):
            continue
        if (stamp.date() != observed_at.date()
                or not dtime(9, 0) <= stamp.time() < dtime(15, 30)
                or stamp >= observed_at.replace(second=0, microsecond=0)):
            continue
        valid.append((stamp + timedelta(minutes=1)).strftime("%H%M%S"))
    valid = sorted(set(valid))
    if not valid:
        return []
    start = _CONFLICT_REPAIR_CURSOR.get(code, 0) % len(valid)
    count = min(2, len(valid))
    _CONFLICT_REPAIR_CURSOR[code] = (start + count) % len(valid)
    return [valid[(start + offset) % len(valid)] for offset in range(count)]


_RECENT_REPAIR_NEXT: dict[str, float] = {}


def _recent_gap_anchor(coverage: dict, bars: list[dict], observed_at: datetime) -> str | None:
    """Target a genuinely unobserved minute in the latest required native window.

    This is a request anchor, not a claim that a missing minute had trades.
    Never construct a candle or infer a business date from an empty response.
    """
    if coverage.get("reason") == "RECENT_MINUTE_GAP":
        window_size = 4 if coverage.get("required_recent_minutes") == 4 else 3
    elif (not coverage.get("opening_range_ready")
          and coverage.get("native_post_open_window_reason") == "POST_OPENING_NATIVE_WINDOW_GAP"):
        window_size = 8
    else:
        return None
    stamps = []
    for bar in bars:
        try:
            stamp = datetime.fromisoformat(bar["timestamp"])
            if stamp.tzinfo is not None:
                stamp = stamp.astimezone(KST)
                if stamp.date() == observed_at.date() and dtime(9, 30) <= stamp.time() < dtime(15, 30):
                    stamps.append(stamp)
        except (KeyError, ValueError, TypeError):
            continue
    latest = sorted(set(stamps))[-window_size:]
    if len(latest) < (window_size if window_size in (3, 4) else 2):
        return None
    expected = [latest[-1] - timedelta(minutes=i)
                for i in range(window_size - 1, -1, -1)]
    missing = [stamp for stamp in expected if stamp not in latest and stamp.time() >= dtime(9, 30)]
    if not missing:
        return None
    return (max(missing) + timedelta(minutes=1)).strftime("%H%M%S")


def _recent_repair_due(code: str) -> bool:
    now = time.monotonic()
    if now < _RECENT_REPAIR_NEXT.get(code, 0.0):
        return False
    _RECENT_REPAIR_NEXT[code] = now + 60.0
    return True


def _request_completed_rows(rows, requested_at):
    """An in-progress candle cannot become completed while other I/O runs.

    Retain malformed rows for the existing diagnostics, but exclude a
    same-session current/future minute based on the request's original clock.
    Preserve the raw HTTP observation ID and all native values.
    """
    requested_at = requested_at.astimezone(KST)
    minute = requested_at.replace(second=0, microsecond=0)
    accepted = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        stamp = _source_time(row, requested_at.date().isoformat())
        if stamp is not None and stamp >= minute:
            continue
        accepted.append(row)
    return accepted


def _stale_minute_anchor(coverage, observed_at):
    """Explicit completed-minute close boundary after a stale observation."""
    if (coverage.get("reason") != "COMPLETED_BAR_STALE"
            and coverage.get("native_post_open_window_reason") != "COMPLETED_BAR_STALE"):
        return None
    return observed_at.astimezone(KST).replace(second=0, microsecond=0).strftime("%H%M%S")


def _current_batch_coverage(stored, fetched, observed_at):
    """Skip redundant repair only if the latest native response supplies it.

    A contradictory OHLCV row is never treated as coverage improvement:
    persistence still performs the original three-observation quarantine.
    """
    merged = {}
    for row in list((stored or {}).get("bars") or []) + list(fetched):
        for bar in clean_completed_bars([row], observed_at):
            key = bar["timestamp"]
            if key in merged and merged[key] != bar:
                return {}
            merged[key] = bar
    return inspect_completed_bar_coverage(
        rows=[{**bar, "date": observed_at.date().isoformat(),
               "time": datetime.fromisoformat(bar["timestamp"]).strftime("%H%M%S")}
              for bar in merged.values()], now=observed_at,
        source_conflicts=(stored or {}).get("source_conflicts"),
    )


def run_once(*, kis, codes: list[str], observed_at: datetime,
             sleep_between: float = 0.8, refresh_clock=None,
             observation_styles: dict[str, str] | None = None,
             discovery_only: bool = False) -> dict:
    """No order methods are called; all I/O is read-only KIS and local proofs."""
    observed_at = observed_at.astimezone(KST)
    if not dtime(9, 0) <= observed_at.time() <= dtime(15, 5):
        return {"state": "OUTSIDE_SESSION", "completed": 0, "unknown": len(codes)}
    scope = get_account_key(env="practice")
    done, unknown, coverage_ok, missing_vwap = 0, 0, 0, 0
    minute_requests, quote_requests, native_request_failures = 0, 0, 0
    symbol_scan_sec: list[float] = []
    symbol_outcomes: dict[str, dict] = {}
    session_date = observed_at.date()
    for code in codes:
        # Production batches can span multiple completed minutes. Do not
        # query every subsequent symbol at the previous batch's clock.
        if refresh_clock is not None:
            observed_at = refresh_clock().astimezone(KST)
            if observed_at.date() != session_date or not dtime(9, 0) <= observed_at.time() <= dtime(15, 5):
                break
        symbol_started = time.monotonic()
        if str(code).zfill(6) == "122630":
            # Separate owner: never collect KR_INFINITE by PB1 proof route.
            continue
        try:
            # KIS time-itemchart returns bounded minute windows. Read both
            # the opening anchor and the latest anchor; a fixed 09:00 query
            # alone cannot prove a post-09:30 rebound. Never fill gaps with
            # synthetic open/high/low/close or an inferred business date.
            # Opening proof is durable.  Do not re-download the entire
            # 09:00-09:29 interval on every minute-data refresh.  Only the
            # genuine latest completed minute is needed in steady state.
            stored = load_opening_proof(
                env="practice", account_scope=scope, code=code, now=observed_at,
                max_age_seconds=None,
            )
            opening_coverage = (
                inspect_completed_bar_coverage(rows=stored["bars"], now=observed_at,
                                               source_conflicts=stored.get("source_conflicts"))
                if stored else {"opening_count": 0, "opening_valid_minutes": []}
            )
            opening_ready = bool(opening_coverage.get("opening_range_ready"))
            unresolved = list((stored or {}).get("source_conflicts") or [])
            repair_due = (
                observed_at.time() >= dtime(9, 30)
                and not (opening_ready or opening_coverage.get(
                    "native_post_open_window_ready", False))
                and _repair_opening_due(code)
            )
            conflict_due = (
                not discovery_only and observed_at.time() >= dtime(9, 30)
                and bool(unresolved) and _repair_conflict_due(code)
            )
            # Prioritize current native data before optional historical repair.
            anchors = [observed_at.strftime("%H%M%S")]
            recent_anchor = _recent_gap_anchor(
                opening_coverage, (stored or {}).get("bars") or [], observed_at,
            )
            recent_anchor = recent_anchor or _stale_minute_anchor(opening_coverage, observed_at)
            if not discovery_only and recent_anchor and recent_anchor not in anchors and _recent_repair_due(code):
                anchors.append(recent_anchor)
                logger.info("[KR][OPENING_OBSERVER][RECENT_GAP_REPAIR] code=%s anchor=%s", code, recent_anchor)
            if repair_due:
                anchors.append("092900")
            # An unresolved native-minute conflict can be >30 bars old.
            # Current-window reads no longer contain it: target the exact
            # conflict timestamp in an authenticated KIS historical window.
            # The original value and conflict quarantine remain intact until
            # the normal multi-read proof resolves it. Maximum two targeted
            # reads per repair cycle to preserve the observer-only KIS budget.
            conflict_anchors = _conflict_repair_anchors(code, unresolved, observed_at) if conflict_due else []
            anchors.extend(conflict_anchors)
            anchors = list(dict.fromkeys(anchors))
            if repair_due or conflict_due:
                logger.info(
                    "[KR][OPENING_OBSERVER][REPAIR_REQUEST] code=%s "
                    "opening_count=%s anchors=%s bounded_backoff=1",
                    code, opening_coverage.get("opening_count"), anchors,
                )
            data: list[dict] = []
            for index, anchor in enumerate(anchors):
                try:
                    request_at = (refresh_clock().astimezone(KST) if refresh_clock else observed_at)
                    if request_at.date() != session_date or request_at.time() > dtime(15, 5):
                        break
                    if index and data and anchor not in conflict_anchors:
                        # Most KIS latest-window responses contain multiple
                        # completed minutes. Reassess before issuing an old
                        # repair request; don't query a hole that is now filled.
                        current = _current_batch_coverage(stored, data, request_at)
                        if (current.get("state") == "COVERED"
                                or current.get("native_post_open_window_ready")):
                            logger.info(
                                "[KR][OPENING_OBSERVER][REPAIR_SUPERSEDED] code=%s anchor=%s action=SKIP_REDUNDANT_NATIVE_READ",
                                code, anchor,
                            )
                            continue
                    minute_requests += 1
                    try:
                        rows = kis.get_intraday_candles_today(
                            code, start_hhmm=anchor, attempts=1,
                        )
                    except TypeError as exc:
                        # Compatible with existing strictly read-only test KIS
                        # adapters that predate the optional attempts argument.
                        if "unexpected keyword argument 'attempts'" not in str(exc):
                            raise
                        rows = kis.get_intraday_candles_today(
                            code, start_hhmm=anchor,
                        )
                    if isinstance(rows, list):
                        accepted = _request_completed_rows(rows, request_at)
                        logger.info("[KR][OPENING_OBSERVER][REQUEST_BOUNDARY] code=%s anchor=%s request_at=%s accepted=%s excluded=%s",
                                    code, anchor, request_at.isoformat(), len(accepted), len(rows)-len(accepted))
                        data.extend(accepted)
                except Exception as minute_exc:
                    native_request_failures += 1
                    logger.warning(
                        "[KR][OPENING_OBSERVER][PARTIAL_MINUTE_FETCH] code=%s "
                        "anchor=%s error_type=%s",
                        code, anchor, type(minute_exc).__name__,
                    )
            if not data:
                raise RuntimeError("NO_KIS_NATIVE_INTRADAY_MINUTE_ROWS")
            # A failed quote must not throw away already-authenticated native
            # minute rows. Persist bars with VWAP=UNKNOWN; styles requiring
            # a verified VWAP remain fail-closed at their original gate.
            try:
                quote_requests += 1
                native_observer_quote = getattr(kis, "get_kr_observer_quote", None)
                quote = (native_observer_quote(code) if callable(native_observer_quote)
                         else kis.get_price_snapshot(code, market="J"))
                if not isinstance(quote, dict):
                    quote = {}
            except Exception as quote_exc:
                native_request_failures += 1
                quote = {}
                logger.warning(
                    "[KR][OPENING_OBSERVER][QUOTE_FAIL_SOFT] code=%s type=%s "
                    "action=PERSIST_NATIVE_MINUTES_WITH_UNVERIFIED_VWAP",
                    code, type(quote_exc).__name__,
                )
            raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else {}
            quote_origin = str(quote.get("source") or raw.get("source") or "UNKNOWN")
            proof_now = now_kst()
            native_diagnostic = diagnose_native_bar_rows(data, proof_now)
            logger.info(
                "[KR][OPENING_OBSERVER][KIS_NATIVE_BAR_AUDIT] code=%s "
                "rows=%d valid_unique=%d duplicates=%d dropped=%s "
                "first_valid=%s last_valid=%s no_trade_proven=0",
                code, native_diagnostic["total_rows"],
                native_diagnostic["valid_unique_minutes"],
                native_diagnostic["duplicate_valid_rows"],
                native_diagnostic["excluded_by_reason"],
                native_diagnostic["first_valid_native"],
                native_diagnostic["last_valid_native"],
            )
            vwap, vwap_status = verified_native_kr_vwap(
                quote, code=code, observed_at=proof_now,
            )
            verified_vwap = vwap_status == "VERIFIED_NATIVE_KIS_VWAP"
            quote_day = str(quote["stck_bsop_date"] if "stck_bsop_date" in quote
                            else raw.get("stck_bsop_date") or "").replace("-", "")
            quote_clock = quote["stck_cntg_hour"] if "stck_cntg_hour" in quote else raw.get("stck_cntg_hour")
            result = save_opening_proof(
                env="practice", account_scope=scope, code=code,
                now=proof_now, raw_bars=data,
                vwap=vwap if verified_vwap else None,
                vwap_source="KIS_NATIVE_WEIGHTED_AVG" if verified_vwap else "UNKNOWN",
            )
            # Report and evaluate the persisted *merged* evidence, not merely
            # this REST response: a partial fetch can complement prior candles.
            persisted = load_opening_proof(
                env="practice", account_scope=scope, code=code, now=proof_now,
            )
            if persisted is None:
                raise RuntimeError("OPENING_PROOF_PERSISTED_READBACK_FAILED")
            coverage = inspect_completed_bar_coverage(
                rows=persisted["bars"], now=proof_now,
                source_conflicts=persisted.get("source_conflicts"),
            )
            # The opening anchor can cover up to the early session; only
            # request the 09:15 repair anchor if that first replay still left
            # unobserved original minutes. A full proof needs no extra read.
            if (coverage["reason"] == "OPENING_BARS_MISSING_OR_INCOMPLETE"
                    and not coverage.get("native_post_open_window_ready")
                    and repair_due and not discovery_only):
                try:
                    repair_at = (refresh_clock().astimezone(KST) if refresh_clock else observed_at)
                    minute_requests += 1
                    try:
                        repair_rows = kis.get_intraday_candles_today(
                            code, start_hhmm="091500", attempts=1,
                        )
                    except TypeError as exc:
                        if "unexpected keyword argument 'attempts'" not in str(exc):
                            raise
                        repair_rows = kis.get_intraday_candles_today(
                            code, start_hhmm="091500",
                        )
                    if isinstance(repair_rows, list) and repair_rows:
                        result = save_opening_proof(
                            env="practice", account_scope=scope, code=code,
                            now=proof_now,
                            raw_bars=_request_completed_rows(repair_rows, repair_at),
                            vwap=vwap if verified_vwap else None,
                            vwap_source=("KIS_NATIVE_WEIGHTED_AVG"
                                         if verified_vwap else "UNKNOWN"),
                        )
                        persisted = load_opening_proof(
                            env="practice", account_scope=scope, code=code,
                            now=proof_now,
                        )
                        if persisted is None:
                            raise RuntimeError("OPENING_REPAIR_READBACK_FAILED")
                        coverage = inspect_completed_bar_coverage(
                            rows=persisted["bars"], now=proof_now,
                source_conflicts=persisted.get("source_conflicts"),
                        )
                    logger.info(
                        "[KR][OPENING_OBSERVER][REPAIR] code=%s anchor=091500 "
                        "native_rows=%s opening_count=%s reason=%s",
                        code, len(repair_rows) if isinstance(repair_rows, list) else 0,
                        coverage["opening_count"], coverage["reason"],
                    )
                except Exception as repair_exc:
                    native_request_failures += 1
                    logger.warning(
                        "[KR][OPENING_OBSERVER][REPAIR_FAIL] code=%s type=%s "
                        "action=KEEP_OLD_VERIFIED_PROOF",
                        code, type(repair_exc).__name__,
                    )
            if result.get("resolved_conflicts"):
                logger.warning(
                    "[KR][OPENING_OBSERVER][CONFLICT_RESOLVED] code=%s minutes=%s "
                    "source=THREE_SPACED_KIS_NATIVE_CONFIRMATIONS audit_persisted=1",
                    code, result["resolved_conflicts"],
                )
            if persisted.get("source_conflicts"):
                logger.warning(
                    "[KR][OPENING_OBSERVER][CONFLICT_QUARANTINE] code=%s minutes=%s "
                    "unverified_buy_blocked=1",
                    code, persisted.get("source_conflicts"),
                )
                if not coverage.get("available_native_paths"):
                    coverage = {**coverage, "state": "UNKNOWN",
                                "reason": "CONFLICTING_NATIVE_COMPLETED_BAR"}
            if (coverage["state"] != "COVERED"
                    and coverage.get("native_post_open_window_ready")):
                # Structural availability only; owner-specific breakout still
                # requires the opening reference in the final timing evaluator.
                coverage = {**coverage, "state": "COVERED",
                            "reason": "INDEPENDENT_NATIVE_5_PLUS_3_AVAILABLE"}
            if coverage["state"] == "COVERED":
                coverage_ok += 1
            elif observed_at.time() >= dtime(9, 30):
                logger.warning(
                    "[KR][OPENING_OBSERVER][COVERAGE_GAP] code=%s "
                    "reason=%s native_rows=%s persisted_bars=%s "
                    "opening_valid=%s opening_unobserved=%s "
                    "unobserved_not_proven_missing=1",
                    code, coverage["reason"], len(data), result["bars"],
                    coverage.get("opening_valid_minutes"),
                    coverage.get("opening_unobserved_minutes"),
                )
            if not verified_vwap:
                missing_vwap += 1
            intelligence = {}
            if os.getenv("KR_OPENING_INTELLIGENCE_ENABLED", "1") == "1":
                intelligence_started = time.monotonic()
                try:
                    intelligence = analyze_opening(
                        proof=persisted, now=proof_now,
                        prep_style=(observation_styles or {}).get(code))
                    logger.info("[KR][OPENING_OBSERVER][INTELLIGENCE] code=%s elapsed_ms=%.3f facts=%s",
                                code, (time.monotonic()-intelligence_started)*1000,
                                json.dumps(intelligence, ensure_ascii=False, sort_keys=True))
                except Exception:
                    logger.exception("[KR][OPENING_OBSERVER][INTELLIGENCE_FAIL] code=%s action=KEEP_EXISTING_PROOF", code)
            symbol_outcomes[code] = {
                "opening_intelligence": intelligence,
                "state": coverage["state"],
                "reason": coverage["reason"],
                "opening_count": coverage["opening_count"],
                "post_open_count": coverage["post_open_count"],
                "latest_bar_age_sec": coverage["latest_bar_age_sec"],
                "vwap_verified": verified_vwap,
                "conflicts": len(persisted.get("source_conflicts") or []),
            }
            logger.info(
                "[KR][OPENING_OBSERVER][COVERAGE] code=%s state=%s reason=%s "
                "opening_count=%s post_open_count=%s bar_age_sec=%s "
                "vwap_verified=%s vwap_reason=%s vwap_origin=%s day_present=%s "
                "exchange_clock_present=%s persisted_bars=%s new_bars=%s "
                "reused_bars=%s conflicts=%s read_only=1",
                code, coverage["state"], coverage["reason"],
                coverage["opening_count"], coverage["post_open_count"],
                coverage["latest_bar_age_sec"], int(verified_vwap),
                vwap_status, quote_origin, int(bool(quote_day)),
                int(bool(quote_clock)), result["bars"], result["new_bars"],
                result["reused_bars"], len(result["source_conflicts"]),
            )
            if result["bars"] > 0:
                done += 1
                logger.info(
                    "[KR][OPENING_OBSERVER] code=%s bars=%s verified_source=KIS "
                    "role=READ_ONLY no_broker_orders=1",
                    code, result["bars"],
                )
            else:
                unknown += 1
        except Exception as exc:
            unknown += 1
            symbol_outcomes[code] = {
                "state": "UNKNOWN", "reason": type(exc).__name__,
                "opening_count": None, "post_open_count": None,
                "latest_bar_age_sec": None, "vwap_verified": False,
                "conflicts": None,
            }
            logger.warning(
                "[KR][OPENING_OBSERVER][UNKNOWN] code=%s error_type=%s "
                "action=NO_TIMING_PROOF",
                code, type(exc).__name__,
            )
        finally:
            elapsed_for_symbol = time.monotonic() - symbol_started
            symbol_scan_sec.append(elapsed_for_symbol)
            if code in symbol_outcomes:
                symbol_outcomes[code]["poll_elapsed_sec"] = elapsed_for_symbol
            logger.info(
                "[KR][OPENING_OBSERVER][SYMBOL_LATENCY] code=%s elapsed_sec=%.3f",
                code, elapsed_for_symbol,
            )
        # This independent collector is rate-limited and does not delay SELL.
        if sleep_between > 0:
            time.sleep(max(sleep_between, 0.0))
    ordered_latencies = sorted(symbol_scan_sec)
    p95 = (ordered_latencies[max(0, int((len(ordered_latencies) * 0.95 + 0.999999)) - 1)]
           if ordered_latencies else None)
    return {
        "state": "COMPLETE", "completed": done, "unknown": unknown,
        "coverage_ok": coverage_ok, "vwap_unverified": missing_vwap,
        "symbol_scan_p95_sec": p95,
        "symbol_scan_max_sec": ordered_latencies[-1] if ordered_latencies else None,
        "native_minute_request_attempts": minute_requests,
        "native_quote_request_attempts": quote_requests,
        "native_request_failures": native_request_failures,
        "native_quote_requests_added": quote_requests,
        "symbol_outcomes": symbol_outcomes,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--final30-file")
    parser.add_argument("--date")
    parser.add_argument("--interval-sec", type=float, default=45.0)
    args = parser.parse_args()
    if args.once == args.loop:
        parser.error("select exactly one of --once or --loop")
    logging.basicConfig(level=logging.INFO)
    from trader.kis_wrapper import KisAPI
    today = args.date or now_kst().date().isoformat()
    if args.once:
        # --once is the strict diagnostic: missing/invalid Final30 is an error.
        codes = final30_codes(trade_date=today, filename=args.final30_file)
        kis = KisAPI(kis_env="practice")
        print(json.dumps(run_once(kis=kis, codes=codes, observed_at=now_kst()), ensure_ascii=False))
        return
    # The live read-only loop must survive delayed PREP/Final30 creation.
    # Do not pre-load Final30 or initialise the KIS client until the same-day
    # PREP contract is actually verified.  No stale prior-day fallback.
    codes: list[str] = []
    kis = None
    # Wake frequently to pick up newly HOT symbols and the next due
    # subset. Per-symbol periods still bound REST demand; no all-Final30
    # scan, and an explicit slower interval continues to slow polling.
    configured_period = max(15.0, float(args.interval_sec))
    planner_wake_sec = min(15.0, configured_period)
    schedule = CandidateSchedule(max_batch=6, hot_sec=max(45, int(configured_period)),
                                 warm_sec=max(180, int(configured_period)))
    promotion = ObservationPromotion()
    cold_discovery = ColdDiscovery()
    last_capacity_warning_key = None
    last_idle_key = None
    last_idle_logged_at = 0.0
    scope = get_account_key(env="practice")
    logger.info(
        "[KR][OPENING_OBSERVER][COLLECTOR_BOOT] date=%s module=%s interval_sec=%s planner_wake_sec=%.1f hot_period_sec=%s warm_period_sec=%s "
        "collection_scope=VERIFIED_HOT_WARM_WITH_BOUNDED_PREP_COLD",
        today, __file__, args.interval_sec, planner_wake_sec, schedule.periods["hot"], schedule.periods["warm"],
    )
    while True:
        now = now_kst()
        if now.date().isoformat() != today or now.time() > dtime(15, 5):
            break
        if now.time() < dtime(9, 0):
            time.sleep(2.0)
            continue
        cycle_started = time.monotonic()
        try:
            priority = observer_collection_priorities(date=today, account_scope=scope, now=now,
                                             final30_file=args.final30_file)
            if priority["state"] == "INVALID_PREP":
                logger.warning("[KR][OPENING_OBSERVER][WAIT_PREP] verified_final30=0 action=RETRY_SAME_DAY_READ_ONLY")
                time.sleep(10.0)
                continue
            # A PREP file can be written after the AM wrapper starts or be
            # replaced atomically during recovery. Validate before each first
            # use; all trading authorisation stays inside PB1 ENFORCE.
            if not codes or kis is None:
                candidate_codes = final30_codes(trade_date=today, filename=args.final30_file)
                candidate_kis = KisAPI(kis_env="practice")
                codes, kis = candidate_codes, candidate_kis
                logger.info("[KR][OPENING_OBSERVER][PREP_READY] final30=%s role=READ_ONLY", len(codes))
            # COLD remains in the signed Final30 universe and continues to
            # receive low-cost PB1 pre-minute evaluation. Native collection
            # remains HOT/WARM first; bounded PREP-only discovery uses spare
            # capacity with the same low-priority broker admission controls.
            if os.getenv("KR_DYNAMIC_COLLECTION_PRIORITY_ENABLED", "1") == "1":
                priority = promotion.apply(priority, now)
            native_priority = {**priority, "cold": []}
            health = schedule.diagnostics(
                native_priority, now, interval_sec=planner_wake_sec,
            )
            warning_key = (
                priority["state"], tuple(health["tiers"][tier]["count"]
                                         for tier in ("hot", "warm", "cold")),
                round(configured_period, 3),
            )
            if health["overcommitted"] and warning_key != last_capacity_warning_key:
                logger.warning(
                    "[KR][OPENING_OBSERVER][THEORETICAL_CAPACITY_SHORTFALL] "
                    "hot=%s warm=%s cold=%s required_poll_rps=%.4f "
                    "slot_capacity_rps=%.4f all_180s_possible=%s "
                    "assumption=ZERO_HTTP_LATENCY action=KEEP_BUY_ENFORCE_FAIL_CLOSED",
                    *warning_key[1], health["configured_demand_per_sec"],
                    health["theoretical_polls_per_sec"],
                    int(health["all_180s_possible_at_slot_capacity"]),
                )
            last_capacity_warning_key = warning_key
            selected = schedule.select(native_priority, now)
            discovery_only = False
            if (not selected and os.getenv("KR_DYNAMIC_COLLECTION_PRIORITY_ENABLED", "1") == "1"
                    and os.getenv("KR_COLD_DISCOVERY_ENABLED", "1") == "1"
                    and os.getenv("KR_PB1_KIS_OBSERVER_BUDGET_ENABLED") == "1"
                    and os.getenv("KR_SHARED_HTTP_BUDGET_ENABLED") == "1"):
                cold = cold_discovery.select(priority, schedule, now)
                if cold:
                    selected = [cold]
                    discovery_only = True
                    logger.info("[KR][OPENING_OBSERVER][COLD_DISCOVERY] code=%s spare_slot=1 order_authorization=0", cold)
            if not selected:
                if priority["state"] in {"PB1_ACTIVE", "PREP_COLLECTION_ACTIVE"} and (priority["hot"] or priority["warm"]):
                    # A nonempty verified shortlist with nothing due is not
                    # a collection-target outage. Keep per-symbol due periods.
                    time.sleep(min(5.0, planner_wake_sec))
                    continue
                # A missing/mismatched priority manifest means zero broker
                # 1m requests. Distinguish this P0 fault from a legitimate
                # owner-selected empty shortlist, without adding COLD I/O.
                idle_key = (
                    priority["state"], priority.get("reason"),
                    tuple(priority["hot"]), tuple(priority["warm"]),
                )
                monotonic_now = time.monotonic()
                if (idle_key != last_idle_key
                        or monotonic_now - last_idle_logged_at >= 180.0):
                    severity = (logging.ERROR if priority["state"] == "PREP_FALLBACK"
                                else logging.INFO)
                    logger.log(
                        severity,
                        "[KR][OPENING_OBSERVER][NO_COLLECTION_TARGET] source=%s "
                        "reason=%s hot=%s warm=%s cold=%s "
                        "action=WAIT_FOR_PB1_FINAL_SHORTLIST_BUY_FAIL_CLOSED",
                        priority["state"], priority.get("reason") or "NO_ELIGIBLE_PREMINUTE_BUY",
                        len(priority["hot"]), len(priority["warm"]), len(priority["cold"]),
                    )
                    last_idle_key, last_idle_logged_at = idle_key, monotonic_now
                time.sleep(min(10.0, max(2.0, float(args.interval_sec) / 3.0)))
                continue
            last_idle_key = None
            logger.info(
                "[KR][OPENING_OBSERVER][QUEUE] source=%s hot=%s warm=%s cold=%s "
                "selected=%s full_universe=%s order_authorization=0 native_collection_scope=%s",
                priority["state"], len(priority["hot"]), len(priority["warm"]),
                len(priority["cold"]), selected, len(codes), "PREP_COLD_DISCOVERY" if discovery_only else "HOT_WARM_ONLY",
            )
            result = run_once(kis=kis, codes=selected, observed_at=now,
                              refresh_clock=now_kst,
                              observation_styles=priority.get("observation_styles"),
                              discovery_only=discovery_only)
            for code, outcome in result.get("symbol_outcomes", {}).items():
                promotion.observe(code, outcome.get("opening_intelligence") or {})
                tier = next(
                    (name for name in ("hot", "warm", "cold")
                     if code in priority[name]), "unknown",
                )
                logger.info(
                    "[KR][OPENING_OBSERVER][POLL_PROOF_HEALTH] code=%s "
                    "tier=%s state=%s reason=%s opening=%s post_open=%s "
                    "last_completed_age_sec=%s vwap_verified=%s conflicts=%s "
                    "poll_elapsed_sec=%.3f poll_not_order_authorization=1",
                    code, tier, outcome["state"], outcome["reason"],
                    outcome["opening_count"], outcome["post_open_count"],
                    outcome["latest_bar_age_sec"],
                    int(outcome["vwap_verified"]), outcome["conflicts"],
                    outcome.get("poll_elapsed_sec", 0.0),
                )
        except Exception:
            logger.exception(
                "[KR][OPENING_OBSERVER][CYCLE_FAIL] action=RETRY_READ_ONLY "
                "sell_path_independent=1"
            )
            time.sleep(5.0)
            continue
        elapsed = time.monotonic() - cycle_started
        logger.info(
            "[KR][OPENING_OBSERVER][PASS] completed=%s unknown=%s "
            "coverage_ok=%s vwap_unverified=%s symbol_p95_sec=%s "
            "symbol_max_sec=%s elapsed_sec=%.1f target_period_sec=%.1f "
            "native_minute_requests=%s native_quote_requests=%s native_failures=%s",
            result["completed"], result["unknown"], result["coverage_ok"],
            result["vwap_unverified"], result["symbol_scan_p95_sec"],
            result["symbol_scan_max_sec"], elapsed,
            max(15.0, float(args.interval_sec)),
            result["native_minute_request_attempts"],
            result["native_quote_request_attempts"],
            result["native_request_failures"],
        )
        time.sleep(max(1.0, planner_wake_sec - elapsed))


if __name__ == "__main__":
    main()
