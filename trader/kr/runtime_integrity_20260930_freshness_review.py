"""Freshness review fix for the Sep-30 KR PB1 liveness patch.

PR149 initially checked the outer ``_PriceCache`` insertion timestamp before
calling ``get_price_snapshot``.  That timestamp is not the market-data source
time: a quote can spend time in the inner two-second ``price_cache`` and then be
re-cached by ``_PriceCache`` with a new timestamp.  A strict live-order age
contract must therefore never use the outer cache insertion time as evidence of
freshness.

This module replaces only PR149's PB1 BUY pretrade quote resolver.  It preserves
all strategy-generated order economics and PR148 broker-boundary reserves.

Freshness authority:
* WebSocket: ``received_at`` (or ``age_sec`` derived from it) from the KIS WS
  service, with the exact PB1 pretrade max-age passed to both get/wait calls.
* REST: the insertion time of the inner ``price_cache`` row, derived from its
  expiry timestamp minus ``PRICE_SNAPSHOT_TTL_SEC``.
* The outer ``_price_cache`` timestamp is never accepted as source provenance.

``KisAPI.get_price_quote`` is itself WS-first.  Before using it as the governed
REST/cache fallback, this guard removes only the target symbol's WS cache row
when that row is already older than the stricter PB1 pretrade age.  This avoids
a stale-under-strict-but-fresh-under-default (for example 1.2s vs 1s/5s) WS row
masking an otherwise available REST refresh.  A newly arrived WS quote is still
accepted only after the same source-time validation.

If source time cannot be proven, or if it exceeds the configured maximum age,
new BUY is deferred before broker submit.
"""
from __future__ import annotations

import logging
import math
import time
from typing import Any

import trader.kis_wrapper as kis_wrapper_module
import trader.kr.runtime_integrity_20260930 as base
from trader.cache_ttl import PRICE_SNAPSHOT_TTL_SEC, price_cache
from trader.universe.validation import validate_tradeable_quote

logger = logging.getLogger(__name__)
_INSTALLED = False


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.startswith("A") and text[1:].isdigit():
        text = text[1:]
    return text.zfill(6) if text.isdigit() else text


def _finite_epoch(value: Any) -> float | None:
    try:
        parsed = float(value)
    except Exception:
        return None
    if not math.isfinite(parsed) or parsed <= 0:
        return None
    return parsed


def _quote_source_ts(quote: Any, *, now_ts: float | None = None) -> float | None:
    """Return original source epoch only when the quote carries provenance."""
    if not isinstance(quote, dict):
        return None
    now_value = time.time() if now_ts is None else float(now_ts)
    raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else {}

    for candidate in (
        quote.get("_source_ts"),
        quote.get("received_at"),
        raw.get("received_at"),
    ):
        source_ts = _finite_epoch(candidate)
        if source_ts is not None:
            return source_ts

    for candidate in (quote.get("age_sec"), raw.get("age_sec")):
        try:
            age = float(candidate)
        except Exception:
            continue
        if math.isfinite(age) and age >= 0:
            return max(0.001, now_value - age)
    return None


def _validate_source_freshness(
    quote: Any,
    *,
    max_age_sec: float,
    source_kind: str,
    now_ts: float | None = None,
) -> tuple[dict[str, Any] | None, str]:
    if not isinstance(quote, dict):
        return None, "quote_missing"
    now_value = time.time() if now_ts is None else float(now_ts)
    source_ts = _quote_source_ts(quote, now_ts=now_value)
    if source_ts is None:
        return None, "quote_freshness_unknown"

    age = max(0.0, now_value - source_ts)
    if age > max(0.05, float(max_age_sec)):
        return None, f"quote_stale:{age:.3f}s"

    out = dict(quote)
    out["_source_ts"] = float(source_ts)
    out["_source_age_sec"] = float(age)
    out["_source_kind"] = str(source_kind)
    return out, "ok"


def _outer_cache_key(code: Any) -> tuple[str, str]:
    return ("J", _normalize_code(code))


def _inner_cache_key(code: Any) -> tuple[str, str]:
    return ("inquire-price", _normalize_code(code))


def _evict_outer_cache(code: Any) -> None:
    """Outer cache time is acquisition-time-unsafe, so strict BUY never trusts it."""
    cache_obj = getattr(kis_wrapper_module, "_price_cache", None)
    cache = getattr(cache_obj, "cache", None)
    if not isinstance(cache, dict):
        return
    key = _outer_cache_key(code)
    lock = getattr(cache_obj, "lock", None)
    try:
        if lock is not None:
            with lock:
                cache.pop(key, None)
        else:
            cache.pop(key, None)
    except Exception:
        logger.exception("[KR_P1][FRESHNESS][OUTER_CACHE_EVICT_FAIL] code=%s", key[1])


def _inner_cache_source_ts(code: Any) -> float | None:
    """Derive REST acquisition time from the inner TTL row without resetting it."""
    key = _inner_cache_key(code)
    row = getattr(price_cache, "cache", {}).get(key)
    if not (isinstance(row, tuple) and len(row) == 2):
        return None
    _value, expires_at = row
    expiry = _finite_epoch(expires_at)
    if expiry is None:
        return None
    ttl = max(0.001, float(PRICE_SNAPSHOT_TTL_SEC))
    source_ts = expiry - ttl
    return source_ts if source_ts > 0 else None


def _evict_inner_cache_if_not_fresh(code: Any, *, max_age_sec: float, now_ts: float | None = None) -> None:
    """Discard an inner REST row when its original acquisition exceeds strict age."""
    now_value = time.time() if now_ts is None else float(now_ts)
    key = _inner_cache_key(code)
    cache = getattr(price_cache, "cache", None)
    if not isinstance(cache, dict):
        return
    row = cache.get(key)
    if row is None:
        return
    source_ts = _inner_cache_source_ts(code)
    age = float("inf") if source_ts is None else max(0.0, now_value - source_ts)
    if source_ts is not None and age <= max(0.05, float(max_age_sec)):
        return
    cache.pop(key, None)
    logger.info(
        "[KR_P1][FRESHNESS][INNER_CACHE_EVICT] code=%s source_age_sec=%s max_age_sec=%.3f",
        key[1],
        "unknown" if not math.isfinite(age) else f"{age:.3f}",
        max(0.05, float(max_age_sec)),
    )


def _evict_ws_cache_if_not_fresh(code: Any, *, max_age_sec: float, now_ts: float | None = None) -> bool:
    """Remove only a stale-under-strict target WS row before REST fallback.

    ``get_price_quote`` also checks WebSocket using the general KR WS age.  When
    PB1 requests a stricter age (for example 1s while the general setting is
    5s), leaving a 1.2s row in the service would make that fallback return the
    same stale-under-strict WS quote instead of progressing to REST.  Touch only
    the exact symbol, under the service lock, and only when its received_at is
    provably older than the strict threshold.
    """
    normalized = _normalize_code(code)
    try:
        service = kis_wrapper_module.get_kis_ws_price_service()
        lock = getattr(service, "_lock", None)
        quotes = getattr(service, "_quotes", None)
        if lock is None or not isinstance(quotes, dict):
            return False
        key = ("KR", normalized)
        now_value = time.time() if now_ts is None else float(now_ts)
        with lock:
            current = quotes.get(key)
            if current is None:
                return False
            received_at = _finite_epoch(getattr(current, "received_at", None))
            if received_at is None:
                return False
            age = max(0.0, now_value - received_at)
            if age <= max(0.05, float(max_age_sec)):
                return False
            if quotes.get(key) is current:
                quotes.pop(key, None)
        logger.info(
            "[KR_P1][FRESHNESS][WS_CACHE_EVICT] code=%s source_age_sec=%.3f max_age_sec=%.3f",
            normalized,
            age,
            max(0.05, float(max_age_sec)),
        )
        return True
    except Exception:
        logger.exception("[KR_P1][FRESHNESS][WS_CACHE_EVICT_FAIL] code=%s", normalized)
        return False


def _strict_ws_quote(code: Any, *, max_age_sec: float) -> tuple[dict[str, Any] | None, str]:
    normalized = _normalize_code(code)
    try:
        service = kis_wrapper_module.get_kis_ws_price_service()
        service.subscribe_kr(normalized)
        quote = service.get_fresh_quote("KR", normalized, max_age_sec=max_age_sec)
        if quote is None:
            wait_sec = max(0.0, base._env_float("KIS_WS_INITIAL_WAIT_SEC", 0.35))
            if wait_sec > 0:
                quote = service.wait_for_fresh_quote(
                    "KR",
                    normalized,
                    max_age_sec=max_age_sec,
                    wait_sec=wait_sec,
                )
    except Exception as exc:
        logger.warning("[KR_P1][FRESHNESS][WS_FAIL] code=%s err=%s", normalized, exc)
        return None, f"ws_quote_fail:{exc}"

    if quote is None:
        return None, "ws_quote_missing"
    checked, reason = _validate_source_freshness(
        quote,
        max_age_sec=max_age_sec,
        source_kind="KIS_WEBSOCKET",
    )
    if checked is None:
        logger.warning(
            "[KR_P1][FRESHNESS][WS_REJECT] code=%s reason=%s max_age_sec=%.3f",
            normalized,
            reason,
            max_age_sec,
        )
    return checked, reason


def _strict_rest_quote(kis: Any, code: Any, *, max_age_sec: float) -> tuple[dict[str, Any] | None, str]:
    normalized = _normalize_code(code)

    # The outer cache timestamp can be newer than the market-data source. Never
    # let it authorize strict pretrade. The inner cache exposes expiry and thus
    # lets us recover the direct REST acquisition time. Also remove a target WS
    # row already known to be too old for the stricter PB1 contract so the
    # WS-first get_price_quote fallback can actually progress to REST.
    _evict_outer_cache(normalized)
    _evict_inner_cache_if_not_fresh(normalized, max_age_sec=max_age_sec)
    _evict_ws_cache_if_not_fresh(normalized, max_age_sec=max_age_sec)

    try:
        quote = kis.get_price_quote(normalized, diag_mode=False, attempts=2)
    except Exception as exc:
        return None, f"rest_quote_fail:{exc}"
    if not isinstance(quote, dict) or not quote:
        return None, "rest_quote_missing"

    # A fresh WS quote may arrive between strict WS miss and this fallback. If
    # get_price_quote returns it, preserve and validate its original received_at
    # rather than incorrectly treating it as REST.
    source_ts = _quote_source_ts(quote)
    raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else {}
    looks_like_ws = (
        quote.get("received_at") is not None
        or raw.get("received_at") is not None
        or "WEBSOCKET" in str(quote.get("source") or raw.get("source") or "").upper()
    )
    if looks_like_ws and source_ts is not None:
        checked, reason = _validate_source_freshness(
            quote,
            max_age_sec=max_age_sec,
            source_kind="KIS_WEBSOCKET",
        )
        return checked, reason

    source_ts = _inner_cache_source_ts(normalized)
    if source_ts is None:
        # A successful non-WS quote without its inner acquisition timestamp
        # cannot prove freshness. Never substitute call-return/cache time.
        return None, "quote_freshness_unknown"

    enriched = dict(quote)
    enriched["_source_ts"] = float(source_ts)
    checked, reason = _validate_source_freshness(
        enriched,
        max_age_sec=max_age_sec,
        source_kind="KIS_REST",
    )
    if checked is None:
        logger.warning(
            "[KR_P1][FRESHNESS][REST_REJECT] code=%s reason=%s max_age_sec=%.3f",
            normalized,
            reason,
            max_age_sec,
        )
    return checked, reason


def _resolve_authoritative_pretrade_quote_strict(
    kis: Any,
    code: Any,
) -> tuple[dict[str, Any] | None, str]:
    """Resolve one source-time-proven quote under the exact PB1 max-age contract."""
    normalized = _normalize_code(code)
    max_age_sec = base._pretrade_quote_max_age_sec()

    ws_quote, ws_reason = _strict_ws_quote(normalized, max_age_sec=max_age_sec)
    if ws_quote is not None:
        ok, reason = validate_tradeable_quote(ws_quote)
        return (ws_quote, "ok") if ok else (ws_quote, reason)

    rest_quote, rest_reason = _strict_rest_quote(kis, normalized, max_age_sec=max_age_sec)
    if rest_quote is not None:
        ok, reason = validate_tradeable_quote(rest_quote)
        return (rest_quote, "ok") if ok else (rest_quote, reason)

    # Preserve the most diagnostic freshness rejection when both sources fail.
    for reason in (ws_reason, rest_reason):
        if reason.startswith("quote_stale") or reason == "quote_freshness_unknown":
            return None, reason
    return None, rest_reason if rest_reason != "rest_quote_missing" else ws_reason


def install_kr_20260930_freshness_review() -> None:
    """Install after runtime_integrity_20260930; no owner/policy/economics change."""
    global _INSTALLED
    if _INSTALLED:
        return
    base._resolve_authoritative_pretrade_quote = _resolve_authoritative_pretrade_quote_strict
    base._kr_p1_20260930_source_time_freshness_installed = True
    _INSTALLED = True
