"""KR PB1 Sep-30 liveness fixes: canonical pretrade quote + budget contract.

This module closes two implementation gaps observed on 2026-09-30 without
changing PB1 entry thresholds, sizing, exit policy, durable order economics, or
KR_INFINITE ownership.

1. AM/afternoon PB1 keeps the PR148 20s pre-submit and 20s post-engine reserves,
   but raises the legacy 90s tick ceiling to 180s and requires 200s remaining
   before starting a new tick near the session end.
2. PB1 BUY pretrade no longer uses the independent ``get_quote_safe`` path.
   Immediately before pretrade validation it discards only a REST price-cache
   row older than the existing 5s KR freshness contract, then calls the PR140
   canonical ``get_price_snapshot`` path exactly once. That path is WebSocket
   first and uses governed REST only when a fresh stream/cache is unavailable.

The acquired quote is used only to validate current tradeability. The already
persisted strategy-generated order price and quantity are never rewritten.
Protective SELL routing and KR_INFINITE (122630) stay on their existing paths.
"""
from __future__ import annotations

import functools
import logging
import os
import time
from typing import Any, Callable

import trader.kis_wrapper as kis_wrapper_module
from trader.kis_wrapper import KisAPI
from trader.universe.validation import validate_tradeable_quote

logger = logging.getLogger(__name__)
_INSTALLED = False


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return float(default)


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.startswith("A") and text[1:].isdigit():
        text = text[1:]
    return text.zfill(6) if text.isdigit() else text


def _kr_infinite_symbol() -> str:
    return _normalize_code(os.getenv("KR_INFINITE_SYMBOL", "122630"))


def _is_kr_infinite_code(value: Any) -> bool:
    return _normalize_code(value) == _kr_infinite_symbol()


def _pretrade_quote_max_age_sec() -> float:
    requested = max(0.1, _env_float("KR_PB1_PRETRADE_QUOTE_MAX_AGE_SEC", 5.0))
    ws_bound = max(0.1, _env_float("KIS_WS_FRESH_MAX_AGE_SEC_KR", 5.0))
    return min(requested, ws_bound)


def _evict_stale_canonical_rest_cache(code: Any, *, market: str = "J") -> float | None:
    """Drop only this symbol's REST cache when older than the pretrade contract.

    PR140's canonical get_price_snapshot() checks fresh WebSocket data before
    consulting the REST cache. Its generic REST cache TTL defaults to 15s,
    while live-order pretrade freshness is 5s. Evicting an older row here makes
    the subsequent single canonical call either obtain a fresh WS quote, use a
    <=5s cached REST quote, perform governed REST, or fail closed.
    """
    cache_obj = getattr(kis_wrapper_module, "_price_cache", None)
    cache = getattr(cache_obj, "cache", None)
    if not isinstance(cache, dict):
        return None

    key = (str(market or "J"), _normalize_code(code))
    row = cache.get(key)
    if row is None:
        return None
    try:
        age = max(0.0, time.time() - float(getattr(row, "ts")))
    except Exception:
        age = float("inf")
    if age <= _pretrade_quote_max_age_sec():
        return age

    lock = getattr(cache_obj, "lock", None)
    try:
        if lock is not None:
            with lock:
                current = cache.get(key)
                if current is row:
                    cache.pop(key, None)
        else:
            if cache.get(key) is row:
                cache.pop(key, None)
    except Exception:
        # If safe eviction itself is uncertain, fail closed later by allowing
        # the canonical acquisition/validation path to decide. Do not fabricate
        # a fresh timestamp.
        logger.exception("[KR_P1][PRETRADE_CACHE_EVICT_FAIL] code=%s", key[1])
        return age

    logger.info(
        "[KR_P1][PRETRADE_CACHE_EVICT] code=%s age_sec=%.3f max_age_sec=%.3f",
        key[1],
        age,
        _pretrade_quote_max_age_sec(),
    )
    return age


def _resolve_authoritative_pretrade_quote(
    kis: Any,
    code: Any,
) -> tuple[dict[str, Any] | None, str]:
    normalized = _normalize_code(code)
    _evict_stale_canonical_rest_cache(normalized, market="J")

    # Exactly one canonical market-data acquisition. get_price_snapshot() is
    # PR140's WS-first path; if fresh WS is absent it falls back to the governed
    # symbol-scoped REST cache/request path.
    try:
        quote = kis.get_price_snapshot(normalized, market="J")
    except Exception as exc:
        return None, f"quote_fail:{exc}"
    if not isinstance(quote, dict):
        return None, "quote_missing"

    ok, reason = validate_tradeable_quote(quote)
    if not ok:
        return quote, reason
    return quote, "ok"


def _append_pretrade_skip(
    engine: Any,
    *,
    code: str,
    market: str | None,
    mode: int | None,
    side: str,
    qty: int | None,
    price: float | None,
    client_order_key: str | None,
    stage: str,
    reason: str,
) -> None:
    display_code = engine._display_code(code) if hasattr(engine, "_display_code") else code
    logger.warning("[PB1][PRETRADE][SKIP] code=%s reason=%s stage=%s", display_code, reason, stage)
    try:
        engine._append_ledger_event(
            event_type="ORDER_SKIP",
            code=code,
            market=market,
            mode=mode,
            side=side,
            qty=qty,
            price=price,
            client_order_key=client_order_key,
            ok=False,
            reasons=[f"pretrade:{reason}"],
            stage=stage,
        )
    except Exception:
        logger.exception("[PB1][LEDGER][PRETRADE_SKIP_FAIL] code=%s", display_code)


def _build_pb1_pretrade_canonical_quote_guard(original: Callable[..., bool]) -> Callable[..., bool]:
    """Replace only PB1-standard BUY quote acquisition; preserve all other gates."""
    @functools.wraps(original)
    def guarded(
        self,
        *,
        code: str,
        market: str | None,
        mode: int | None,
        side: str,
        qty: int | None,
        price: float | None,
        client_order_key: str | None,
        stage: str,
    ) -> bool:
        if str(side or "").upper() != "BUY" or _is_kr_infinite_code(code):
            return original(
                self,
                code=code,
                market=market,
                mode=mode,
                side=side,
                qty=qty,
                price=price,
                client_order_key=client_order_key,
                stage=stage,
            )

        # Current PB1 pretrade has policy/live-gate checks before its legacy
        # validate_tradeable() call. Preserve those exactly by letting the
        # original return when any gate is active.
        gate_fn = getattr(self, "_order_precheck_gate_reasons", None)
        if callable(gate_fn):
            try:
                if gate_fn(side=side, stage=stage):
                    return original(
                        self,
                        code=code,
                        market=market,
                        mode=mode,
                        side=side,
                        qty=qty,
                        price=price,
                        client_order_key=client_order_key,
                        stage=stage,
                    )
            except Exception:
                return original(
                    self,
                    code=code,
                    market=market,
                    mode=mode,
                    side=side,
                    qty=qty,
                    price=price,
                    client_order_key=client_order_key,
                    stage=stage,
                )

        kis = getattr(self, "kis", None)
        if kis is None:
            return False

        # The sanctioned production client is KisAPI and always exposes the
        # PR140 canonical snapshot method.  Existing unit/integration adapters
        # predate PR140 and often implement only get_quote_safe/order methods;
        # preserve their legacy validator contract rather than turning an
        # adapter capability gap into a new execution-policy block.
        if not callable(getattr(kis, "get_price_snapshot", None)):
            logger.debug(
                "[KR_P1][PRETRADE_QUOTE_COMPAT] code=%s action=LEGACY_VALIDATOR adapter=%s",
                _normalize_code(code),
                type(kis).__name__,
            )
            return original(
                self,
                code=code,
                market=market,
                mode=mode,
                side=side,
                qty=qty,
                price=price,
                client_order_key=client_order_key,
                stage=stage,
            )

        quote, acquisition_reason = _resolve_authoritative_pretrade_quote(kis, code)
        ok, reason = validate_tradeable_quote(quote)
        if not ok:
            reason = acquisition_reason if acquisition_reason != "ok" else reason
            _append_pretrade_skip(
                self,
                code=code,
                market=market,
                mode=mode,
                side=side,
                qty=qty,
                price=price,
                client_order_key=client_order_key,
                stage=stage,
                reason=reason,
            )
            logger.warning(
                "[KR_P1][PRETRADE_QUOTE_DEFER] code=%s reason=%s action=NO_BROKER_SUBMIT",
                _normalize_code(code),
                reason,
            )
            return False

        # Do not mutate price/qty here. The durable BUY intent was already
        # persisted by PB1 before pretrade, so rewriting broker economics here
        # would violate the immutable order contract.
        logger.info(
            "[KR_P1][PRETRADE_QUOTE_OK] code=%s durable_price_unchanged=%s",
            _normalize_code(code),
            price,
        )
        return True

    return guarded


def _apply_live_session_budget_contract() -> None:
    session = str(os.getenv("PB1_SESSION") or os.getenv("WSL_RUN_SESSION") or "").strip().lower()
    if session not in {"am", "afternoon", "pm"}:
        return

    # Upgrade only known legacy/default values so an explicit operator override
    # remains respected. The production AM/PM WSL wrappers currently export 90.
    timeout_raw = str(os.getenv("PB1_TICK_HARD_TIMEOUT_SEC") or "").strip()
    if timeout_raw in {"", "90", "90.0"}:
        os.environ["PB1_TICK_HARD_TIMEOUT_SEC"] = "180"

    min_budget_raw = str(os.getenv("PB1_MIN_TICK_BUDGET_SEC") or "").strip()
    if min_budget_raw in {"", "75", "75.0", "105", "105.0"}:
        os.environ["PB1_MIN_TICK_BUDGET_SEC"] = "200"

    # PR148 safety reserves remain explicit and unchanged.
    os.environ.setdefault("KR_POST_ENGINE_RESERVE_SEC", "20")
    os.environ.setdefault("KR_ORDER_SUBMIT_MIN_REMAINING_SEC", "20")

    logger.warning(
        "[KR_P1][BUDGET_CONTRACT] session=%s tick_hard_timeout_sec=%s min_tick_budget_sec=%s "
        "post_engine_reserve_sec=%s order_submit_min_remaining_sec=%s",
        session,
        os.getenv("PB1_TICK_HARD_TIMEOUT_SEC"),
        os.getenv("PB1_MIN_TICK_BUDGET_SEC"),
        os.getenv("KR_POST_ENGINE_RESERVE_SEC"),
        os.getenv("KR_ORDER_SUBMIT_MIN_REMAINING_SEC"),
    )


def install_kr_20260930_runtime_integrity() -> None:
    """Install after PR148 runtime guards; strategy-owner policy is unchanged."""
    global _INSTALLED
    if _INSTALLED:
        return

    _apply_live_session_budget_contract()

    import trader.pb1_engine as pb1_engine

    if not getattr(pb1_engine.PB1Engine, "_kr_p1_20260930_pretrade_quote_installed", False):
        pb1_engine.PB1Engine._pretrade_check = _build_pb1_pretrade_canonical_quote_guard(
            pb1_engine.PB1Engine._pretrade_check
        )
        pb1_engine.PB1Engine._kr_p1_20260930_pretrade_quote_installed = True

    _INSTALLED = True
