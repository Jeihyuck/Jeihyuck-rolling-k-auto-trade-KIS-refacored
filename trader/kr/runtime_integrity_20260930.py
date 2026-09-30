"""KR PB1 Sep-30 liveness fixes: shared pretrade quote + budget contract.

This module closes two implementation gaps observed on 2026-09-30 without
changing PB1 entry thresholds, sizing, exit policy, or KR_INFINITE ownership.

1. AM/afternoon PB1 keeps the PR148 20s pre-submit and 20s post-engine reserves,
   but raises the legacy 90s tick ceiling to 180s and requires 200s remaining
   before starting a new tick near the session end.
2. PB1 BUY pretrade no longer uses the independent ``get_quote_safe`` path.
   It reuses the canonical ``get_price_snapshot`` result when still fresh, or
   refreshes exactly once through that same WS-first/REST-fallback path.  The
   exact quote used for tradeability validation is also used to derive a
   conservative refreshed limit cap.  A refreshed cap may lower a BUY limit,
   but can never raise the original strategy-generated limit or order notional.

Protective SELL routing and KR_INFINITE (122630) stay on their existing paths.
"""
from __future__ import annotations

import functools
import logging
import math
import os
import threading
import time
from typing import Any, Callable

from trader.kis_wrapper import KisAPI
from trader.kr_price_utils import normalize_kr_order_price
from trader.universe.validation import validate_tradeable_quote

logger = logging.getLogger(__name__)
_INSTALLED = False
_STATE = threading.local()


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


def _quote_cache(kis: Any) -> dict[str, dict[str, Any]]:
    cache = getattr(kis, "_kr_pb1_pretrade_quote_cache", None)
    if not isinstance(cache, dict):
        cache = {}
        try:
            setattr(kis, "_kr_pb1_pretrade_quote_cache", cache)
        except Exception:
            pass
    return cache


def _remember_quote(kis: Any, code: Any, quote: Any, *, source: str) -> None:
    normalized = _normalize_code(code)
    if not normalized or not isinstance(quote, dict):
        return
    ok, _reason = validate_tradeable_quote(quote)
    if not ok:
        return
    _quote_cache(kis)[normalized] = {
        "quote": dict(quote),
        "captured_monotonic": time.monotonic(),
        "source": str(source or "unknown"),
    }


def _fresh_cached_quote(kis: Any, code: Any) -> tuple[dict[str, Any] | None, str, float | None]:
    normalized = _normalize_code(code)
    item = _quote_cache(kis).get(normalized) or {}
    quote = item.get("quote")
    captured = item.get("captured_monotonic")
    if not isinstance(quote, dict) or captured is None:
        return None, "cache_miss", None
    try:
        age = max(0.0, time.monotonic() - float(captured))
    except Exception:
        return None, "cache_invalid", None
    if age > _pretrade_quote_max_age_sec():
        return None, "cache_stale", age
    ok, reason = validate_tradeable_quote(quote)
    if not ok:
        return None, f"cache_invalid:{reason}", age
    return dict(quote), str(item.get("source") or "shared_snapshot"), age


def _build_price_snapshot_capture(original: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(original)
    def captured(self, code: Any, *args: Any, **kwargs: Any):
        quote = original(self, code, *args, **kwargs)
        _remember_quote(self, code, quote, source="get_price_snapshot")
        return quote

    return captured


def _resolve_authoritative_pretrade_quote(kis: Any, code: Any) -> tuple[dict[str, Any] | None, str, float | None, str]:
    cached, source, age = _fresh_cached_quote(kis, code)
    if cached is not None:
        return cached, source, age, "ok"

    # PR140 made get_price_snapshot the canonical KR WS-first -> governed REST
    # fallback path. Reuse it instead of the legacy independent
    # get_quote_safe(diag_mode=True) pretrade fetch.
    try:
        quote = kis.get_price_snapshot(_normalize_code(code), market="J")
    except Exception as exc:
        return None, "refresh_failed", None, f"quote_fail:{exc}"
    if not isinstance(quote, dict):
        return None, "refresh_missing", None, "quote_missing"
    ok, reason = validate_tradeable_quote(quote)
    if not ok:
        return quote, "refresh_invalid", 0.0, reason
    _remember_quote(kis, code, quote, source="pretrade_refresh")
    return dict(quote), "pretrade_refresh", 0.0, "ok"


def _clear_pending_buy() -> None:
    _STATE.pending_pb1_buy = None


def _set_pending_buy(*, kis: Any, code: Any, stage: str, quote_source: str, quote_age: float | None, candidate_limit: float | None) -> None:
    _STATE.pending_pb1_buy = {
        "kis_id": id(kis),
        "code": _normalize_code(code),
        "stage": str(stage or ""),
        "created_monotonic": time.monotonic(),
        "quote_source": str(quote_source or "unknown"),
        "quote_age_sec": quote_age,
        "candidate_limit": float(candidate_limit) if candidate_limit not in (None, "") else None,
    }


def _pending_buy_for(kis: Any, code: Any) -> dict[str, Any] | None:
    pending = getattr(_STATE, "pending_pb1_buy", None)
    if not isinstance(pending, dict):
        return None
    if pending.get("kis_id") != id(kis) or pending.get("code") != _normalize_code(code):
        return None
    try:
        age = max(0.0, time.monotonic() - float(pending.get("created_monotonic") or 0.0))
    except Exception:
        return None
    if age > _pretrade_quote_max_age_sec():
        return None
    return pending


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


def _build_pb1_pretrade_shared_quote_guard(original: Callable[..., bool]) -> Callable[..., bool]:
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

        _clear_pending_buy()

        # Preserve all existing live-gate/order-precheck policy. The original
        # returns before market-data I/O when these reasons are present.
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

        quote, source, age, acquisition_reason = _resolve_authoritative_pretrade_quote(kis, code)
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
                "[KR_P1][PRETRADE_QUOTE_DEFER] code=%s source=%s reason=%s action=NO_BROKER_SUBMIT",
                _normalize_code(code),
                source,
                reason,
            )
            return False

        candidate_limit: float | None = None
        calc = getattr(self, "_calc_order_price", None)
        if callable(calc):
            try:
                candidate_limit, _candidate_source = calc(_normalize_code(code), quote, None)
                if candidate_limit is not None:
                    candidate_limit = float(candidate_limit)
                    if not math.isfinite(candidate_limit) or candidate_limit <= 0:
                        candidate_limit = None
            except Exception:
                logger.exception("[KR_P1][PRETRADE_LIMIT_REFRESH_FAIL] code=%s", _normalize_code(code))
                candidate_limit = None

        _set_pending_buy(
            kis=kis,
            code=code,
            stage=stage,
            quote_source=source,
            quote_age=age,
            candidate_limit=candidate_limit,
        )
        logger.info(
            "[KR_P1][PRETRADE_QUOTE_OK] code=%s source=%s age_sec=%s candidate_limit=%s",
            _normalize_code(code),
            source,
            "NA" if age is None else f"{age:.3f}",
            "NA" if candidate_limit is None else f"{candidate_limit:.2f}",
        )
        return True

    return guarded


def _build_pb1_limit_buy_quote_cap(original: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(original)
    def guarded(self, pdno: str, qty: int, price: int, *args: Any, **kwargs: Any):
        final_price: int | float = price
        pending = _pending_buy_for(self, pdno)
        try:
            if pending is not None:
                candidate = pending.get("candidate_limit")
                try:
                    original_norm, _ = normalize_kr_order_price(float(price or 0), side="BUY")
                except Exception:
                    original_norm = 0
                try:
                    candidate_norm, _ = normalize_kr_order_price(float(candidate or 0), side="BUY")
                except Exception:
                    candidate_norm = 0
                if original_norm > 0 and candidate_norm > 0:
                    # Never raise the strategy-generated BUY limit/notional.
                    final_price = min(original_norm, candidate_norm)
                    logger.info(
                        "[KR_P1][BUY_LIMIT_SHARED_QUOTE] code=%s original=%s refreshed=%s final=%s source=%s",
                        _normalize_code(pdno),
                        original_norm,
                        candidate_norm,
                        final_price,
                        pending.get("quote_source"),
                    )
            return original(self, pdno, qty, final_price, *args, **kwargs)
        finally:
            _clear_pending_buy()

    return guarded


def _build_pb1_market_buy_context_clear(original: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(original)
    def guarded(self, pdno: str, qty: int, *args: Any, **kwargs: Any):
        try:
            return original(self, pdno, qty, *args, **kwargs)
        finally:
            _clear_pending_buy()

    return guarded


def _apply_live_session_budget_contract() -> None:
    session = str(os.getenv("PB1_SESSION") or os.getenv("WSL_RUN_SESSION") or "").strip().lower()
    if session not in {"am", "afternoon", "pm"}:
        return

    # Upgrade only known legacy/default values so explicit operator overrides
    # remain respected.
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

    if not getattr(KisAPI, "_kr_p1_20260930_snapshot_capture_installed", False):
        KisAPI.get_price_snapshot = _build_price_snapshot_capture(KisAPI.get_price_snapshot)
        KisAPI._kr_p1_20260930_snapshot_capture_installed = True

    if not getattr(pb1_engine.PB1Engine, "_kr_p1_20260930_pretrade_quote_installed", False):
        pb1_engine.PB1Engine._pretrade_check = _build_pb1_pretrade_shared_quote_guard(
            pb1_engine.PB1Engine._pretrade_check
        )
        pb1_engine.PB1Engine._kr_p1_20260930_pretrade_quote_installed = True

    if not getattr(KisAPI, "_kr_p1_20260930_buy_limit_quote_cap_installed", False):
        KisAPI.buy_stock_limit = _build_pb1_limit_buy_quote_cap(KisAPI.buy_stock_limit)
        KisAPI.buy_stock_market = _build_pb1_market_buy_context_clear(KisAPI.buy_stock_market)
        KisAPI._kr_p1_20260930_buy_limit_quote_cap_installed = True

    _INSTALLED = True
