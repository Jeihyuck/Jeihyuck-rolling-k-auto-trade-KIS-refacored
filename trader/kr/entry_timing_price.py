"""Conservative KR PB1 price fallback bounded by ACTIVE completed-1m proof.

Only the pre-existing prpr+slippage *fallback* is eligible. A genuine ask
above the accepted price ceiling is never replaced by a pretend executable
ask. A passive no-ask LIMIT may not fill, but can never chase above the
original 0.4% absolute-deviation gate or frozen style maximum.
"""
from __future__ import annotations

from datetime import datetime
import math
from typing import Any, Mapping

from trader.kr.opening_bar_contract import MAX_ORDER_PRICE_DEVIATION, KST
from trader.kr_price_utils import normalize_kr_order_price


def _pos(value: Any) -> float | None:
    try:
        number = float(value)
        return number if math.isfinite(number) and number > 0 else None
    except (TypeError, ValueError):
        return None


def resolve_bounded_prpr_limit(
    *, code: str, original_price: float, timing: Mapping[str, Any],
    quote: Mapping[str, Any], observed_at: datetime, max_quote_age_sec: float = 5.0,
) -> dict[str, Any]:
    """Return diagnostics + passive/ask LIMIT; never broker authorization.

    The caller MUST independently re-evaluate full timing, account, risk,
    quantity, budget, duplicate/order state and final broker-submit fences.
    This function cannot turn a WAIT/UNKNOWN timing verdict into a BUY.
    """
    result: dict[str, Any] = {
        "state": "BLOCK",
        "reason": "PRICE_INPUT_UNVERIFIED",
        "new_limit": None,
        "original_price": original_price,
        "source": "KR_PB1_PRPR_BOUNDED_LIMIT",
        "not_order_authorization": True,
    }

    def block(reason: str, **extra: Any) -> dict[str, Any]:
        return {**result, "reason": reason, **extra}

    if observed_at.tzinfo is None:
        return block("QUOTE_STALE")
    now = observed_at.astimezone(KST)
    if str(timing.get("state")) != "READY_SHADOW":
        return block("TIMING_NOT_READY")
    reference = _pos(timing.get("reference_price"))
    strategy_cap = _pos(timing.get("max_entry_price"))
    original = _pos(original_price)
    if not all((reference, strategy_cap, original)):
        return block("PRICE_INPUT_UNVERIFIED")
    receipt = _pos(quote.get("received_at"))
    if receipt is None:
        return block("QUOTE_STALE", quote_age_sec=None)
    quote_age = now.timestamp() - receipt
    if quote_age < -2 or quote_age > max_quote_age_sec or bool(quote.get("stale")):
        return block("QUOTE_STALE", quote_age_sec=round(quote_age, 3))
    source = str(quote.get("source") or "").upper()
    identity = str(quote.get("source_identity") or "").upper()
    # Never treat a free-text source label as authenticated broker evidence.
    # REST binds the submitted request code; WS binds the parsed subscription
    # code. Missing/contradictory identity must fail closed for every symbol.
    expected_identity = {
        "KIS_REST_INQUIRE_PRICE": "AUTHENTICATED_REST_REQUEST_BOUND",
        "KIS_WEBSOCKET": "KIS_WS_PARSED_SYMBOL",
    }.get(source)
    if expected_identity is None or identity != expected_identity:
        return block("QUOTE_SOURCE_UNVERIFIED", quote_age_sec=round(quote_age, 3))
    market = str(quote.get("market") or "").upper()
    if market != "KR":
        return block("QUOTE_MARKET_UNVERIFIED")
    requested_raw = str(quote.get("requested_symbol") or "").strip()
    symbol_raw = str(quote.get("symbol") or "").strip()
    if not requested_raw and not symbol_raw:
        return block("QUOTE_SYMBOL_UNVERIFIED")
    normalized_code = str(code).removeprefix("A").zfill(6)
    for raw_code in (requested_raw, symbol_raw):
        if not raw_code:
            continue
        normalized_quote = raw_code.removeprefix("A")
        if not normalized_quote.isascii() or not normalized_quote.isdigit() or normalized_quote.zfill(6) != normalized_code:
            return block("QUOTE_SYMBOL_MISMATCH")
    quote_day = str(quote.get("stck_bsop_date") or "").replace("-", "")
    if quote_day and quote_day != now.strftime("%Y%m%d"):
        return block("QUOTE_STALE", quote_date_mismatch=True)
    last = _pos(quote.get("last") or quote.get("prpr") or quote.get("stck_prpr"))
    if last is None:
        return block("QUOTE_LAST_UNVERIFIED")
    lower = reference * (1 - MAX_ORDER_PRICE_DEVIATION)
    upper = min(reference * (1 + MAX_ORDER_PRICE_DEVIATION), strategy_cap, original)
    # A materially different last trade means that source quote and completed
    # minute proof do not represent the same acceptable trading region.
    if not lower <= last <= upper:
        return block("MINUTE_CLOSE_PRICE_DEVIATION",
                     reference_price=reference, quote_last=last,
                     price_floor=lower, price_ceiling=upper)
    ask = _pos(quote.get("ask"))
    if ask is not None:
        proposed, tick = normalize_kr_order_price(ask, side="BUY")
        if proposed > upper:
            return block("ASK_ABOVE_MAX_ENTRY_PRICE", quote_ask=ask,
                         price_ceiling=upper, reference_price=reference)
        if proposed < lower:
            return block("MINUTE_CLOSE_PRICE_DEVIATION", quote_ask=ask)
        method = "VERIFIED_ASK"
    else:
        # No authoritative sell ask: a passive LIMIT is a ceiling, never a
        # claim that the offer is executable. Use SELL floor rounding to
        # avoid the BUY tick rounding crossing the proven max.
        proposed, tick = normalize_kr_order_price(upper, side="SELL")
        method = "PASSIVE_LIMIT_NO_ASK"
    if proposed <= 0 or proposed < lower or proposed > upper:
        return block("INVALID_TICK_PRICE", price_ceiling=upper, proposed=proposed)
    if normalize_kr_order_price(proposed, side="BUY")[0] != proposed:
        return block("INVALID_TICK_PRICE", proposed=proposed)
    return {
        **result, "state": "BOUNDED_LIMIT", "reason": method,
        "new_limit": proposed, "tick_size": tick,
        "reference_price": reference, "quote_last": last,
        "quote_ask": ask, "quote_source": source,
        "quote_age_sec": round(quote_age, 3),
        "native_quote_day_present": bool(quote_day),
        "price_floor": lower, "price_ceiling": upper,
        "signed_final_deviation_pct": round(100 * (proposed / reference - 1), 4),
    }


def maybe_bound_pb1_prpr_fallback(
    *, engine: Any, cf: Any, stage: str, order_type: str,
    original_price: float, now: datetime,
) -> dict[str, Any]:
    """Opt in *only* the standard KR PB1 new LIMIT path, never other sleeves.

    This pre-planning operation cannot authorize BUY. It may lower the
    synthetic +0.5% limit if the original completed-1m decision at the
    unadjusted native close was READY, and the latest KIS quote is fresh.
    The normal pre-submit evaluator runs AGAIN at the final price.
    """
    import os
    from trader.account_state import get_account_key
    from trader.kr.opening_bar_contract import (
        evaluate_proof_for_candidate, load_opening_proof,
    )
    features = getattr(cf, "features", None)
    if not isinstance(features, dict):
        return {"state": "NOT_APPLICABLE"}
    symbol = str(getattr(cf, "code", "") or "").removeprefix("A").zfill(6)
    if not (
        str(getattr(engine, "STRATEGY_NAME", "")) == "pb1_pullback_close"
        and str(getattr(engine, "env", "")).lower() == "practice"
        and str(stage or "").upper() != "PB1-CLOSE"
        and str(order_type or "").upper() == "LIMIT"
        and len(symbol) == 6 and symbol.isdigit() and symbol != "122630"
        and str(features.get("order_price_source") or "") == "prpr_slippage"
        and str(os.getenv("KR_PB1_ENTRY_TIMING_MODE") or "ENFORCE").upper() == "ENFORCE"
        and os.getenv("KR_PB1_ENTRY_TIMING_APPROVED", "1") == "1"
    ):
        return {"state": "NOT_APPLICABLE"}
    reference_price = _pos(original_price)
    if reference_price is None or now.tzinfo is None:
        return {"state": "BLOCK", "reason": "PRICE_INPUT_UNVERIFIED"}
    account_scope = get_account_key(env="practice")
    proof = load_opening_proof(
        env="practice", account_scope=account_scope, code=symbol, now=now,
    )
    bars = (proof or {}).get("bars") or []
    close = _pos(bars[-1].get("close")) if bars else None
    if close is None:
        return {"state": "BLOCK", "reason": "NATIVE_MINUTE_PROOF_UNAVAILABLE"}
    if reference_price / close <= 1 + MAX_ORDER_PRICE_DEVIATION:
        return {"state": "NOT_APPLICABLE"}
    style = str(
        features.get("entry_style_selected")
        or features.get("entry_setup_family") or ""
    ).upper()
    if style in {"MOMENTUM", "BREAKOUT", "PULLBACK"}:
        style = "ENTRY_" + style
    timing = evaluate_proof_for_candidate(
        env="practice", account_scope=account_scope, code=symbol,
        now=now, style=style,
        setup_ok=bool(features.get("setup_ok", getattr(cf, "setup_ok", False))),
        trigger_ok=bool(features.get("breakout_trigger_ok") or features.get("trigger_ok")),
        pivot=features.get("pivot"), order_price=close,
    )
    if timing.get("state") != "READY_SHADOW":
        return {"state": "BLOCK", "reason": "TIMING_NOT_READY",
                "signal_state": timing.get("state"),
                "signal_reason": timing.get("reason")}
    try:
        quote = engine._get_price_snapshot_cached(symbol, market="J")
    except Exception:
        return {"state": "BLOCK", "reason": "QUOTE_FETCH_UNAVAILABLE"}
    return resolve_bounded_prpr_limit(
        code=symbol, original_price=reference_price,
        timing=timing,
        quote=quote if isinstance(quote, dict) else {},
        # REST may take seconds after the minute proof was evaluated. Check
        # quote age against its *actual receipt evaluation instant*, not the
        # earlier bar-proof timestamp, to avoid classifying a newly fetched
        # authenticated quote as time travelling / stale.
        observed_at=datetime.now(KST),
    )



def apply_verified_bounded_limit(
    *, cf: Any, plan: dict[str, Any], price_proof: Mapping[str, Any],
    original_price: float, qty: int,
) -> float:
    """Atomically update the already-approved BUY price/notional projections.

    This helper never authorizes an order: the caller must still pass the
    immutable exit contract, independent sizing/duplicate/risk gates and the
    real broker pre-submit checks. Inconsistent bound evidence fails closed.
    """
    features = getattr(cf, "features", None)
    bounded = _pos(price_proof.get("new_limit"))
    original = _pos(original_price)
    floor = _pos(price_proof.get("price_floor"))
    ceiling = _pos(price_proof.get("price_ceiling"))
    if (
        not isinstance(plan, dict) or not isinstance(features, dict)
        or price_proof.get("state") != "BOUNDED_LIMIT"
        or price_proof.get("not_order_authorization") is not True
        or not isinstance(qty, int) or isinstance(qty, bool) or qty <= 0
        or not all((bounded, original, floor, ceiling))
        or bounded > original or not floor <= bounded <= ceiling
        or normalize_kr_order_price(bounded, side="BUY")[0] != bounded
    ):
        raise ValueError("KR_BOUND_PRICE_PROOF_INVALID")
    notional = float(bounded) * qty
    plan.update(
        limit_price=float(bounded), order_price=float(bounded),
        planned_value=notional, price_source="timing_bounded_prpr_limit",
    )
    features.update(
        limit_price=float(bounded), order_price=float(bounded),
        order_price_source="timing_bounded_prpr_limit",
        planned_value=notional,
    )
    cf.planned_value = notional
    sizing = getattr(cf, "sizing_details", None)
    if isinstance(sizing, dict):
        sizing.update(qty=qty, price=float(bounded), notional=notional)
    return float(bounded)
