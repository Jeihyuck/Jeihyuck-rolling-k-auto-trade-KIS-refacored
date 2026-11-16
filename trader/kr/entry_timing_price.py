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
    if source not in {"KIS_REST_INQUIRE_PRICE", "KIS_WEBSOCKET"}:
        return block("QUOTE_SOURCE_UNVERIFIED", quote_age_sec=round(quote_age, 3))
    requested_code = str(quote.get("requested_symbol") or quote.get("symbol") or "").removeprefix("A")
    if requested_code and requested_code.zfill(6) != str(code).zfill(6):
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
