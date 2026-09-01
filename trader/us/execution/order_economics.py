"""Canonical quantity/price/notional contract for US order intents."""
from __future__ import annotations

from typing import Any


PRICE_FIELDS = ("limit_price", "executable_price", "price_usd", "price", "current_price")


def authoritative_intent_price(intent: dict[str, Any], executable_price: float | None = None) -> float:
    if executable_price is not None:
        price = float(executable_price)
        if price > 0:
            return price
    for field in PRICE_FIELDS:
        value = intent.get(field)
        if value not in (None, ""):
            price = float(value)
            if price > 0:
                return price
    return 0.0


def normalize_order_intent_economics(
    intent: dict[str, Any], executable_price: float | None = None, *, reason: str = "qty_or_price_mutation"
) -> dict[str, Any]:
    """Mutate *intent* so qty, quantity and notional describe one order."""
    qty_value = intent.get("qty") if intent.get("qty") is not None else intent.get("quantity")
    qty = max(0, int(qty_value or 0))
    price = authoritative_intent_price(intent, executable_price)
    old_qty = intent.get("qty")
    old_notional = intent.get("notional_usd")
    intent["qty"] = qty
    intent["quantity"] = qty
    # Legacy signal-only callers may provide a notional without an executable
    # price.  There is no authoritative value from which to recompute it.
    if price <= 0:
        intent["notional_usd"] = float(old_notional or 0.0)
        return intent
    intent["notional_usd"] = round(qty * price, 4)
    if old_qty != qty or old_notional is None or abs(float(old_notional or 0) - intent["notional_usd"]) > 0.01:
        intent.setdefault("meta", {}).setdefault("economics_normalizations", []).append({
            "old_qty": old_qty, "new_qty": qty, "old_notional": old_notional,
            "new_notional": intent["notional_usd"], "price": price, "reason": reason,
        })
    return intent


def order_intent_economics_valid(intent: dict[str, Any], tolerance: float = 0.01) -> bool:
    qty = max(0, int(intent.get("qty") or 0))
    expected = qty * authoritative_intent_price(intent)
    return abs(float(intent.get("notional_usd") or 0.0) - expected) <= tolerance
