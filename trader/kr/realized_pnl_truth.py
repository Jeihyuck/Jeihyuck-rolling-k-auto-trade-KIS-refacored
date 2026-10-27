"""Price evidence guard before writing realized KR sell PnL.

No synthetic execution price may be substituted for an absent KIS fill.
"""
from __future__ import annotations

import math
from typing import Any


def verified_sell_fill_price(value: Any) -> float | None:
    if isinstance(value, bool):
        return None  # bool is never a KIS execution price.
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or parsed <= 0:
        return None
    return parsed


def classify_sell_pnl_evidence(*, confirmed_qty: int, accounted_qty: int, price: Any) -> str:
    """Distinguish accounted shares from independently proven realization."""
    if confirmed_qty < 0 or accounted_qty < 0 or accounted_qty > confirmed_qty:
        return "INVALID_QTY_PROVENANCE"
    if confirmed_qty == 0:
        return "NO_CONFIRMED_SELL"
    if verified_sell_fill_price(price) is None:
        return "REALIZED_PNL_UNRESOLVED"
    return "CONFIRMED" if accounted_qty == confirmed_qty else "PARTIAL_PRICE_PROOF"


def verified_finite_amount(value: Any, *, nonnegative: bool = False) -> float | None:
    """Validate stored cost/notional/PnL inputs before arithmetic or DB writes."""
    if isinstance(value, bool):
        return None
    try:
        amount = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(amount) or (nonnegative and amount < 0):
        return None
    return amount
