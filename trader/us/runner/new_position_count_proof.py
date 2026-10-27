"""Read-only US new-position count from order identity + actual KIS fills.

Never count a BUY intent, ACK, synthetic balance delta or account holding as
a *new filled* position. Incomplete join evidence remains explicitly partial.
"""
from __future__ import annotations

import math
from typing import Any

from trader.us.utils.order_no import normalize_us_order_no


def _positive_qty(value: Any) -> bool:
    try:
        n = float(value)
        return math.isfinite(n) and n > 0
    except (TypeError, ValueError):
        return False


def verified_new_positions(
    order_audit: list[dict], broker_fills: list[dict],
) -> dict:
    """Count distinct symbols with NEW_POSITION_BUY backed by KIS order fills.

    Match the exact canonical order number AND symbol AND BUY side. Without
    identities a fill remains unmatched; zero must not masquerade as proof.
    """
    by_identity: dict[tuple[str, str], list[dict]] = {}
    for row in order_audit or []:
        if str(row.get("side") or "").upper() != "BUY":
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        order_no = normalize_us_order_no(
            row.get("canonical_order_no") or row.get("raw_order_no")
        )
        if symbol and order_no:
            by_identity.setdefault((symbol, order_no), []).append(row)

    confirmed = set()
    unmatched = 0
    eligible_buy_fills = 0
    for row in broker_fills or []:
        meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
        if meta.get("is_synthetic") or row.get("is_synthetic"):
            continue
        if str(row.get("side") or "").upper() != "BUY":
            continue
        if not _positive_qty(row.get("cumulative_filled_qty")
                             or row.get("qty") or row.get("filled_qty")):
            continue
        eligible_buy_fills += 1
        symbol = str(row.get("symbol") or row.get("pdno") or "").strip().upper()
        order_no = normalize_us_order_no(
            row.get("order_no_norm") or row.get("order_no")
        )
        matching = by_identity.get((symbol, order_no), [])
        if len(matching) != 1:
            unmatched += 1
            continue
        if str(matching[0].get("position_action") or "") == "NEW_POSITION_BUY":
            confirmed.add(symbol)
        elif not matching[0].get("position_action"):
            unmatched += 1
    status = (
        "VERIFIED" if unmatched == 0
        else "PARTIAL_UNVERIFIED"
    )
    return {
        "actual_new_positions": len(confirmed),
        "symbols": sorted(confirmed),
        "status": status,
        "kis_buy_fill_rows": eligible_buy_fills,
        "unmatched_buy_fill_rows": unmatched,
        "source": "KIS_ACTUAL_FILL_X_ORDER_AUDIT_IDENTITY",
    }
